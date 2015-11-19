package io.eguan.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2015 Oodrive
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static io.eguan.dtx.proto.TxProtobufUtils.toUuid;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.dtx.DistTxWrapper.TxMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * The transaction initiator is in charge of the execution of a transaction. It retrieves the requests from the queue
 * shared with the {@link DtxManager} and process the request by initiating the two phase commit.
 * 
 * The two phase commit is implemented through the use of the distributed executor provided by Hazelcast.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
final class TransactionInitiator {
    static final Logger LOGGER = LoggerFactory.getLogger(TransactionInitiator.class);

    static final String TX_ID_GENERATOR_NAME = "TX_ID_GEN";
    static final String TX_CURRENT_ID = "TX_CURRENT_ID";

    private static final long TX_CURRENT_ID_TIMEOUT = 100; // ms
    private static final long JOIN_TERMINATION_TIMEOUT = 1000; // ms

    private final HazelcastInstance hazelcastInstance;
    private final BlockingQueue<Request> requestQueue;
    private final ProcessRequestQueue processRequestQueue;
    private final UUID nodeId;
    private final long txTimeout;

    private volatile boolean started = false;

    @GuardedBy("submitSemaphore")
    private volatile Request currentRequest;

    private final DtxManager dtxManager;

    /**
     * The semaphore guarding request submission and dequeuing as well as consulting a task's state.
     * 
     * The total number of permits is 3. The executing thread will acquire 2 permits before dequeuing, only holding one
     * while waiting. Submission will acquire one permit while adding to the queue. Task status queries will hold two
     * permits while searching the queue and current request.
     * 
     * Thus dequeuing stops as long as a task status query is running, while still allowing submission.
     */
    private final Semaphore submitSemaphore = new Semaphore(3);

    /**
     * Gets the {@link Semaphore} guarding request submission and dequeuing.
     * 
     * @return a {@link Semaphore}
     */
    final Semaphore getSubmitSemaphore() {
        return submitSemaphore;
    }

    /**
     * The thread which is in charge to execute the two phase commit.
     * 
     * 
     */
    private final class ProcessRequestQueue extends Thread {
        private volatile boolean running = true;

        private final Predicate<DistOpResult> exitZeroPredicate = new Predicate<DistOpResult>() {
            @Override
            public final boolean apply(final DistOpResult input) {
                return input.getExitStatus() == 0;
            }
        };

        private final Predicate<DistOpResult> exitNonZeroPredicate = new Predicate<DistOpResult>() {
            @Override
            public final boolean apply(final DistOpResult input) {
                return input.getExitStatus() != 0;
            }
        };

        @Override
        public final void run() {

            while (running) {

                if (currentRequest != null && DtxTaskStatus.COMMITTED.compareTo(currentRequest.getTaskStatus()) > 0) {
                    processRequest(currentRequest);
                }

                try {
                    submitSemaphore.acquire(2);
                }
                catch (final InterruptedException e) {
                    continue;
                }

                try {
                    submitSemaphore.release();
                    currentRequest = requestQueue.take();
                }
                catch (final InterruptedException e) {
                    continue;
                }
                finally {
                    submitSemaphore.release(2);
                }

                processRequest(currentRequest);

            }
        }

        private final void stopProcessing() {
            this.running = false;
        }

        // execute the two phase commit algorithm
        // the access to the request variable is thread safe
        private final void processRequest(final Request request) {

            if (!dtxManager.isQuorumOnline()) {
                LOGGER.error("No quorum online, not starting transaction; request=" + request);
                // TODO: record the changed state somehow somewhere
                dtxManager.setTask(request.getTaskId(), DtxConstants.DEFAULT_LAST_TX_VALUE, request.getResourceId(),
                        DtxTaskStatus.ROLLED_BACK, null);
                return;

            }

            Set<DtxNode> targetNodes = dtxManager.getOnlinePeers();

            try {
                // generate a new transaction id
                final long newTxId = newTxId();
                LOGGER.info("Processing transaction; txId=" + newTxId + ", taskId=" + request.getTaskId() + ", nodeId="
                        + dtxManager.getNodeId());

                final UUID resId = request.getResourceId();

                // build the transaction
                final TxMessage transaction = TxMessage.newBuilder().setVersion(ProtocolVersion.VERSION_1)
                        .setTxId(newTxId).setTaskId(toUuid(request.getTaskId())).setInitiatorId(toUuid(nodeId))
                        .setResId(toUuid(resId)).setTimeout(txTimeout)
                        .setPayload(ByteString.copyFrom(request.getPayload())).build();

                // dispatch monitors
                final Set<Member> targetMembers = new HashSet<Member>();
                final Member localMember = hazelcastInstance.getCluster().getLocalMember();
                for (final DtxNode currNode : targetNodes) {
                    targetMembers.add(currNode.asHazelcastMember(localMember));
                }

                final DistributedTask<Void> monitorTask = new DistributedTask<Void>(new TransactionMonitorHandler(
                        newTxId, resId, txTimeout, targetNodes), null, targetMembers);
                hazelcastInstance.getExecutorService().execute(monitorTask);

                switch (currentRequest.getTaskStatus()) {
                case PENDING:
                    // 2PC : phase START
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Processing start; txId=" + newTxId + ", nodeId=" + dtxManager.getNodeId());
                    }
                    final Map<DtxNode, DistOpResult> startResult = processStart(transaction, targetNodes);

                    final Map<DtxNode, DistOpResult> successfulStartResult = Maps.filterValues(startResult,
                            exitZeroPredicate);
                    final int startSuccessSize = successfulStartResult.size();
                    if (!dtxManager.countsAsQuorum(startSuccessSize)) {
                        final Map<DtxNode, DistOpResult> rollbackResult = processRollback(newTxId,
                                getMembersForRollback(startResult));

                        final String startErrorReport = buildErrorReport("No quorum", "start", startResult,
                                rollbackResult);
                        LOGGER.warn(startErrorReport);
                        // TODO: hand over the report to the task

                        request.setTaskStatus(DtxTaskStatus.ROLLED_BACK);
                        setTask(newTxId, request, DtxTaskStatus.ROLLED_BACK);
                        return;
                    }

                    request.setTaskStatus(DtxTaskStatus.STARTED);
                    setTask(newTxId, request, DtxTaskStatus.STARTED);

                    // reduce targetNodes to the quorum for prepare
                    if (startSuccessSize < targetNodes.size()) {
                        targetNodes = Sets.filter(targetNodes, new Predicate<DtxNode>() {

                            @Override
                            public final boolean apply(final DtxNode arg0) {
                                return successfulStartResult.containsKey(arg0);
                            }
                        });
                    }
                    //$FALL-THROUGH$
                case STARTED:

                    // synchronization barrier between all nodes
                    boolean isInterruptedOnPrepare;
                    do {
                        isInterruptedOnPrepare = false;
                        try {
                            waitForPrepare(newTxId);
                        }
                        catch (final InterruptedException e1) {
                            isInterruptedOnPrepare = true;
                        }
                    } while (isInterruptedOnPrepare);

                    // 2PC : phase PREPARE AND DECISION
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Processing prepare; txId=" + newTxId + ", nodeId=" + dtxManager.getNodeId());
                    }
                    final Map<DtxNode, DistOpResult> prepareResult = processPrepare(newTxId, targetNodes);

                    final int prepareSuccessSize = Maps.filterValues(prepareResult, exitZeroPredicate).size();

                    if (!dtxManager.countsAsQuorum(prepareSuccessSize)) {
                        final Map<DtxNode, DistOpResult> rollbackResult = processRollback(newTxId,
                                getMembersForRollback(prepareResult));

                        final String prepareErrorReport = buildErrorReport("No quorum", "start", prepareResult,
                                rollbackResult);
                        LOGGER.warn(prepareErrorReport);
                        // TODO: hand over the report to the task

                        request.setTaskStatus(DtxTaskStatus.ROLLED_BACK);
                        updateTask(newTxId, DtxTaskStatus.ROLLED_BACK);
                        return;
                    }

                    request.setTaskStatus(DtxTaskStatus.PREPARED);
                    updateTask(newTxId, DtxTaskStatus.PREPARED);
                    //$FALL-THROUGH$
                case PREPARED:
                    processCommit(newTxId, targetNodes);
                    request.setTaskStatus(DtxTaskStatus.COMMITTED);
                    updateTask(newTxId, DtxTaskStatus.COMMITTED);
                    break;
                case COMMITTED:
                case ROLLED_BACK:
                default:
                    // nothing
                }
            }
            finally {
                // increment global transaction ID
                incrementTxId();
            }
        }

        // start request
        private final Map<DtxNode, DistOpResult> processStart(final TxMessage transaction, final Set<DtxNode> nodes) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processing start on transaction " + transaction.getTxId());
            }
            return processOperation(transaction.getTxId(), nodes, new StartHandler(transaction, nodes), "start");
        }

        // prepare request
        private final Map<DtxNode, DistOpResult> processPrepare(final long txId, final Set<DtxNode> nodes) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processing prepare on transaction " + txId);
            }
            return processOperation(txId, nodes, new PrepareHandler(txId), "prepare");
        }

        // commit request
        private final Map<DtxNode, DistOpResult> processCommit(final long txId, final Set<DtxNode> nodes) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processing commit on transaction " + txId);
            }
            return processOperation(txId, nodes, new CommitHandler(txId, nodes), "commit");
        }

        // abort request
        private final Map<DtxNode, DistOpResult> processRollback(final long txId, final Set<DtxNode> nodes) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("processing rollback on transaction " + txId);
            }
            if (nodes.isEmpty()) {
                return new HashMap<DtxNode, DistOpResult>();
            }
            return processOperation(txId, nodes, new RollBackHandler(txId, nodes), "rollback");
        }

        /**
         * Run an operation asynchronously for each member and wait for responses during a limited time specified by
         * {@link TransactionInitiator#txTimeout} in ms.
         * 
         * @param txId
         *            the transaction ID
         * @param nodes
         *            participants to the transaction
         * @param callable
         *            the callable to execute remotely
         * @param descOp
         *            the description of the operation, used for logging
         * @return the {@link DistOpResult} provided for every participant
         */
        private final Map<DtxNode, DistOpResult> processOperation(final long txId, final Set<DtxNode> nodes,
                final Callable<DistOpResult> callable, final String descOp) {

            // TODO: add health check and/or automatic recovery if Hazelcast was disconnected/shutdown

            // synchronization mechanism used to wait for ACKs from remote peers
            final CountDownLatch waitForAcks = new CountDownLatch(nodes.size());

            // map used to store the result of remote operations
            final Map<DtxNode, DistOpResult> asyncResults = new ConcurrentHashMap<DtxNode, DistOpResult>();

            // run the task into remote peers
            final ExecutorService executorService = hazelcastInstance.getExecutorService();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Submitting distributed task; members=" + nodes);
            }
            for (final DtxNode targetParticipant : nodes) {
                final DistributedTask<DistOpResult> prepareTask = makeDistributedTask(callable, targetParticipant,
                        waitForAcks, asyncResults);
                executorService.execute(prepareTask);
            }

            // wait for ACKs using CountDownLatch class
            boolean isInterruptedOnAwait;
            do {
                isInterruptedOnAwait = false;
                try {
                    if (!waitForAcks.await(txTimeout, TimeUnit.MILLISECONDS)) {
                        LOGGER.error("Timeout while waiting for results; operation=" + descOp);
                        // TODO: signal the calling method
                    }
                }
                catch (final InterruptedException e) {
                    isInterruptedOnAwait = true;
                }
            } while (isInterruptedOnAwait);

            // check if errors occurred during the operation
            return checkAsyncResults(asyncResults, nodes, descOp, txId);
        }

        /**
         * Return all members in a given result map that must be included in the rollback following the operation having
         * produced this result.
         * 
         * @param resultMap
         *            the input result map, which has at least one non-zero return value
         * @return a (possibly empty) {@link Set} of Members
         */
        private final Set<DtxNode> getMembersForRollback(final Map<DtxNode, DistOpResult> resultMap) {
            final HashSet<DtxNode> result = new HashSet<DtxNode>(resultMap.keySet());
            for (final DtxNode currMember : resultMap.keySet()) {
                switch (resultMap.get(currMember).getExitStatus()) {
                // the following return codes leave the transaction non-existent, so no rollback is sent
                case XAException.XAER_PROTO: // transaction is already started/prepared
                case XAException.XAER_NOTA: // transaction does not exist or has an invalid/already executed ID
                    result.remove(currMember);
                    break;
                case 0: // no error -> send rollback
                case -1: // general/unspecified error -> rollback!
                case XAException.XAER_INVAL: // transaction or transaction context is invalid
                case XAException.XAER_RMFAIL: // resource manager is unavailable
                case XAException.XAER_RMERR: // an internal resource manager error occurred
                case XAException.XA_RBROLLBACK: // transaction must be rolled back for an unspecified reason
                case XAException.XA_RBDEADLOCK: // transaction must be rolled back due to a deadlock
                case XAException.XA_RBINTEGRITY: // transaction must be rolled back as it violates resource integrity
                case XAException.XA_RBPROTO: // transaction must be rolled back following an internal protocol error
                default:
                    // do nothing, leaving the member included for rollback
                    break;
                }
            }
            return result;
        }

        /**
         * Loop over the {@link AsyncResult} list, if an error occurred then log it.
         * 
         * @param asyncResults
         *            the list of results
         * @param expectedSize
         *            the expected result size
         * @param op
         *            the operation name is used for the logging
         * @param txId
         *            the transaction ID
         * @return the result of every participant of the transaction
         */
        private final Map<DtxNode, DistOpResult> checkAsyncResults(final Map<DtxNode, DistOpResult> asyncResults,
                final Set<DtxNode> participants, final String op, final long txId) {

            final int expectedSize = participants.size();
            final HashMap<DtxNode, DistOpResult> results = new HashMap<DtxNode, DistOpResult>(expectedSize);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Analysing results; expectedSize=" + expectedSize + ", resultSize=" + asyncResults.size()
                        + ", results=" + asyncResults);
            }

            for (final DtxNode currParticipants : participants) {
                final Optional<DistOpResult> membResult = Optional.fromNullable(asyncResults.get(currParticipants));
                results.put(currParticipants, membResult.isPresent() ? membResult.get() : new DistOpResult(-1,
                        "Missing result"));
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Returning result; result=" + results);
            }
            return results;
        }

        /**
         * Make a {@link DistributedTask} object with an {@link ExecutionCallback}.
         * 
         * When the operation is done, the callback set the result into the {@link AsyncResult} list and call the method
         * {@link CountDownLatch#countDown()} onto "waitForAcks" parameter.
         * 
         * @param callable
         *            the {@link Callable} to invoke remotely
         * @param member
         *            the member on which the operation is executed
         * @param waitForAcks
         *            used for synchronization
         * @param asyncResults
         *            used to retrieve results
         * @return the pre-configured {@link DistributedTask} instance
         */
        private final DistributedTask<DistOpResult> makeDistributedTask(final Callable<DistOpResult> callable,
                final DtxNode member, final CountDownLatch waitForAcks, final Map<DtxNode, DistOpResult> asyncResults) {

            final DistributedTask<DistOpResult> task = new DistributedTask<DistOpResult>(callable,
                    member.asHazelcastMember(hazelcastInstance.getCluster().getLocalMember()));

            task.setExecutionCallback(new ExecutionCallback<DistOpResult>() {

                @Override
                public final void done(final Future<DistOpResult> future) {
                    DistOpResult resultOp;

                    if (future.isDone()) {
                        boolean isInterruptedOnGet;
                        do {
                            isInterruptedOnGet = false;
                            try {
                                resultOp = future.get();
                                // apparently Hazelcast's futures can return null on internal exceptions
                                if (resultOp == null) {
                                    resultOp = new DistOpResult(-1, "Unspecified Hazelcast failure");
                                }
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Adding normal result for member; member=" + member + ", result="
                                            + resultOp);
                                }
                                asyncResults.put(member, resultOp);
                                waitForAcks.countDown();
                            }
                            catch (final ExecutionException e) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Adding error result for member; member=" + member + ", error="
                                            + e.getMessage() + ", cause=" + e.getCause());
                                }
                                asyncResults.put(member, new DistOpResult(-1, e));
                                waitForAcks.countDown();
                            }
                            catch (final InterruptedException e) {
                                isInterruptedOnGet = true;
                            }
                        } while (isInterruptedOnGet);
                    }
                }
            });

            return task;
        }

        /**
         * Generate a new global transaction id.
         * 
         * @return txId
         */
        private final long newTxId() {
            return hazelcastInstance.getAtomicNumber(TX_ID_GENERATOR_NAME).incrementAndGet();
        }

        private final void incrementTxId() {
            hazelcastInstance.getAtomicNumber(TX_CURRENT_ID).incrementAndGet();
        }

        /**
         * Wait until the distributed current transaction id is equal to txId.
         * 
         * @param txId
         * @throws InterruptedException
         */
        private final void waitForPrepare(final long txId) throws InterruptedException {
            final AtomicNumber currentTxId = hazelcastInstance.getAtomicNumber(TX_CURRENT_ID);

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Waiting for next transaction; txId=" + txId + ", current=" + currentTxId.get());
            }
            while ((currentTxId.get() + 1) != txId) {
                Thread.sleep(TX_CURRENT_ID_TIMEOUT);
            }
        }

        /**
         * Builds an error report for transaction.
         * 
         * @param message
         *            the main error message, i.e. the description of the error condition
         * @param operation
         *            name of the operation, one of start, prepare, commit, rollback
         * @param opResult
         *            a {@link Map} of {@link DistOpResult}s indexed by {@link DtxNode}
         * @param rollbackResult
         *            {@link DistOpResult} for all nodes having performed the rollback
         * @return a one-line error report containing all of the above information
         */
        private final String buildErrorReport(final String message, final String operation,
                final Map<DtxNode, DistOpResult> opResult, final Map<DtxNode, DistOpResult> rollbackResult) {
            final StringBuilder result = new StringBuilder();

            result.append("Distributed execution error: ").append(message);

            final Map<DtxNode, DistOpResult> errorResults = Maps.filterValues(opResult, exitNonZeroPredicate);
            result.append("; operation=").append(operation).append(", nodes=").append(opResult.size())
                    .append(", errors=").append(errorResults.size());

            for (final Entry<DtxNode, DistOpResult> errEntry : errorResults.entrySet()) {
                result.append("; node=").append(errEntry.getKey()).append(", result=").append(errEntry.getValue());
            }
            if (rollbackResult != null && !rollbackResult.isEmpty()) {
                result.append("; rollback results: ");
                for (final Entry<DtxNode, DistOpResult> rollbackEntry : rollbackResult.entrySet()) {
                    result.append("node=").append(rollbackEntry.getKey()).append(", result=")
                            .append(rollbackEntry.getValue()).append("; ");
                }
            }
            return result.toString();
        }
    }

    /**
     * Constructs an instance for a given DTX node.
     * 
     * @param nodeId
     *            the id of the node which the transaction initiator will be running
     * @param hazelcastInstance
     *            the Hazelcast instance used to deal with remote communications
     * @param dtxManager
     *            the {@link DtxManager} containing this instance
     * @param requestQueue
     *            the queue where requests reside
     * @param txTimeout
     *            the timeout in ms, time during a remote call is valid
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */
    TransactionInitiator(@Nonnull final UUID nodeId, @Nonnull final HazelcastInstance hazelcastInstance,
            @Nonnull final DtxManager dtxManager, @Nonnull final BlockingQueue<Request> requestQueue,
            final long txTimeout) throws NullPointerException {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance, "hazelcastInstance must not be null");
        this.dtxManager = Objects.requireNonNull(dtxManager, "DtxManager must not be null");
        this.requestQueue = Objects.requireNonNull(requestQueue, "requestQueue must not be null");
        this.processRequestQueue = new ProcessRequestQueue();
        this.processRequestQueue.setName("ProcessQueue_" + dtxManager.getNodeId());
        this.processRequestQueue.setDaemon(true);
        this.txTimeout = (txTimeout > 0) ? txTimeout : 0;
    }

    /**
     * Gets the {@link DtxTaskStatus status} of a given task.
     * 
     * @param taskId
     *            the ID of the requested task
     * @return a valid {@link DtxTaskStatus}, {@link DtxTaskStatus#UNKNOWN} if it could not be determined
     */
    final DtxTaskStatus getTaskStatus(final UUID taskId) {

        try {
            submitSemaphore.acquire(2);
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted while getting task status");
        }
        try {
            // checks if the task is still in the queue
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Searching task in request queue; taskID=" + taskId);
            }
            for (final Request currRequest : this.requestQueue) {
                if (currRequest.getTaskId().equals(taskId)) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found task in request queue; taskID=" + taskId);
                    }
                    return currRequest.getTaskStatus();
                }
            }
            if (currentRequest != null) {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Querying current request for task status; taskID=" + taskId);
                }
                final UUID currTaskId = currentRequest.getTaskId();
                if ((currTaskId != null) && currTaskId.equals(taskId)) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Current request has requested task ID; taskID=" + taskId);
                    }
                    return currentRequest.getTaskStatus();
                }
            }
        }
        finally {
            submitSemaphore.release(2);
        }

        return this.dtxManager.searchTaskStatus(taskId);

    }

    /**
     * Start the transaction initiator, the request queue will be processed.
     */
    final void start() {
        processRequestQueue.start();
        started = true;
    }

    /**
     * Stop the transaction initiator.
     */
    final void stop() {

        processRequestQueue.stopProcessing();
        processRequestQueue.interrupt();

        try {
            processRequestQueue.join(JOIN_TERMINATION_TIMEOUT);
        }
        catch (final InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the terminaison of the thread {} ",
                    processRequestQueue.getName());
        }
        started = false;
    }

    /**
     * Request a transaction associated to a task to be canceled.
     * 
     * @param taskId
     *            the task id
     * @return <code>false</code> if the task could not be cancelled, <code>true</code> otherwise
     */
    final boolean requestCancel(final UUID taskId) {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Tries to merge the given "last transaction ID" with the operational state of the atomic next transaction ID
     * generator and current transaction ID counters.
     * 
     * The current merge implementation attempts to increment global counters as long as there are no unfinished
     * transactions and waits if any are started during the merge. As no distributed locks are defined to protect access
     * to these counters, this is a very real possibility and can significantly slow down the merge.
     * 
     * If this instance isn't {@link #started}, nothing is done.
     * 
     * 
     * @param lastTxIdToMerge
     *            the value to which to update the next transaction ID generator, i.e. the next generated transaction ID
     *            will be this value + 1
     * @throws IllegalStateException
     *             if the merge procedure is interrupted
     */
    final void mergeLastTxCounters(final long lastTxIdToMerge) throws IllegalStateException {
        if (!started) {
            return;
        }

        final AtomicNumber genCounter = hazelcastInstance.getAtomicNumber(TX_ID_GENERATOR_NAME);
        final AtomicNumber currCounter = hazelcastInstance.getAtomicNumber(TX_CURRENT_ID);

        long lastGenValue = genCounter.get();
        if (lastTxIdToMerge <= lastGenValue) {
            return;
        }

        final long lastCurrValue = currCounter.get();
        // if both are equal, try updating both
        if (lastGenValue == lastCurrValue) {
            if (genCounter.compareAndSet(lastGenValue, lastTxIdToMerge)
                    && currCounter.compareAndSet(lastCurrValue, lastTxIdToMerge)) {
                return;
            }
            // try once more with updated values
            mergeLastTxCounters(lastTxIdToMerge);
            if ((genCounter.get() >= lastTxIdToMerge) && (currCounter.get() >= lastTxIdToMerge)) {
                return;
            }
        }

        // we're currently executing a transaction, so try to increment only the generator, then the current txId
        boolean genCounterUpdated = false;
        while (lastGenValue < lastTxIdToMerge) {
            // attempt to update the gen counter
            genCounterUpdated = genCounter.compareAndSet(lastGenValue, lastTxIdToMerge);
            while (!genCounterUpdated) {
                // genCounter changed, thus wait for the current counter to be updated
                lastGenValue = genCounter.get();
                // if the gen counter is already where we want it to be, break out of this loop
                if (lastGenValue >= lastTxIdToMerge) {
                    // set local update success to reflect global update status
                    genCounterUpdated = (currCounter.get() >= lastTxIdToMerge);
                    break;
                }
                // wait for transactions to complete, with a global timeout at (expected nb of tx) * (tx timeout)
                final long timeoutTime = ((lastGenValue - currCounter.get()) * this.txTimeout)
                        + System.currentTimeMillis();
                while ((System.currentTimeMillis() < timeoutTime) && (currCounter.get() < lastGenValue)) {
                    try {
                        Thread.sleep(TX_CURRENT_ID_TIMEOUT);
                    }
                    catch (final InterruptedException e) {
                        throw new IllegalStateException("Interrupted");
                    }
                }
                genCounterUpdated = genCounter.compareAndSet(lastGenValue, lastTxIdToMerge);
            }
        }
        // if the gen counter was successfully updated, update the current tx counter
        if (genCounterUpdated) {
            DtxUtils.updateAtomicNumberToAtLeast(currCounter, lastTxIdToMerge);
        }
    }

    /**
     * Create a new task or update a task in the task keeper.
     * 
     * @param newTxId
     *            the transaction ID to set
     * @param request
     *            the request is used to construct the task
     * @param newStatus
     *            the new status for the task.
     * 
     * @param timestamp
     *            the timestamp for the task.
     */
    private final void setTask(final long newTxId, final Request request, final DtxTaskStatus newStatus) {
        dtxManager.setTask(request.getTaskId(), newTxId, request.getResourceId(), newStatus, null);
    }

    /**
     * Update a task status in the task keeper corresponding to a given transaction ID.
     * 
     * @param txId
     *            the transaction id of the task
     * @param newStatus
     *            the {@link DtxTaskStatus} to use
     */
    private final void updateTask(final long txId, final DtxTaskStatus newStatus) {
        dtxManager.setTaskStatus(txId, newStatus);
    }

}
