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

import static io.eguan.dtx.DtxConstants.DEFAULT_JOURNAL_FILE_PREFIX;
import static io.eguan.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static io.eguan.dtx.DtxResourceManagerState.LATE;
import static io.eguan.dtx.DtxResourceManagerState.POST_SYNC_PROCESSING;
import static io.eguan.dtx.DtxResourceManagerState.SYNCHRONIZING;
import static io.eguan.dtx.DtxResourceManagerState.UNDETERMINED;
import static io.eguan.dtx.DtxResourceManagerState.UNREGISTERED;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static io.eguan.dtx.DtxTaskStatus.COMMITTED;
import static io.eguan.dtx.DtxTaskStatus.ROLLED_BACK;
import static io.eguan.dtx.DtxTaskStatus.STARTED;
import static io.eguan.dtx.DtxTaskStatus.UNKNOWN;
import static io.eguan.dtx.DtxUtils.updateAtomicLongToAtLeast;
import static io.eguan.dtx.proto.TxProtobufUtils.fromUuid;
import io.eguan.dtx.DtxTaskApiAbstract.TaskLoader;
import io.eguan.dtx.events.DtxResourceManagerEvent;
import io.eguan.dtx.journal.JournalRecord;
import io.eguan.dtx.journal.JournalRotationManager;
import io.eguan.dtx.journal.WritableTxJournal;
import io.eguan.dtx.proto.TxProtobufUtils;
import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry;
import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode;
import io.eguan.proto.dtx.DistTxWrapper.TxMessage;
import io.eguan.proto.dtx.DistTxWrapper.TxNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * The transaction manager managing all transactions for all resource managers of a single node.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
final class TransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManager.class.getName());

    private static final List<DtxResourceManagerState> TX_AUTH_STATES = Arrays.asList(new DtxResourceManagerState[] {
            UP_TO_DATE, POST_SYNC_PROCESSING });

    /**
     * Generates a prefix string for a journal file.
     * 
     * @param nodeId
     *            the ID of the DTX node to include in the result
     * @param resId
     *            the ID of the {@link DtxResourceManager} for which the journal is written
     * @return a non-empty {@link String}
     */
    static final String newJournalFilePrefix(final UUID nodeId, final UUID resId) {
        return DEFAULT_JOURNAL_FILE_PREFIX + nodeId + "_" + resId;
    }

    private final DtxManagerConfig dtxManagerConfig;
    private final ConcurrentHashMap<Long, DtxResourceManagerContext> contextMap = new ConcurrentHashMap<>();

    /**
     * {@link ConcurrentHashMap} holding the {@link DtxResourceManager} instances registered with this
     * {@link DtxManager}.
     */
    @GuardedBy("transactionLock")
    private final ConcurrentHashMap<UUID, DtxResourceManager> resourceManagers;

    @GuardedBy("transactionLock")
    private final ConcurrentHashMap<UUID, WritableTxJournal> journals;

    @GuardedBy("transactionLock")
    private final ConcurrentHashMap<UUID, DtxResourceManagerState> states;

    /**
     * Shared lock whose only purpose is to postpone shutdown until all transaction processing is done.
     */
    private final ReentrantReadWriteLock transactionLock = new ReentrantReadWriteLock();

    /**
     * Exclusive lock to be held for replaying transactions during synchronization.
     */
    private final ReentrantLock syncReplayLock = new ReentrantLock();

    /**
     * Exclusive lock to be held while modifying any resource manager's synchronization state.
     */
    private final ReentrantLock syncStateLock = new ReentrantLock();

    /**
     * Flag set once the {@link #shutdown()} method completes.
     * 
     * This is used to refuse transaction operations after shutdown.
     */
    private volatile boolean shutdown = true;

    /**
     * The last successfully started transaction's ID.
     */
    private final AtomicLong lastFinishedTxId = new AtomicLong(DEFAULT_LAST_TX_VALUE);

    /**
     * The last successfully prepared transaction's ID.
     * 
     * Note: This cannot be persisted, but will be reinitialized with the first {@link #prepare(long)}.
     */
    private final AtomicLong lastPreparedTxId = new AtomicLong(DEFAULT_LAST_TX_VALUE);

    /**
     * The rotation manager handling all this instance's journal rotations.
     */
    private final JournalRotationManager journalRotationMgr;

    /**
     * The {@link DtxManager} containing this instance.
     * 
     * Note: This is only to be used for referencing the source of events. Directly calling its methods exposes to risks
     * of deadlocking as they might in turn call this instance.
     */
    private final DtxManager dtxManager;

    /**
     * Constructs an instance associated to a {@link DtxManager}.
     * 
     * The created instance will accept transaction processing step calls and forward them to the registered
     * {@link DtxResourceManager}s.
     * 
     * @param configuration
     *            the {@link DtxManagerConfig} used to instantiate the containing DTX node
     * @param dtxManager
     *            containing {@link DtxManager} for reference
     */
    @ParametersAreNonnullByDefault
    TransactionManager(@Nonnull final DtxManagerConfig configuration, final DtxManager dtxManager) {
        this.dtxManagerConfig = Objects.requireNonNull(configuration);
        this.dtxManager = Objects.requireNonNull(dtxManager);
        resourceManagers = new ConcurrentHashMap<UUID, DtxResourceManager>();
        journals = new ConcurrentHashMap<UUID, WritableTxJournal>();
        states = new ConcurrentHashMap<UUID, DtxResourceManagerState>();

        // TODO: get parameter values from configuration
        journalRotationMgr = new JournalRotationManager(0);
    }

    /**
     * Starts the first phase of transaction processing.
     * 
     * @param transaction
     *            the complete {@link TxMessage} object representing a valid transaction.
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws XAException
     *             if the start operation did not complete, with the following return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_PROTO} if the transaction is already started</li>
     *             <li>{@link XAException#XAER_NOTA} if the transaction has an invalid/already executed ID</li>
     *             <li>{@link XAException#XAER_INVAL} if the transaction is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     * @throws IllegalStateException
     *             if the {@link TransactionManager} was {@link #shutdown shut down}
     */
    final void start(@Nonnull final TxMessage transaction, @Nonnull final Iterable<TxNode> participants)
            throws XAException, IllegalStateException {

        Objects.requireNonNull(transaction);

        final DtxResourceManager dtxResourceManager = checkedGetResourceManager(TxProtobufUtils.fromUuid(transaction
                .getResId()));

        final long txId = transaction.getTxId();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Starting transaction; txId=" + txId + ", resId=" + dtxResourceManager.getId().toString());
        }

        final WritableTxJournal journal = journals.get(dtxResourceManager.getId());

        // checks if transaction is already started
        if (this.contextMap.keySet().contains(Long.valueOf(txId)) && !syncReplayLock.isHeldByCurrentThread()) {
            throw new XAException(XAException.XAER_PROTO);
        }

        this.transactionLock.readLock().lock();
        try {

            // checks if transaction has already been completed and we're not replaying transactions
            if (txId <= this.lastFinishedTxId.longValue() && !syncReplayLock.isHeldByCurrentThread()) {
                LOGGER.error("Invalid transaction ID; txId=" + txId + ", lastTxId=" + lastFinishedTxId.longValue());
                throw new XAException(XAException.XAER_NOTA);
            }

            if (shutdown) {
                LOGGER.error("Shut down");
                throw new IllegalStateException("Shut down.");
            }

            final DtxResourceManagerContext startedCtx = dtxResourceManager.start(transaction.getPayload()
                    .toByteArray());

            // logs to journal
            journal.writeStart(transaction, participants);

            // Create or update task in the task keeper
            setTask(dtxResourceManager, transaction, DtxTaskStatus.STARTED);

            contextMap.put(Long.valueOf(transaction.getTxId()), startedCtx);
        }
        catch (final IOException e) {
            LOGGER.error("Failed to write start to journal; resourceID=" + dtxResourceManager.getId().toString()
                    + ",txID=" + txId, e);
            // TODO: add error treatment/return
        }
        finally {
            transactionLock.readLock().unlock();
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Started transaction; txId=" + txId);
        }

    }

    /**
     * Prepares transaction execution as first part of the 2-phase commit.
     * 
     * Returning <code>true</code> means there is no obstacle to committing the transaction.
     * 
     * @param txId
     *            the transaction ID associated to the transaction context
     * @return {@link Boolean#TRUE} if the transaction can be committed, {@link Boolean#FALSE} otherwise
     * @throws XAException
     *             if the prepare operation fails, with the following return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_NOTA} if no transaction with the given transaction ID exists</li>
     *             <li>{@link XAException#XAER_PROTO} if the given transaction is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the transaction context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBDEADLOCK} transaction must be rolled back due to a deadlock</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     * @throws IllegalStateException
     *             if the {@link TransactionManager} was {@link #shutdown shut down}
     */
    final Boolean prepare(final long txId) throws XAException, IllegalStateException {
        final DtxResourceManagerContext dtxResourceManagerContext = getContext(txId);

        final DtxResourceManager dtxResourceManager = checkedGetResourceManager(dtxResourceManagerContext
                .getResourceManagerId());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Preparing transaction; txId=" + txId + ", resId=" + dtxResourceManager.getId().toString());
        }

        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
        try {
            final boolean holdsSyncLock = syncReplayLock.isHeldByCurrentThread();

            if (txId <= this.lastFinishedTxId.longValue() && !holdsSyncLock) {
                LOGGER.error("Invalid transaction ID; txId=" + txId + ", lastTxId=" + lastFinishedTxId.longValue());
                throw new XAException(XAException.XAER_NOTA);
            }

            /*
             * Checks that all preceding transactions have been prepared. This relies on the initiator enforcing a
             * global prepare order to avoid invalidating transactions just to preserve commitment ordering.
             */
            final long lastPrepd = lastPreparedTxId.longValue();
            final long lastFinished = lastFinishedTxId.longValue();
            if ((txId <= lastPrepd) && !holdsSyncLock) {
                LOGGER.error("Commitment order violation: Transaction ID is out of order; txId=" + txId
                        + ", lastPreparedTxId=" + lastPrepd);
                throw new XAException(XAException.XAER_PROTO);
            }

            /*
             * Checks that this instance can currently execute a prepare without violating commitment ordering
             * constraints.
             */
            if ((lastPrepd > lastFinished) && !holdsSyncLock) {
                LOGGER.error("Commitment order violation: Last prepared transaction is not finished; txId=" + txId
                        + ", lastPreparedTxId=" + lastPrepd + ", lastFinishedTxId=" + lastFinished);
                throw new XAException(XAException.XAER_PROTO);
            }

            if (shutdown) {
                throw new IllegalStateException("Shut down");
            }
            final Boolean result = dtxResourceManager.prepare(dtxResourceManagerContext);

            // Update task status in the task keeper
            updateTask(txId, DtxTaskStatus.PREPARED);

            // updates the last transaction ID
            updateAtomicLongToAtLeast(lastPreparedTxId, txId);

            return result;
        }
        finally {
            transactionLock.readLock().unlock();
        }

    }

    /**
     * Commits the transaction as second part of the 2-phase commit.
     * 
     * @param txId
     *            the transaction ID associated to the transaction context
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws XAException
     *             if the commit operation fails, with the following return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_NOTA} if no transaction with the given transaction ID exists</li>
     *             <li>{@link XAException#XAER_PROTO} if the given transaction is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the transaction context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBDEADLOCK} transaction must be rolled back due to a deadlock</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     * @throws IllegalStateException
     *             if the {@link TransactionManager} was {@link #shutdown shut down}
     */
    final void commit(final long txId, @Nonnull final Iterable<TxNode> participants) throws XAException,
            IllegalStateException {
        final DtxResourceManagerContext dtxResourceManagerContext = getContext(txId);

        final DtxResourceManager dtxResourceManager = checkedGetResourceManager(dtxResourceManagerContext
                .getResourceManagerId());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Committing transaction; txId=" + txId + ", resId=" + dtxResourceManager.getId().toString());
        }

        final WritableTxJournal journal = journals.get(dtxResourceManager.getId());

        transactionLock.readLock().lock();
        try {
            if (shutdown) {
                throw new IllegalStateException("Shut down");
            }

            dtxResourceManager.commit(dtxResourceManagerContext);

            // logs to journal
            journal.writeCommit(txId, participants);

            // Update task status in the task keeper
            updateTask(txId, DtxTaskStatus.COMMITTED);

            // updates the last transaction ID
            updateAtomicLongToAtLeast(lastFinishedTxId, txId);

            if (COMMITTED.equals(dtxResourceManagerContext.getTxStatus())) {
                contextMap.remove(Long.valueOf(txId));
            }
        }
        catch (final IOException e) {
            LOGGER.error("Failed to write commit to journal; resourceID=" + dtxResourceManager.getId().toString()
                    + ",txID=" + txId, e);
            // TODO: add error treatment/return
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Rolls back the transaction at any moment after its {@link #start(TxMessage) start} and before it is
     * {@link #commit(long) committed}.
     * 
     * @param txId
     *            the transaction ID associated to the transaction context
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws XAException
     *             if the roll-back operation fails, with the following return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_NOTA} if no transaction with the given transaction ID exists</li>
     *             <li>{@link XAException#XAER_PROTO} if the given context is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             </ul>
     * @throws IllegalStateException
     *             if the {@link TransactionManager} was {@link #shutdown shut down}
     */
    final void rollback(final long txId, @Nonnull final Iterable<TxNode> participants) throws XAException,
            IllegalStateException {
        final DtxResourceManagerContext dtxResourceManagerContext = getContext(txId);

        final DtxResourceManager dtxResourceManager = checkedGetResourceManager(dtxResourceManagerContext
                .getResourceManagerId());

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Rolling back transaction; txId=" + txId + ", resId=" + dtxResourceManager.getId().toString());
        }

        final WritableTxJournal journal = journals.get(dtxResourceManager.getId());

        transactionLock.readLock().lock();
        try {
            if (shutdown) {
                throw new IllegalStateException("Shut down");
            }

            dtxResourceManager.rollback(dtxResourceManagerContext);

            // logs to journal
            journal.writeRollback(txId, 0, participants);

            // Update task status in the task keeper
            updateTask(txId, DtxTaskStatus.ROLLED_BACK);

            // updates the last transaction ID
            updateAtomicLongToAtLeast(lastFinishedTxId, txId);

            if (ROLLED_BACK.equals(dtxResourceManagerContext.getTxStatus())) {
                contextMap.remove(Long.valueOf(txId));
            }
        }
        catch (final IOException e) {
            LOGGER.error("Failed to write rollback to journal; resourceID=" + dtxResourceManager.getId().toString()
                    + ",txID=" + txId, e);
            // TODO: add error treatment/return
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets the transactional (read) lock status for the current thread.
     * 
     * The lock status returned applies primarily to the shared read lock taken by {@link #start(TxMessage, Iterable)},
     * {@link #prepare(long)}, {@link #commit(long, Iterable)} and {@link #rollback(long, Iterable)}, but it also
     * returns <code>true</code> if the transaction lock is write locked exclusively by the current thread. This should
     * be checked for all methods acquiring locks on DTX runtime states and which might be called from within a
     * transaction (e.g. {@link DtxManager#registerResourceManager(DtxResourceManager)}).
     * 
     * @return <code>true</code> if the current thread holds the transaction lock, <code>false</code> otherwise
     */
    final boolean holdsTransactionLock() {
        return transactionLock.getReadHoldCount() > 0 || transactionLock.isWriteLockedByCurrentThread();
    }

    /**
     * Gets the {@link DtxResourceManagerContext} from the {@link #contextMap} and throws an appropriate
     * {@link XAException} if none was found.
     * 
     * @param txId
     *            the transaction ID to search
     * @return a non-<code>null</code> {@link DtxResourceManagerContext}
     * @throws XAException
     *             {@link XAException#XAER_NOTA} if no transaction context was found for this ID
     */
    @Nonnull
    private final DtxResourceManagerContext getContext(final long txId) throws XAException {
        final DtxResourceManagerContext result = contextMap.get(Long.valueOf(txId));
        if (result == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        return result;
    }

    /**
     * Gets the {@link DtxResourceManager} from the {@link #resourceManagers} collection and throws an appropriate
     * {@link XAException} if none was found.
     * 
     * @param resourceId
     *            the ID under which the {@link DtxResourceManager} is registered
     * @return a non-<code>null</code> {@link DtxResourceManager}
     * @throws XAException
     *             {@link XAException#XAER_RMFAIL} (resource manager unavailable) if no {@link DtxResourceManager} was
     *             found for this ID
     */
    private final DtxResourceManager checkedGetResourceManager(final UUID resourceId) throws XAException {
        final DtxResourceManager result = this.getRegisteredResourceManager(resourceId);

        if (result == null) {
            throw new XAException(XAException.XAER_RMFAIL);
        }

        final DtxResourceManagerState syncState = states.get(resourceId);
        if (syncState == null || (!TX_AUTH_STATES.contains(syncState) && !syncReplayLock.isHeldByCurrentThread())) {
            throw new XAException(XAException.XAER_RMFAIL);
        }

        return result;
    }

    /**
     * Starts the instance.
     * 
     * @param txInit
     *            the {@link TransactionInitiator} to update if necessary
     * @throws IllegalThreadStateException
     *             if the current thread is currently executing a transaction, however improbable this may sound
     */
    final void startUp(final TransactionInitiator txInit) throws IllegalThreadStateException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting transaction manager");
        }

        // avoid deadlocks (and other niceties) by refusing to act from within a transaction
        if (holdsTransactionLock()) {
            throw new IllegalThreadStateException("Startup attempted from within a transaction.");
        }

        transactionLock.writeLock().lock();
        try {
            if (!shutdown) {
                return;
            }

            journalRotationMgr.start();

            for (final WritableTxJournal currJournal : journals.values()) {
                try {
                    currJournal.start();
                    updateAtomicLongToAtLeast(lastFinishedTxId, currJournal.getLastFinishedTxId());
                    readTasksFromJournal(currJournal.newReadOnlyTxJournal());
                }
                catch (final IOException e) {
                    LOGGER.error("Error while starting journal", e);
                    throw new IllegalStateException(e);
                }

                if (txInit != null) {
                    txInit.mergeLastTxCounters(lastFinishedTxId.get());
                }
            }
            dtxManager.startPurgeTaskKeeper();

            // reset all resource manager synchronization states
            for (final UUID currStateKey : states.keySet()) {
                setResManagerSyncState(currStateKey, UNDETERMINED);
            }

            shutdown = false;
        }
        finally {
            transactionLock.writeLock().unlock();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Started transaction manager; registered resource managers=" + resourceManagers);
        }

    }

    /**
     * Shuts down the instance properly.
     * 
     * @throws IllegalThreadStateException
     *             if the calling thread is executing a transaction and thus trying to shoot itself in the foot
     */
    final void shutdown() throws IllegalThreadStateException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Shutting down transaction manager");
        }

        // avoid deadlocks (and other niceties) by refusing to act from within a transaction
        if (holdsTransactionLock()) {
            throw new IllegalThreadStateException("Shutdown attempt from within a transaction.");
        }

        syncReplayLock.lock();
        try {
            transactionLock.writeLock().lock();
            try {
                if (shutdown) {
                    return;
                }

                // stops the rotation
                journalRotationMgr.stop();

                // closes journals
                for (final WritableTxJournal currJournal : journals.values()) {
                    try {
                        currJournal.stop();
                    }
                    catch (final IOException e) {
                        LOGGER.error("Error while closing journal", e);
                    }
                }
                dtxManager.stopPurgeTaskKeeper();

                // reset all resource manager synchronization states
                for (final UUID currStateKey : states.keySet()) {
                    setResManagerSyncState(currStateKey, UNDETERMINED);
                }

                this.shutdown = true;
            }
            finally {
                transactionLock.writeLock().unlock();
            }
        }
        finally {
            syncReplayLock.unlock();
        }

    }

    /**
     * Gets the shutdown state of this instance.
     * 
     * @return <code>true</code> if this {@link TransactionManager} is {@link #shutdown() shut down}, <code>false</code>
     *         otherwise
     */
    final boolean isShutdown() {
        return shutdown;
    }

    /**
     * Registers a {@link DtxResourceManager} with this instance.
     * 
     * @param resourceManager
     *            the {@link DtxResourceManager} to register
     * @param txInit
     *            the {@link TransactionInitiator} to update if necessary, may be <code>null</code> if this instance was
     *            not started
     * @throws IllegalArgumentException
     *             if a resource manager with identical ID is already registered
     * @throws IllegalStateException
     *             if the journal for the resource manager could not be initialized
     */
    final void registerResourceManager(final DtxResourceManager resourceManager, final TransactionInitiator txInit)
            throws IllegalArgumentException, IllegalStateException {

        // releases all read locks and waits for the write lock
        // Note: excludes a deadlock caused by having this method called within a transaction phase
        final int readHoldCount = transactionLock.getReadHoldCount();
        while (transactionLock.getReadHoldCount() > 0) {
            transactionLock.readLock().unlock();
        }

        // takes the write lock as we're about to modify the resource manager list
        transactionLock.writeLock().lock();
        try {
            // validate the input parameter
            final UUID resId = resourceManager.getId();
            if (resourceManagers.keySet().contains(resId)) {
                throw new IllegalArgumentException("Resource Manager already registered with ID=" + resId.toString());
            }

            // create and start the journal
            final String filename = newJournalFilePrefix(dtxManagerConfig.getLocalPeer().getNodeId(), resId);

            // TODO: add configuration value for rotation threshold
            final WritableTxJournal newJournal = new WritableTxJournal(dtxManagerConfig.getJournalDirectory().toFile(),
                    filename, 0, journalRotationMgr);
            // starts the journal only if the transaction manager is already started
            if (!shutdown) {
                try {
                    newJournal.start();
                    updateAtomicLongToAtLeast(lastFinishedTxId, newJournal.getLastFinishedTxId());
                }
                catch (final IOException e) {
                    LOGGER.error("Could not start journal for resource manager", e);
                    throw new IllegalStateException(e);
                }

                if (txInit != null) {
                    txInit.mergeLastTxCounters(lastFinishedTxId.get());
                }
            }
            this.journals.put(resId, newJournal);
            this.states.put(resId, UNDETERMINED);
            this.resourceManagers.put(resId, resourceManager);

            // Update task in the task keeper only if transaction manager is started
            if (!shutdown) {
                readTasksFromJournal(newJournal.newReadOnlyTxJournal());
            }

            dtxManager.postEvent(new DtxResourceManagerEvent(dtxManager, resourceManager.getId(), UNREGISTERED,
                    UNDETERMINED), true);

        }
        finally {
            // on the way out, re-acquire all previously held read locks before releasing the write lock
            try {
                for (int i = 0; i < readHoldCount; i++) {
                    transactionLock.readLock().lockInterruptibly();
                }
            }
            catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
            finally {
                transactionLock.writeLock().unlock();
            }
        }
    }

    /**
     * Unregisters a resource manager.
     * 
     * @param resourceManagerId
     *            the {@link UUID} of the {@link DtxResourceManager} to unregister
     * @throws IllegalStateException
     *             if the journal for the resource manager does not exist or cannot be stopped
     */
    final void unregisterResourceManager(final UUID resourceManagerId) throws IllegalStateException {

        // if we're in a transaction, release all read locks and wait for the write lock
        // Note: excludes a deadlock caused by having this method called within a transaction phase
        final int readHoldCount = transactionLock.getReadHoldCount();
        while (transactionLock.getReadHoldCount() > 0) {
            transactionLock.readLock().unlock();
        }

        // takes the write lock as we're about to modify the resource manager list
        transactionLock.writeLock().lock();
        try {

            if (!resourceManagers.keySet().contains(resourceManagerId)) {
                return;
            }

            final WritableTxJournal resJournal = journals.get(resourceManagerId);

            if (resJournal == null) {
                throw new IllegalStateException("No journal instance for resource manager; ID="
                        + resourceManagerId.toString());
            }

            try {
                resJournal.stop();
            }
            catch (final IOException e) {
                LOGGER.error("Could not stop journal for resource manager", e);
                throw new IllegalStateException(e);
            }

            this.journals.remove(resourceManagerId);
            final DtxResourceManagerState oldState = this.states.get(resourceManagerId);
            this.states.remove(resourceManagerId);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unregistering resource manager; resId=" + resourceManagerId);
            }
            this.resourceManagers.remove(resourceManagerId);

            dtxManager.postEvent(new DtxResourceManagerEvent(dtxManager, resourceManagerId, oldState, UNREGISTERED),
                    true);

        }
        finally {
            // on the way out, re-acquire all previously held read locks before releasing the write lock
            try {
                for (int i = 0; i < readHoldCount; i++) {
                    transactionLock.readLock().lockInterruptibly();
                }
            }
            catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
            finally {
                transactionLock.writeLock().unlock();
            }
        }
    }

    /**
     * Gets one of the registered {@link DtxResourceManager}s.
     * 
     * @param resMgrId
     *            the {@link UUID} of the {@link DtxResourceManager} to get
     * @return a {@link DtxResourceManager} instance or <code>null</code> if none was found
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    final DtxResourceManager getRegisteredResourceManager(@Nonnull final UUID resMgrId) throws NullPointerException {
        transactionLock.readLock().lock();
        try {
            return resourceManagers.get(resMgrId);
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets all registered {@link DtxResourceManager}s.
     * 
     * @return a (possibly empty) {@link Collection} of {@link DtxResourceManager}s
     */
    final Collection<DtxResourceManager> getRegisteredResourceManagers() {
        transactionLock.readLock().lock();
        try {
            return Collections.unmodifiableCollection(resourceManagers.values());
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Create or update a task in the task keeper corresponding to a given transaction.
     * 
     * @param dtxResourceManager
     *            the {@link DtxResourceManager} of the task
     * @param transaction
     *            the {@link TxMessage} to use
     * @param newStatus
     *            the {@link DtxTaskStatus} to set
     * @param timestamp
     *            a timestamp
     */
    private final void setTask(final DtxResourceManager dtxResourceManager, final TxMessage transaction,
            final DtxTaskStatus newStatus) {
        final UUID taskId = fromUuid(transaction.getTaskId());

        // Check if the resource manager is not null and if the task info was not already set
        if ((dtxResourceManager != null) && !dtxManager.isDtxTaskInfoSet(taskId)) {
            // dtx task info is created only if the resource manager is registered
            final DtxTaskInfo info = dtxResourceManager.createTaskInfo(transaction.getPayload().toByteArray());
            dtxManager.setTask(taskId, transaction.getTxId(), fromUuid(transaction.getResId()), newStatus, info);

        }
        else {
            // set a task with no dtx task info
            dtxManager.setTask(taskId, transaction.getTxId(), fromUuid(transaction.getResId()), newStatus, null);
        }
    }

    /**
     * Create or update a task in the task keeper corresponding to a given transaction.
     * 
     * @param dtxResourceManager
     *            the {@link DtxResourceManager} of the task
     * @param transaction
     *            the {@link TxMessage} to use
     * @param newStatus
     *            the {@link DtxTaskStatus} to set
     * @param timestamp
     *            a timestamp
     */
    final void loadTask(final DtxResourceManager dtxResourceManager, final TxMessage transaction,
            final DtxTaskStatus newStatus, final long timestamp) {

        try {
            final UUID taskId = fromUuid(transaction.getTaskId());

            // Check if the resource manager is not null and if the task info was not already set
            if (dtxResourceManager != null) {
                // dtx task info is created only if the resource manager is registered
                final DtxTaskInfo info = dtxResourceManager.createTaskInfo(transaction.getPayload().toByteArray());
                dtxManager.loadTask(taskId, transaction.getTxId(), fromUuid(transaction.getResId()), newStatus, info,
                        timestamp);

            }
            else {
                // set a task with no dtx task info
                dtxManager.loadTask(taskId, transaction.getTxId(), fromUuid(transaction.getResId()), newStatus, null,
                        timestamp);
            }
        }
        catch (final IllegalArgumentException e) {
            LOGGER.warn("Could not load task: " + e);
        }
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

    /**
     * Update the tasks in the task keeper by reading the journal.
     * 
     * @param journal
     *            the {@link JournalRecord} to read
     */
    final void readTasksFromJournal(final Iterable<JournalRecord> journal) {
        final Map<Long, TxJournalEntry> startedEntries = new HashMap<>();
        for (final JournalRecord currRecord : journal) {
            TxJournalEntry currEntry;
            TxJournalEntry startedEntry;
            try {
                currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            }
            catch (final InvalidProtocolBufferException e) {
                LOGGER.warn("Could not read journal entry; journal=" + journal);
                continue;
            }
            final long txId = currEntry.getTxId();

            switch (currEntry.getOp()) {
            case START:
                startedEntries.put(Long.valueOf(txId), currEntry);
                break;
            case COMMIT:
                startedEntry = startedEntries.get(Long.valueOf(txId));
                if (startedEntry != null) {
                    loadTask(dtxManager.getRegisteredResourceManager(fromUuid(startedEntry.getTx().getResId())),
                            startedEntry.getTx(), DtxTaskStatus.COMMITTED, startedEntry.getTimestamp());

                    startedEntries.remove(Long.valueOf(txId));
                }
                else {
                    LOGGER.warn("Transaction: " + currEntry.getTx() + " was not started before");
                }
                break;
            case ROLLBACK:
                startedEntry = startedEntries.get(Long.valueOf(txId));
                if (startedEntry != null) {
                    loadTask(dtxManager.getRegisteredResourceManager(fromUuid(startedEntry.getTx().getResId())),
                            startedEntry.getTx(), DtxTaskStatus.ROLLED_BACK, startedEntry.getTimestamp());
                    startedEntries.remove(Long.valueOf(txId));
                }
                else {
                    LOGGER.warn("Transaction: " + currEntry.getTx() + " was not started before");
                }
                break;
            default:
                // nothing
                break;
            }

        }
        // Add tasks with only the started status
        for (final Entry<Long, TxJournalEntry> entry : startedEntries.entrySet()) {
            loadTask(dtxManager.getRegisteredResourceManager(fromUuid(entry.getValue().getTx().getResId())), entry
                    .getValue().getTx(), DtxTaskStatus.STARTED, entry.getValue().getTimestamp());
        }

    }

    /**
     * Read a complete task in the journal. Return a task with status unknown if the task is not found
     * 
     * @param taskId
     *            the task {@link UUID} to read
     * @return a {@link TaskLoader}
     */
    final TaskLoader readTask(final UUID taskId) {
        TaskLoader result = null;
        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted while searching task; taskID=" + taskId);
        }
        try {
            // searches writable journals, limited to the current file
            for (final WritableTxJournal currJournal : journals.values()) {
                result = readTaskFromJournal(taskId, currJournal);
                if (result != null) {
                    return result;
                }
            }

            // read all journals from the start including all backups
            for (final WritableTxJournal currJournal : journals.values()) {
                result = readTaskFromJournal(taskId, currJournal.newReadOnlyTxJournal());
                if (result != null) {
                    return result;
                }
            }
            LOGGER.warn("Could not find task status, returning; taskId=" + taskId + ", status=" + result);
            return TaskLoader.createUnknownTask(taskId);
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Read a complete task in the journal. Return null if not found
     * 
     * @param taskId
     *            the task {@link UUID} to read
     * @param journal
     *            the journal {@link JournalRecord} to read
     * @return a {@link TaskLoader} or null
     */
    private final TaskLoader readTaskFromJournal(final UUID taskId, final Iterable<JournalRecord> journal) {

        long foundTxId = DEFAULT_LAST_TX_VALUE;
        TaskLoader result = null;

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reading journal; wanted taskId=" + taskId + ", journal=" + journal);
        }
        TxJournalEntry resultEntry = null;
        for (final JournalRecord currRecord : journal) {
            TxJournalEntry currEntry;
            try {
                currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            }
            catch (final InvalidProtocolBufferException e) {
                LOGGER.warn("Could not read journal entry; journal=" + journal);
                continue;
            }
            switch (currEntry.getOp()) {
            case START:
                // checks if the task ID in the transaction message matches
                if (taskId.equals(fromUuid(currEntry.getTx().getTaskId()))) {
                    resultEntry = currEntry;
                    foundTxId = currEntry.getTxId();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found start in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                }
                break;
            case COMMIT:
                // checks if the commit belongs to the identified transaction
                if ((resultEntry != null) && TxOpCode.START.equals(resultEntry.getOp())
                        && (foundTxId == currEntry.getTxId())) {
                    result = createTaskLoader(resultEntry, DtxTaskStatus.COMMITTED);

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found commit in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                    return result;
                }
                break;
            case ROLLBACK:
                // checks if the rollback belongs to the identified transaction
                if ((resultEntry != null) && TxOpCode.START.equals(resultEntry.getOp())
                        && (foundTxId == currEntry.getTxId())) {
                    result = createTaskLoader(resultEntry, DtxTaskStatus.ROLLED_BACK);

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found rollback in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                    return result;
                }
                break;
            default:
                // nothing
            }
        }

        if (resultEntry != null) {
            result = createTaskLoader(resultEntry, DtxTaskStatus.STARTED);
        }
        return result;
    }

    private TaskLoader createTaskLoader(final TxJournalEntry resultEntry, final DtxTaskStatus status) {

        DtxTaskInfo info = null;
        final DtxTaskAdm taskAdm = new DtxTaskAdm(fromUuid(resultEntry.getTx().getTaskId()), null, null,
                fromUuid(resultEntry.getTx().getResId()), status);
        final DtxResourceManager resourceManager = dtxManager.getRegisteredResourceManager(fromUuid(resultEntry.getTx()
                .getResId()));

        if (resourceManager != null) {
            info = resourceManager.createTaskInfo(resultEntry.getTx().getPayload().toByteArray());
        }
        return new TaskLoader(taskAdm, info);
    }

    /**
     * Searches for a given task in all available journals and returns its status.
     * 
     * @param taskId
     *            the {@link UUID} of the requested task
     * @return a valid {@link DtxTaskStatus}, {@link DtxTaskStatus#UNKNOWN} if it was not found
     */
    final DtxTaskStatus searchTaskStatus(final UUID taskId) {
        DtxTaskStatus result = UNKNOWN;

        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted while searching task; taskID=" + taskId);
        }
        try {
            // searches writable journals, limited to the current file
            for (final WritableTxJournal currJournal : journals.values()) {
                result = readTaskStatusFromJournal(taskId, currJournal);
                if (!UNKNOWN.equals(result)) {
                    return result;
                }
            }

            // read all journals from the start including all backups
            for (final WritableTxJournal currJournal : journals.values()) {
                result = readTaskStatusFromJournal(taskId, currJournal.newReadOnlyTxJournal());
                if (!UNKNOWN.equals(result)) {
                    return result;
                }
            }
            LOGGER.warn("Could not find task status, returning; taskId=" + taskId + ", status=" + result);
            return result;
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    private final DtxTaskStatus readTaskStatusFromJournal(final UUID taskId, final Iterable<JournalRecord> journal) {
        DtxTaskStatus result = UNKNOWN;
        long foundTxId = DEFAULT_LAST_TX_VALUE;
        boolean keepSearching = true;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Reading journal; wanted taskId=" + taskId + ", journal=" + journal);
        }
        for (final JournalRecord currRecord : journal) {
            TxJournalEntry currEntry;
            try {
                currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            }
            catch (final InvalidProtocolBufferException e) {
                LOGGER.warn("Could not read journal entry; journal=" + journal);
                continue;
            }
            switch (currEntry.getOp()) {
            case START:
                // checks if the task ID in the transaction message matches
                if (taskId.equals(fromUuid(currEntry.getTx().getTaskId()))) {
                    result = STARTED;
                    foundTxId = currEntry.getTxId();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found start in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                }
                break;
            case COMMIT:
                // checks if the commit belongs to the identified transaction
                if (STARTED.equals(result) && (foundTxId == currEntry.getTxId())) {
                    result = COMMITTED;
                    keepSearching = false;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found commit in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                }
                break;
            case ROLLBACK:
                // checks if the rollback belongs to the identified transaction
                if (STARTED.equals(result) && (foundTxId == currEntry.getTxId())) {
                    result = ROLLED_BACK;
                    keepSearching = false;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Found rollback in journal; taskId=" + taskId + ", txId=" + foundTxId);
                    }
                }
                break;
            default:
                // nothing
            }
            if (!keepSearching) {
                break;
            }
        }
        return result;
    }

    /**
     * Gets the ID of the last completed transaction for this instance.
     * 
     * @return a non-negative transaction ID or {@value #DEFAULT_LAST_TX_VALUE} if non was executed yet
     * @throws IllegalStateException
     *             if this instance is not {@link #startUp() started}
     */
    final long getLastCompleteTxId() throws IllegalStateException {
        if (shutdown) {
            throw new IllegalStateException("Not started");
        }
        return lastFinishedTxId.longValue();
    }

    /**
     * Gets the last complete transaction for a given {@link #registerResourceManager(DtxResourceManager) registered}
     * resource manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return a positive transaction ID or {@value #DEFAULT_LAST_TX_VALUE} if no transaction has been completed yet or
     *         the resource manager is not registered with this instance
     * @throws IllegalStateException
     *             if the resource manager is unknown or processing is interrupted
     */
    final long getLastCompleteTxIdForResMgr(final UUID resId) throws IllegalStateException {
        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            // wraps the interruption into a runtime exception
            throw new IllegalStateException("Interrupted on waiting for lock", e);
        }
        try {
            final WritableTxJournal targetJournal = journals.get(resId);
            if (targetJournal == null) {
                return DEFAULT_LAST_TX_VALUE;
            }
            return targetJournal.getLastFinishedTxId();
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets a registered resource manager's status.
     * 
     * @param resUuid
     *            the requested resource manager's {@link UUID}
     * @return a valid {@link DtxResourceManagerState} if the resource manager is registered, <code>null</code>
     *         otherwise
     */
    final DtxResourceManagerState getResourceManagerState(@Nonnull final UUID resUuid) {
        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted.");
        }
        try {
            return states.get(resUuid);
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets the information of the journal for a given {@link #registerResourceManager(DtxResourceManager) registered}
     * resource manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return a String describing the journal
     * 
     * @throws IllegalStateException
     *             if the resource manager is unknown or processing is interrupted
     */
    final String getJournalPathForResMgr(final UUID resId) throws IllegalStateException {
        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            // wraps the interruption into a runtime exception
            throw new IllegalStateException("Interrupted on waiting for lock", e);
        }
        try {
            final WritableTxJournal targetJournal = journals.get(resId);
            if (targetJournal == null) {
                return "";
            }
            return targetJournal.getJournalFilename();
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets the status of the journal for a given {@link #registerResourceManager(DtxResourceManager) registered}
     * resource manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return true if the journal is started.
     * 
     * @throws IllegalStateException
     *             if the resource manager is unknown or processing is interrupted
     */
    final boolean getJournalStatusForResMgr(final UUID resId) throws IllegalStateException {
        try {
            transactionLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            // wraps the interruption into a runtime exception
            throw new IllegalStateException("Interrupted on waiting for lock", e);
        }
        try {
            final WritableTxJournal targetJournal = journals.get(resId);
            if (targetJournal == null) {
                return false;
            }
            return targetJournal.isStarted();
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Gets the configured local node from the configuration.
     * 
     * @return a {@link DtxNode} representing the local node
     * @throws IllegalStateException
     *             if this instance is {@link #shutdown() shut down}
     */
    final DtxNode getLocalNode() throws IllegalStateException {
        transactionLock.readLock().lock();
        try {
            if (shutdown) {
                throw new IllegalStateException("Not started");
            }
            return this.dtxManagerConfig.getLocalPeer();
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Evaluates and updates the synchronization state of the given resource manager according to the given discovered
     * last transaction ID and the presence of a quorum.
     * 
     * @param resUuid
     *            the target resource manager's {@link UUID}
     * @param lastDiscTxId
     *            the last discovered transaction ID relative to which the synchronization status is to be updated
     * @param quorumPresent
     *            <code>true</code> if the last transaction ID was discovered with responses from a quorum of nodes,
     *            <code>false</code> otherwise
     */
    final void evaluateResManagerSyncState(final UUID resUuid, final long lastDiscTxId, final boolean quorumPresent) {

        syncStateLock.lock();
        try {
            if (!resourceManagers.containsKey(resUuid)) {
                return;
            }

            final DtxResourceManagerState oldState;
            final DtxResourceManagerState newState;
            transactionLock.readLock().lock();
            try {
                final long localLastTxId = getLastCompleteTxIdForResMgr(resUuid);

                oldState = states.get(resUuid);

                if (localLastTxId < lastDiscTxId) {
                    // check if we're already synchronizing
                    if (SYNCHRONIZING == oldState || LATE == oldState) {
                        // don't revert to LATE or trigger any additional event
                        return;
                    }

                    if (POST_SYNC_PROCESSING == oldState) {
                        dtxManager.getPostSyncProcessor().resetAfterPostProc(resUuid);
                        return;
                    }

                    newState = LATE;
                }
                else {
                    if (quorumPresent) {
                        // transitions to up-to-date only if a quorum responded
                        newState = UP_TO_DATE == oldState ? UP_TO_DATE : POST_SYNC_PROCESSING;
                    }
                    else {
                        // leave things unchanged
                        newState = oldState;
                    }
                }

                if ((oldState == null) || (oldState == newState)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Keeping old synchronization status; status=" + oldState + ", node="
                                + dtxManagerConfig.getLocalPeer() + ", resId=" + resUuid);
                    }
                    return;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Setting new synchronization status; previous=" + oldState + ", update=" + newState
                            + ", node=" + dtxManagerConfig.getLocalPeer());
                }
                states.put(resUuid, newState);

            }
            finally {
                transactionLock.readLock().unlock();
            }
            dtxManager.postEvent(new DtxResourceManagerEvent(dtxManager, resUuid, oldState, newState), true);
        }
        finally {
            syncStateLock.unlock();
        }

    }

    /**
     * Sets a new state for a resource manager.
     * 
     * This does nothing if the resource manager is not registered or already in the target state.
     * 
     * @param resUuid
     *            the target resource manager's {@link UUID}
     * @param newState
     *            the new {@link DtxResourceManagerState} after the transition
     */
    final void setResManagerSyncState(final UUID resUuid, final DtxResourceManagerState newState) {

        syncStateLock.lock();
        try {
            final DtxResourceManagerState oldState;
            transactionLock.readLock().lock();
            try {
                oldState = states.get(resUuid);
                if ((oldState == null) || (oldState == newState)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Keeping old synchronization status; status=" + oldState + ", node="
                                + dtxManagerConfig.getLocalPeer() + ", resId=" + resUuid);
                    }
                    return;
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Setting new synchronization status; previous=" + oldState + ", update=" + newState
                            + ", node=" + dtxManagerConfig.getLocalPeer());
                }
                states.put(resUuid, newState);
            }
            finally {
                transactionLock.readLock().unlock();
            }
            dtxManager.postEvent(new DtxResourceManagerEvent(dtxManager, resUuid, oldState, newState), true);
        }
        finally {
            syncStateLock.unlock();
        }
    }

    /**
     * Gets the IDs and of all resource managers whose state is {@link DtxResourceManagerState#UNDETERMINED}.
     * 
     * @return a (possibly empty) {@link Map} of {@link UUID} and last complete transaction ID
     */
    final Map<UUID, Long> getUndeterminedResourceManagers() {
        transactionLock.readLock().lock();
        try {
            final HashMap<UUID, Long> result = new HashMap<UUID, Long>();
            for (final UUID currId : states.keySet()) {
                if (UNDETERMINED == states.get(currId)) {
                    result.put(currId, Long.valueOf(getLastCompleteTxIdForResMgr(currId)));
                }
            }
            return result;
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Replay the given transactions on the specified resource manager.
     * 
     * @param resMgrId
     *            the target resource manager's {@link UUID}
     * @param journalDiff
     *            the set of transactions to replay in iteration order
     * @param expTxNb
     *            expected number of transactions to be replayed
     * @return the transaction ID of the last successfully replayed transaction
     */
    @ParametersAreNonnullByDefault
    final long replayTransactions(final UUID resMgrId, final Iterable<TxJournalEntry> journalDiff, final int expTxNb) {
        long result = DEFAULT_LAST_TX_VALUE;
        try {
            syncReplayLock.lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted on waiting for sync lock", e);
        }
        try {
            result = getLastCompleteTxIdForResMgr(resMgrId);

            checkedGetResourceManager(resMgrId);

            final BitSet failList = new BitSet(expTxNb);

            for (final TxJournalEntry currEntry : journalDiff) {
                final int failIndex = (int) (currEntry.getTxId() % expTxNb);

                final long currTxId = currEntry.getTxId();

                // skips ahead if the current transaction has already been replayed
                if (currTxId <= result) {
                    continue;
                }

                switch (currEntry.getOp()) {
                case START:
                    try {
                        // executes start if its context is not already registered
                        final DtxResourceManagerContext txCxt = contextMap.get(Long.valueOf(currTxId));

                        if (txCxt == null) {
                            start(currEntry.getTx(), currEntry.getTxNodesList());
                            prepare(currTxId);
                            break;
                        }
                        // handles the existing context case
                        switch (txCxt.getTxStatus()) {
                        case UNKNOWN:
                        case PENDING:
                            start(currEntry.getTx(), currEntry.getTxNodesList());
                            //$FALL-THROUGH$
                        case STARTED:
                            prepare(currTxId);
                            break;
                        case PREPARED:
                        case COMMITTED:
                        case ROLLED_BACK:
                        default:
                            // nothing
                        }
                    }
                    catch (final XAException xe) {
                        LOGGER.warn("Exception while replaying first phase; errorCode=" + xe.errorCode);
                        failList.set(failIndex);
                    }
                    break;
                case COMMIT:
                    if (failList.get(failIndex)) {
                        // does not commit if previous phases failed
                        throw new IllegalStateException("Commit of failed transaction; txId=" + currTxId);
                    }
                    commit(currTxId, currEntry.getTxNodesList());
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Committed transaction; txId=" + currTxId);
                    }
                    break;
                case ROLLBACK:
                    if (failList.get(failIndex)) {
                        // ignores rolled back transactions if start or prepare failed
                        updateAtomicLongToAtLeast(lastFinishedTxId, currTxId);
                        failList.clear(failIndex);
                        break;
                    }
                    rollback(currTxId, currEntry.getTxNodesList());
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Rolled back transaction; txId=" + currTxId);
                    }
                    break;
                default:
                    // nothing
                }
                result = getLastCompleteTxIdForResMgr(resMgrId);
            }

            if (!failList.isEmpty()) {
                throw new IllegalStateException("There are failed transactions on replay");
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Replayed transactions; nodeId=" + dtxManager.getNodeId() + ", lastTxId=" + result);
            }
            return result;
        }
        catch (final IllegalStateException e) {
            LOGGER.warn("Exception while replaying", e);
            return result;
        }
        catch (final XAException e) {
            LOGGER.warn("XAException while replaying; code=" + e.errorCode);
            return result;
        }
        finally {
            syncReplayLock.unlock();
        }
    }

    /**
     * Extract transaction journal records between two given transaction IDs.
     * 
     * @param resMgrId
     *            the target resource manager's
     * @param firstTxId
     *            the transaction ID from which to extract (exclusive)
     * @param lastTxId
     *            the last transaction ID to which to extract
     * @return an {@link Iterable<JournalRecord>} containing all or a subset of the requested transactions
     */
    final Iterable<TxJournalEntry> extractTransactions(final UUID resMgrId, final long firstTxId, final long lastTxId) {
        transactionLock.readLock().lock();
        try {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Extracting transactions; resourceId=" + resMgrId + ", firstTxId=" + firstTxId
                        + ", lastTxId=" + lastTxId);
            }
            final ArrayList<TxJournalEntry> result = new ArrayList<TxJournalEntry>((int) (2 * (lastTxId - firstTxId)));
            final WritableTxJournal targetJournal = journals.get(resMgrId);
            if (targetJournal == null) {
                return result;
            }
            for (final JournalRecord currRecord : targetJournal.newReadOnlyTxJournal()) {
                final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
                final long txId = currEntry.getTxId();
                if ((firstTxId < txId) && (txId <= lastTxId)) {
                    result.add(currEntry);
                }
            }
            return result;
        }
        catch (final InvalidProtocolBufferException e) {
            LOGGER.warn("Exception reading journal", e);
            return new ArrayList<TxJournalEntry>();
        }
        finally {
            transactionLock.readLock().unlock();
        }
    }
}
