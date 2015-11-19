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

import static io.eguan.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static io.eguan.dtx.TransactionManager.newJournalFilePrefix;
import static io.eguan.dtx.proto.TxProtobufUtils.fromUuid;
import static io.eguan.dtx.proto.TxProtobufUtils.toUuid;
import static io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode.START;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidConfigurationContext.ContextTestHelper;
import io.eguan.dtx.DtxConstants;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerState;
import io.eguan.dtx.TransactionManager;
import io.eguan.dtx.DtxEventListeners.StateCountListener;
import io.eguan.dtx.config.DtxConfigurationContext;
import io.eguan.dtx.config.TestValidDtxConfigurationContext;
import io.eguan.dtx.journal.JournalRecord;
import io.eguan.dtx.journal.JournalRotationManager;
import io.eguan.dtx.journal.WritableTxJournal;
import io.eguan.dtx.proto.TxProtobufUtils;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry;
import io.eguan.proto.dtx.DistTxWrapper.TxMessage;
import io.eguan.proto.dtx.DistTxWrapper.TxNode;
import io.eguan.utils.Strings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Helper class for all tests related to the DTX subsystem.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class DtxTestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtxTestHelper.class);

    /**
     * Default Hazelcast listening port.
     */
    public static final int DEFAULT_HZ_PORT = 12341;

    /**
     * Default offset to add to port numbers when instantiating several Hazelcast peers.
     */
    private static final int HZ_PORT_OFFSET = 10;

    private static final int HZ_PORT_UPPER_LIMIT = 30000;

    private static final int HZ_PORT_SEED = 5000;

    private static final AtomicInteger HZ_PORT;
    static {
        HZ_PORT = new AtomicInteger(DEFAULT_HZ_PORT + new Random(System.currentTimeMillis()).nextInt(HZ_PORT_SEED));
    }

    private static final long DEFAULT_TX_TIMEOUT_MS = 10000;

    private static final int PARTICIPANT_COUNT = 5;

    private static final int STATE_UPDATE_WAIT_TIMEOUT_S = 10;

    /**
     * Artificial transaction ID for testing purposes.
     */
    private static final AtomicLong TX_ID = new AtomicLong(0);

    /**
     * The default always available loopback network host address.
     */
    private static final InetAddress LOCALHOST = InetAddress.getLoopbackAddress();

    private static final MetaConfiguration DEFAULT_CONFIG;
    static {
        final Properties props = new TestValidDtxConfigurationContext().getTestHelper().getDefaultConfig();
        MetaConfiguration defaultConfig = null;
        try {
            defaultConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(props),
                    DtxConfigurationContext.getInstance());
        }
        catch (NullPointerException | IllegalArgumentException | IOException | ConfigValidationException e) {
            LOGGER.error("Default test configuration initialization failed", e);
        }
        finally {
            DEFAULT_CONFIG = defaultConfig;
        }
    }

    /**
     * Gets the default configuration satisfying the requirements of {@link DtxConfigurationContext}.
     * 
     * @return a {@link MetaConfiguration} or <code>null</code> if its initialization failed
     */
    public static final MetaConfiguration getDefaultConfiguration() {
        return DEFAULT_CONFIG;
    }

    /**
     * Default transaction content.
     */
    public static final TxMessage DEFAULT_TX_MESSAGE = TxMessage.newBuilder().setVersion(ProtocolVersion.VERSION_1)
            .setTxId(DtxTestHelper.nextTxId()).setTaskId(toUuid(UUID.randomUUID()))
            .setInitiatorId(toUuid(UUID.randomUUID())).setResId(toUuid(UUID.randomUUID()))
            .setPayload(ByteString.copyFrom(DtxDummyRmFactory.DEFAULT_PAYLOAD)).setTimeout(DEFAULT_TX_TIMEOUT_MS)
            .build();

    /**
     * Private constructor.
     */
    private DtxTestHelper() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Gets the default minimal valid configuration for constructing a {@link DtxManager}.
     * 
     * @param journalDir
     *            the journal directory
     * 
     * @return a functional {@link DtxManagerConfig} instance
     */
    static final DtxManagerConfig newDtxManagerConfig(final Path journalDir) {
        final DtxNode localPeerAddr = new DtxNode(UUID.randomUUID(), new InetSocketAddress(LOCALHOST,
                nextHazelcastPort()));

        return newDtxManagerConfig(localPeerAddr, journalDir);
    }

    /**
     * Creates a {@link DtxManagerConfig} instance with the given local and remote peers.
     * 
     * @param localPeer
     *            a non-<code>null</code> {@link DtxNode}
     * @param journalDir
     *            the journal directory
     * @param remotePeers
     *            a list of {@link DtxNode}s describing the remote peers to which the local peer connects
     * @return a valid {@link DtxManagerConfig} instance
     */
    static final DtxManagerConfig newDtxManagerConfig(final DtxNode localPeer, final Path journalDir,
            final DtxNode... remotePeers) {

        /*
         * computes a unique hash from the sorted set of peers to avoid interfering with simultaneously running test
         * clusters
         */
        final Set<DtxNode> peerSet = new TreeSet<DtxNode>(new Comparator<DtxNode>() {
            @Override
            public int compare(final DtxNode o1, final DtxNode o2) {
                return o1.getNodeId().compareTo(o2.getNodeId());
            }
        });
        peerSet.addAll(Arrays.asList(remotePeers));
        peerSet.add(localPeer);
        String configKey = "";
        try {
            final MessageDigest digester = MessageDigest.getInstance("MD5");
            for (final DtxNode currNode : peerSet) {
                digester.update(currNode.toString().getBytes());
            }
            configKey = Strings.toHexString(digester.digest());
        }
        catch (final NoSuchAlgorithmException e) {
            LOGGER.warn("Exception computing cluster config hash", e);
            // nothing
        }

        return new DtxManagerConfig(getDefaultConfiguration(), journalDir, "testCluster-" + configKey, configKey,
                localPeer, remotePeers);

    }

    /**
     * Constructs a new Hazelcast cluster with random properties (node IDs and ports).
     * 
     * Note: All addresses will point to {@link #LOCALHOST} and use partly random port numbers designed to minimize
     * collisions when running tests in parallel.
     * 
     * @param nbOfNodes
     *            the total number of nodes in the cluster
     * @return a {@link Set} of distinct {@link DtxNode}s
     */
    static final Set<DtxNode> newRandomCluster(final int nbOfNodes) {
        final HashSet<DtxNode> result = new HashSet<DtxNode>(nbOfNodes);
        for (int i = 0; i < nbOfNodes; i++) {
            result.add(new DtxNode(UUID.randomUUID(), new InetSocketAddress(LOCALHOST, nextHazelcastPort())));
        }
        return result;
    }

    private static final int nextHazelcastPort() {
        final int oldPort = HZ_PORT.get();
        if (oldPort + HZ_PORT_OFFSET > HZ_PORT_UPPER_LIMIT) {
            HZ_PORT.set(DEFAULT_HZ_PORT + new Random(System.currentTimeMillis()).nextInt(HZ_PORT_SEED));
        }
        return HZ_PORT.addAndGet(HZ_PORT_OFFSET);
    }

    /**
     * Registers the given {@link DtxResourceManager} with the {@link TransactionManager}.
     * 
     * @param txMgr
     *            the {@link TransactionManager} to register with
     * @param resMgr
     *            the {@link DtxResourceManager} to register, defaults to
     *            {@link DtxDummyRmFactory#newResMgrThatDoesEverythingRight(UUID)} if given <code>null</code>
     * @param syncState
     *            the {@link DtxResourceManagerState} to set, defaults to {@link DtxResourceManagerState#UP_TO_DATE} if
     *            <code>null</code>
     * @return the registered {@link DtxResourceManager}'s {@link UUID}
     * @throws XAException
     *             if {@link DtxDummyRmFactory#newResMgrThatDoesEverythingRight(UUID)} fails
     */
    static final UUID registerResMgrWithTxMgr(final TransactionManager txMgr, final DtxResourceManager resMgr,
            final DtxResourceManagerState syncState) throws XAException {

        final DtxResourceManager targetResMgr = resMgr == null ? DtxDummyRmFactory
                .newResMgrThatDoesEverythingRight(null) : resMgr;

        final UUID resUuid = targetResMgr.getId();

        txMgr.registerResourceManager(targetResMgr, null);
        assertEquals(targetResMgr, txMgr.getRegisteredResourceManager(resUuid));

        txMgr.setResManagerSyncState(resUuid, syncState == null ? UP_TO_DATE : syncState);

        return resUuid;
    }

    /**
     * Builds a minimal transaction with {@link #DEFAULT_TX_MESSAGE} and the next available transaction ID.
     * 
     * @param resUuid
     *            the {@link DtxResourceManager}'s ID to include in the transaction
     * @return a valid {@link TxMessage}
     */
    static final TxMessage buildDefaultTransaction(@Nonnull final UUID resUuid) {

        return TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(nextTxId())
                .setResId(toUuid(resUuid)).build();
    }

    /**
     * Awaits the first change of the given resource manager to a target state.
     * 
     * @param targetDtxMgr
     *            the {@link DtxManager} having registered the resource manager
     * @param resMgrId
     *            the {@link UUID} of the resource manager
     * @param targetState
     *            the {@link DtxResourceManagerState} to wait for
     * @throws InterruptedException
     *             if interrupted while waiting
     * @throws TimeoutException
     *             if the target state has not been reached after a fixed timeout of
     *             {@value #STATE_UPDATE_WAIT_TIMEOUT_S} seconds.
     */
    static final void awaitStateUpdate(final DtxManager targetDtxMgr, final UUID resMgrId,
            final DtxResourceManagerState targetState) throws InterruptedException, TimeoutException {

        final DtxResourceManagerState currState = targetDtxMgr.getResourceManagerState(resMgrId);
        if (targetState == currState) {
            return;
        }

        final CountDownLatch targetStateLatch = new CountDownLatch(1);
        final HashMultimap<UUID, DtxManager> resMgrMap = HashMultimap.create();
        resMgrMap.put(resMgrId, targetDtxMgr);
        final StateCountListener upToDateListener = new StateCountListener(targetStateLatch, targetState, resMgrMap);
        targetDtxMgr.registerDtxEventListener(upToDateListener);

        try {
            if (!targetDtxMgr.isStarted()) {
                targetDtxMgr.start();
            }

            if (!targetStateLatch.await(STATE_UPDATE_WAIT_TIMEOUT_S, SECONDS)) {
                // check once more in case the listener failed to detect the status change
                if (targetState != targetDtxMgr.getResourceManagerState(resMgrId)) {
                    throw new TimeoutException("Waiting for target state timed out; targetState=" + targetState);
                }
            }
        }
        finally {
            targetDtxMgr.unregisterDtxEventListener(upToDateListener);
        }

    }

    /**
     * Gets the next valid transaction ID.
     * 
     * IDs produced by this method are guaranteed to be unique, positive and greater than all previous values (excluding
     * overflows).
     * 
     * @return a positive long
     */
    public static final long nextTxId() {
        return TX_ID.incrementAndGet();
    }

    /**
     * Generates a {@link Set} of {@link TxNode}s for journalled operations on {@link TransactionManager}s.
     * 
     * @return a {@link Set} with {@value #PARTICIPANT_COUNT} {@link DtxNode}s pointing to localhost on different ports
     *         with random IDs
     */
    public static final Set<TxNode> newRandomParticipantsSet() {
        final HashSet<TxNode> result = new HashSet<TxNode>();
        for (int i = 0; i < PARTICIPANT_COUNT; i++) {
            final DtxNode dtxNode = new DtxNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", DEFAULT_HZ_PORT
                    + (i * HZ_PORT_OFFSET)));
            result.add(TxProtobufUtils.toTxNode(dtxNode));
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * Writes any number of complete transactions (i.e. start, [commit|rollback]) to the given journal.
     * 
     * @param target
     *            a {@link WritableTxJournal} to which to write
     * @param numberOfTx
     *            the number of transactions to write
     * @param resUuid
     *            the {@link UUID} to include as resource manager ID, random if <code>null</code>
     * @param participants
     *            the {@link Set} of participating TxNodes to include
     * @return the ID of the last written transaction
     * @throws IllegalStateException
     *             if the journal is not in a state that allows writing to it
     * @throws IOException
     *             if writing to the journal fails
     */
    public static final long writeCompleteTransactions(final WritableTxJournal target, final int numberOfTx,
            final UUID resUuid, final Set<TxNode> participants) throws IllegalStateException, IOException {
        long txId = 0;

        final Uuid resId = TxProtobufUtils.toUuid(resUuid == null ? UUID.randomUUID() : resUuid);

        for (int i = 0; i < numberOfTx; i++) {
            txId = DtxTestHelper.nextTxId();
            final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                    .setTxId(txId).setResId(resId).setTaskId(TxProtobufUtils.toUuid(UUID.randomUUID())).build();

            target.writeStart(defTx, participants);

            // writes commits and rollbacks for every other transaction
            if (i % 2 == 0) {
                target.writeRollback(txId, -1, participants);
            }
            else {
                target.writeCommit(txId, participants);
            }
        }
        return txId;
    }

    /**
     * Read any number of complete transactions (i.e. start, [commit|rollback]) to the given journal.
     * 
     * @param target
     *            a {@link WritableTxJournal} to which to read
     * @return an array List with the task ID
     */
    public static final ArrayList<UUID> readCompleteTransactions(final WritableTxJournal target) {

        final ArrayList<UUID> taskLists = new ArrayList<UUID>();

        for (final JournalRecord currRecord : target) {
            TxJournalEntry currEntry;
            try {
                currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            }
            catch (final InvalidProtocolBufferException e) {
                LOGGER.warn("Could not read journal entry; journal=" + target);
                continue;
            }
            if (currEntry.getOp().equals(START)) {
                final UUID taskId = fromUuid(currEntry.getTx().getTaskId());
                taskLists.add(taskId);
            }
        }
        return taskLists;
    }

    /**
     * Prepares, i.e. writes transactions to a set of journals according to the information given as a {@link Table}.
     * 
     * @param dtxMgrTxTable
     *            (sorted) {@link TreeBasedTable} mapping resource manager {@link UUID}s, last transaction IDs to
     *            {@link DtxManager} instances
     * @param journalDirMap
     *            a {@link Map} providing the temporary directories for each {@link DtxManager}
     * @param setupRotMgr
     *            a central {@link JournalRotationManager} used to write the prepared journals
     * @return a {@link Table} of non-failing mock {@link DtxResourceManager}s with their last transaction ID and
     *         journal file directories
     * @throws IllegalStateException
     *             if writing to journals fails due to their internal state
     * @throws IOException
     *             if writing or reading data fails
     * @throws XAException
     *             if mock setup fails
     */
    public static final Table<DtxResourceManager, Long, Path> prepareExistingJournals(
            final TreeBasedTable<Long, UUID, DtxManager> dtxMgrTxTable, final Map<DtxManager, Path> journalDirMap,
            final JournalRotationManager setupRotMgr) throws IllegalStateException, IOException, XAException {

        // reference map to order resource managers by their last tx ID
        final Map<DtxResourceManager, Long> rankMap = new HashMap<DtxResourceManager, Long>();

        final Comparator<DtxResourceManager> rowComp = new Comparator<DtxResourceManager>() {

            @Override
            public final int compare(final DtxResourceManager o1, final DtxResourceManager o2) {
                // compare last tx IDs before falling back to classic comparison
                final Long rank1 = rankMap.get(o1);
                final Long rank2 = rankMap.get(o2);
                final int rankComp = Long.compare(rank1.longValue(), rank2.longValue()) * -1;
                if (rankComp != 0) {
                    return rankComp;
                }

                // fall back to comparing IDs
                final UUID id1 = o1.getId();
                final UUID id2 = o2.getId();
                final int idComp = id1.compareTo(id2);

                // maintain coherence with equals()
                if (idComp == 0) {
                    return Integer.compare(o1.hashCode(), o2.hashCode());
                }
                return idComp;
            }
        };

        final Comparator<Long> columnComp = Collections.reverseOrder();

        final Table<DtxResourceManager, Long, Path> result = TreeBasedTable.create(rowComp, columnComp);

        final HashMap<UUID, DtxManager> previousLog = new HashMap<UUID, DtxManager>();

        long lastTxId = TX_ID.get();

        for (final Long currTargetTxId : dtxMgrTxTable.rowKeySet()) {

            final long targetTxId = currTargetTxId.longValue();
            if (targetTxId <= lastTxId) {
                throw new IllegalArgumentException("Not increasing transaction IDs; lastTxId=" + lastTxId
                        + ", targetTxId=" + targetTxId);
            }

            final Set<TxNode> participants = newRandomParticipantsSet();

            final SortedMap<UUID, DtxManager> currRow = dtxMgrTxTable.row(currTargetTxId);

            // / insert here
            for (final UUID currResMgrId : currRow.keySet()) {

                final DtxResourceManager currResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(currResMgrId);

                final DtxManager currDtxMgr = currRow.get(currResMgrId);
                final Path currTmpDir = journalDirMap.get(currDtxMgr);

                final String journalFilename = newJournalFilePrefix(currDtxMgr.getNodeId(), currResMgrId);

                final WritableTxJournal targetJournal = new WritableTxJournal(currTmpDir.toFile(), journalFilename, 0,
                        setupRotMgr);

                targetJournal.start();
                assertEquals(DEFAULT_LAST_TX_VALUE, targetJournal.getLastFinishedTxId());

                final WritableTxJournal prevJournal;
                final DtxManager prevDtxMgr = previousLog.get(currResMgrId);
                if (prevDtxMgr != null) {
                    prevJournal = new WritableTxJournal(journalDirMap.get(prevDtxMgr).toFile(), newJournalFilePrefix(
                            prevDtxMgr.getNodeId(), currResMgrId), 0, setupRotMgr);
                }
                else {
                    prevJournal = null;
                }

                // re-reads the previous journal and copies it to the new target
                if (prevJournal != null) {
                    prevJournal.start();
                    for (final JournalRecord currRecord : prevJournal.newReadOnlyTxJournal()) {
                        final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
                        switch (currEntry.getOp()) {
                        case START:
                            targetJournal.writeStart(currEntry.getTx(), currEntry.getTxNodesList());
                            break;
                        case COMMIT:
                            targetJournal.writeCommit(currEntry.getTxId(), currEntry.getTxNodesList());
                            break;
                        case ROLLBACK:
                            targetJournal.writeRollback(currEntry.getTxId(), currEntry.getErrCode(),
                                    currEntry.getTxNodesList());
                            break;
                        default:
                            // nothing
                        }
                    }
                    prevJournal.stop();
                }

                final int nbToWrite = Long.valueOf(
                        lastTxId == DEFAULT_LAST_TX_VALUE ? targetTxId - 1 : targetTxId - lastTxId).intValue();
                lastTxId = DtxTestHelper
                        .writeCompleteTransactions(targetJournal, nbToWrite, currResMgrId, participants);
                assertEquals(targetTxId, lastTxId);
                assertEquals(lastTxId, targetJournal.getLastFinishedTxId());
                targetJournal.stop();

                previousLog.put(currResMgrId, currDtxMgr);

                rankMap.put(currResMgr, currTargetTxId);
                result.put(currResMgr, currTargetTxId, currTmpDir);
            }

        }
        return result;
    }

    /**
     * Performs multiple checks to ensure all {@link TransactionManager}s recorded the same transactions.
     * 
     * @param resUuid
     *            the resource manager {@link UUID} to check
     * @param txManagers
     *            a {@link List} of {@link TransactionManager}s
     * @param nbOfTx
     *            the maximum number of transactions that can be checked
     */
    public static final void checkJournalSync(final UUID resUuid, final List<TransactionManager> txManagers,
            final int nbOfTx) {
        final int nbOfTxMgrs = txManagers.size();
        final HashMap<DtxNode, Long> lateTxMgrs = new HashMap<DtxNode, Long>(nbOfTxMgrs);

        long lastTxId = DtxConstants.DEFAULT_LAST_TX_VALUE;
        for (final TransactionManager currTxMgr : txManagers) {
            lastTxId = Math.max(lastTxId, currTxMgr.getLastCompleteTxIdForResMgr(resUuid));
        }

        for (final TransactionManager currTxMgr : txManagers) {
            final long lastCompleteTx = currTxMgr.getLastCompleteTxIdForResMgr(resUuid);
            if (lastTxId > lastCompleteTx) {
                lateTxMgrs.put(currTxMgr.getLocalNode(), Long.valueOf(lastCompleteTx));
            }
        }
        if (!lateTxMgrs.isEmpty()) {
            throw new AssertionError("Some transaction managers are late; expected last tx=" + lastTxId
                    + ", late list=" + lateTxMgrs);
        }

        // references set for commits and rollbacks on any of the transaction managers
        final BitSet refCommitSet = new BitSet(nbOfTx);
        final BitSet refRollbackSet = new BitSet(nbOfTx);

        final HashMap<TransactionManager, BitSet> commitMap = new HashMap<TransactionManager, BitSet>();
        final HashMap<TransactionManager, BitSet> rollbackMap = new HashMap<TransactionManager, BitSet>();

        for (final TransactionManager currTxMgr : txManagers) {
            final BitSet commitSet = new BitSet(nbOfTx);
            final BitSet rollbackSet = new BitSet(nbOfTx);
            commitMap.put(currTxMgr, commitSet);
            rollbackMap.put(currTxMgr, rollbackSet);
            for (final TxJournalEntry currTxEntry : currTxMgr.extractTransactions(resUuid,
                    DtxConstants.DEFAULT_LAST_TX_VALUE, lastTxId)) {
                final long currTxId = currTxEntry.getTxId();
                final int currIndex = Long.valueOf(currTxId % nbOfTx).intValue();
                switch (currTxEntry.getOp()) {
                case START:
                    break;
                case COMMIT:
                    commitSet.set(currIndex);
                    refCommitSet.set(currIndex);
                    break;
                case ROLLBACK:
                    rollbackSet.set(currIndex);
                    refRollbackSet.set(currIndex);
                    break;
                default:
                    // nothing
                }
            }
        }

        // check single commits and rollbacks
        for (final TransactionManager currTxMgr : txManagers) {
            final BitSet commitSet = commitMap.get(currTxMgr);
            commitSet.xor(refCommitSet);
            if (!commitSet.isEmpty()) {
                throw new AssertionError("Incoherence in commit log; offending transaction offsets="
                        + extractSetBitsMsgFromTxSet(commitSet));
            }

            final BitSet rollbackSet = rollbackMap.get(currTxMgr);
            rollbackSet.xor(refRollbackSet);
            if (!rollbackSet.isEmpty()) {
                throw new AssertionError("Incoherence in rollback log; offending transaction offsets="
                        + extractSetBitsMsgFromTxSet(rollbackSet));
            }

            assertTrue(rollbackSet.isEmpty());
        }
    }

    private static final String extractSetBitsMsgFromTxSet(final BitSet txSet) {
        if (txSet.isEmpty()) {
            return null;
        }

        final StringBuffer result = new StringBuffer();
        final int lastIndex = txSet.size() - 1;
        int setIndex = txSet.nextSetBit(0);
        do {
            result.append(setIndex);
            setIndex = txSet.nextSetBit(setIndex + 1);
            if (setIndex > 0) {
                result.append(",");
            }
        } while ((setIndex > 0) && (setIndex < lastIndex));

        return result.toString();
    }
}
