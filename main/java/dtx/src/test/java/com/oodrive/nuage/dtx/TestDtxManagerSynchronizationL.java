package com.oodrive.nuage.dtx;

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

import static com.oodrive.nuage.dtx.DtxDummyRmFactory.DEFAULT_PAYLOAD;
import static com.oodrive.nuage.dtx.DtxNodeState.INITIALIZED;
import static com.oodrive.nuage.dtx.DtxNodeState.STARTED;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.POST_SYNC_PROCESSING;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.SYNCHRONIZING;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;
import static com.oodrive.nuage.dtx.DtxTaskStatus.COMMITTED;
import static com.oodrive.nuage.dtx.DtxTaskStatus.ROLLED_BACK;
import static com.oodrive.nuage.dtx.DtxTaskStatus.UNKNOWN;
import static com.oodrive.nuage.dtx.DtxTestHelper.checkJournalSync;
import static com.oodrive.nuage.dtx.DtxTestHelper.prepareExistingJournals;
import static javax.transaction.xa.XAException.XAER_RMERR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.oodrive.nuage.dtx.DtxDummyRmFactory.DtxResourceManagerBuilder;
import com.oodrive.nuage.dtx.DtxDummyRmFactory.ToggleAnswer;
import com.oodrive.nuage.dtx.DtxEventListeners.SeparateStateCountListener;
import com.oodrive.nuage.dtx.DtxEventListeners.StateCountListener;

/**
 * Tests covering the discovery phase between nodes upon joining the cluster.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */

public final class TestDtxManagerSynchronizationL extends AbstractCommonClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDtxManagerSynchronizationL.class);

    private static final int NB_OF_TEST_TX = 10;

    private static final int SYNC_WAIT_TIME_S = 20;

    private static final int TX_WAIT_TIME_MS = 100;

    private static final int PRE_VERIFICATION_LATENCY_MS = 500;

    private static final int POST_SYNC_DURATION_MS = 10000;

    private static final int NB_OF_SYNC_ATTEMPTS = 5;

    /**
     * Tests successful synchronization of nodes upon startup with one pre-registered {@link DtxResourceManager}s.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncOneResMgrOnInit() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.stop();
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        // registers resource managers
        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {

                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);

                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerResourceManager(currResMgr);
            }
        }

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        // starts nodes
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            if (STARTED == currDtxMgr.getStatus()) {
                continue;
            }
            LOGGER.debug("Starting; node=" + currDtxMgr.getNodeId());
            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }
        upToDateListener.checkForAssertErrors(LOGGER);

    }

    /**
     * Tests successful synchronization of nodes upon registering on a started cluster.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncOneResMgrOnRegister() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();

        long targetTxId = DtxTestHelper.nextTxId();
        final long firstTxId = targetTxId;

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        final ArrayList<TransactionManager> txMgrList = new ArrayList<TransactionManager>();

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        // initializes and starts all nodes
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            currDtxMgr.init();
            LOGGER.debug("Initialized; node=" + currDtxMgr.getNodeId());

            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));

            txMgrList.add(currDtxMgr.getTxManager());
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);
                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerResourceManager(currResMgr);
            }
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        final int nbOfTx = Long.valueOf(targetTxId - firstTxId).intValue();
        checkJournalSync(resUuid, txMgrList, nbOfTx);

        // verify a minimum of transactions actually got replayed
        for (final Long currIndex : testTable.rowKeySet()) {
            final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
            final int expectedDiff = Long.valueOf(targetTxId - currIndex.longValue()).intValue();
            verify(currDtxMgr.getRegisteredResourceManager(resUuid), atLeast(expectedDiff)).prepare(
                    any(DtxResourceManagerContext.class));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }
        upToDateListener.checkForAssertErrors(LOGGER);
    }

    /**
     * Tests successful synchronization of nodes upon registering on a started cluster.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncMultipleResMgrOnRegister() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final UUID firstResUuid = UUID.randomUUID();
        final UUID secondResUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), firstResUuid, currDtxMgr);
            upToDateTable.put(firstResUuid, currDtxMgr, new CountDownLatch(1));
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), secondResUuid, currDtxMgr);
            upToDateTable.put(secondResUuid, currDtxMgr, new CountDownLatch(1));
        }

        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);
                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerResourceManager(currResMgr);
            }
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : upToDateTable.row(firstResUuid).keySet()) {
            assertTrue(upToDateTable.get(firstResUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }
        for (final DtxManager currDtxMgr : upToDateTable.row(secondResUuid).keySet()) {
            assertTrue(upToDateTable.get(secondResUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(firstResUuid);
            currDtxMgr.unregisterResourceManager(secondResUuid);
        }

        upToDateListener.checkForAssertErrors(LOGGER);
    }

    /**
     * Tests successful synchronization of nodes upon startup in increasing last transaction ID order to trigger
     * sync->post-sync->late cycles for starts beyond the quorum.
     * 
     * @throws Exception
     *             if setup fails, not part of this test
     */
    @Test
    public final void testSyncWithPostSyncCycles() throws Exception {
        LOGGER.info("Executing");

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();

        long targetTxId = DtxTestHelper.nextTxId();
        final long firstTxId = targetTxId;

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.stop();
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();

        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        // registers resource managers
        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {

                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);

                currDtxMgr.registerDtxEventListener(upToDateListener);
                final DtxResourceManager longPostSyncResMgr = new DtxResourceManagerBuilder().setId(currResId)
                        .setPostSync(null, new Answer<Void>() {

                            @Override
                            public final Void answer(final InvocationOnMock invocation) throws Throwable {
                                Thread.sleep(POST_SYNC_DURATION_MS);
                                return null;
                            }
                        }).build();
                currDtxMgr.registerResourceManager(longPostSyncResMgr);
            }
        }

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        final ArrayList<TransactionManager> txMgrList = new ArrayList<TransactionManager>();

        // starts nodes in increasing last tx ID order to trigger post-sync->late cycles
        for (final DtxManager currDtxMgr : dtxMgrs) {
            if (STARTED == currDtxMgr.getStatus()) {
                continue;
            }
            LOGGER.debug("Starting; node=" + currDtxMgr.getNodeId());
            currDtxMgr.start();

            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));

            txMgrList.add(currDtxMgr.getTxManager());
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(2 * SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        // delay verification by a few milliseconds to let things settle down
        Thread.sleep(PRE_VERIFICATION_LATENCY_MS);

        final int nbOfTx = Long.valueOf(targetTxId - firstTxId).intValue();
        checkJournalSync(resUuid, txMgrList, nbOfTx);

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }
        upToDateListener.checkForAssertErrors(LOGGER);

    }

    /**
     * Tests successful synchronization of nodes after shutting down any one node.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncOneResMgrTempOffline() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        // initializes and starts all nodes
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            currDtxMgr.init();
            LOGGER.debug("Initialized; node=" + currDtxMgr.getNodeId());

            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);
                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerResourceManager(currResMgr);
            }
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
        }

        final ArrayList<DtxManager> dtxMgrKillList = new ArrayList<DtxManager>(dtxMgrs);
        Collections.shuffle(dtxMgrKillList);

        // stops one node out of order, executes some transactions and then lets it catch up
        for (final DtxManager currVictimDtxMgr : dtxMgrKillList) {

            final CountDownLatch catchUpLatch = new CountDownLatch(1);
            final HashMultimap<UUID, DtxManager> catchUpMap = HashMultimap.create();
            catchUpMap.put(resUuid, currVictimDtxMgr);
            final StateCountListener resyncListener = new StateCountListener(catchUpLatch, UP_TO_DATE, catchUpMap);
            currVictimDtxMgr.registerDtxEventListener(resyncListener);

            final long lastOnlineTxId = currVictimDtxMgr.getLastCompleteTxIdForResMgr(resUuid);

            currVictimDtxMgr.stop();
            assertEquals(INITIALIZED, currVictimDtxMgr.getStatus());

            final int submitIndex = (dtxMgrKillList.indexOf(currVictimDtxMgr) + 1) % dtxMgrKillList.size();
            final DtxManager submitNode = dtxMgrKillList.get(submitIndex);
            for (int i = 0; i < NB_OF_TEST_TX; i++) {
                final UUID taskId = submitNode.submit(resUuid, DEFAULT_PAYLOAD);
                while (DtxTaskStatus.COMMITTED != submitNode.getTask(taskId).getStatus()) {
                    Thread.sleep(TX_WAIT_TIME_MS);
                }
            }

            final long newLastTxId = submitNode.getLastCompleteTxIdForResMgr(resUuid);
            assertTrue(lastOnlineTxId + NB_OF_TEST_TX <= newLastTxId);

            currVictimDtxMgr.start();
            assertTrue(catchUpLatch.await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
            assertEquals(newLastTxId, currVictimDtxMgr.getLastCompleteTxIdForResMgr(resUuid));

            currVictimDtxMgr.unregisterDtxEventListener(resyncListener);
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterResourceManager(resUuid);
        }
        upToDateListener.checkForAssertErrors(LOGGER);

    }

    /**
     * Tests successful synchronization of nodes upon registering on a started cluster.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncPartialTempReplayFailure() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();

        long targetTxId = DtxTestHelper.nextTxId();
        final long firstTxId = targetTxId;

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> syncTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            if (currIndex.longValue() == targetTxId) {
                continue;
            }
            syncTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(NB_OF_SYNC_ATTEMPTS));
        }

        final Set<DtxManager> lateDtxMgrs = syncTable.columnKeySet();

        final SeparateStateCountListener syncListener = new SeparateStateCountListener(syncTable, SYNCHRONIZING);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        final ArrayList<ToggleAnswer<Void>> answerList = new ArrayList<ToggleAnswer<Void>>();

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            for (final Long currIndex : resMgrMap.row(currResMgr).keySet()) {
                final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
                currDtxMgr.registerDtxEventListener(syncListener);
                currDtxMgr.registerDtxEventListener(upToDateListener);

                // register a persistently failing resource manager to keep synchronizing
                if (lateDtxMgrs.contains(currDtxMgr)) {
                    final ToggleAnswer<Void> commitAns = new ToggleAnswer<Void>(true, XAER_RMERR,
                            new DtxDummyRmFactory.DefaultCommitAnswer());
                    final ToggleAnswer<Void> rollbackAns = new ToggleAnswer<Void>(true, XAER_RMERR,
                            new DtxDummyRmFactory.DefaultRollbackAnswer());
                    final DtxResourceManager faultyResourceManager = new DtxResourceManagerBuilder().setId(resUuid)
                            .setCommit(null, commitAns).setRollback(null, rollbackAns).build();
                    answerList.add(commitAns);
                    answerList.add(rollbackAns);
                    currDtxMgr.registerResourceManager(faultyResourceManager);
                }
                else {
                    currDtxMgr.registerResourceManager(currResMgr);
                }
            }
        }

        // waits for all late nodes to at least try synchronizing
        for (final DtxManager currDtxMgr : syncTable.row(resUuid).keySet()) {
            assertTrue(syncTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        // checks that no successful synchronization happened
        final ArrayList<TransactionManager> txMgrList = new ArrayList<TransactionManager>();
        for (final DtxManager currDtxMgr : DTX_MGR_JOURNAL_MAP.keySet()) {
            final long lastCompleteTx = currDtxMgr.getLastCompleteTxIdForResMgr(resUuid);
            assertEquals(currDtxMgr, testTable.get(Long.valueOf(lastCompleteTx), resUuid));
            txMgrList.add(currDtxMgr.getTxManager());
        }

        // toggles error-generating behavior on each resource manager
        for (final ToggleAnswer<Void> currAnswer : answerList) {
            currAnswer.toggleErrorTrigger();
        }

        // waits for all nodes to be up to date
        for (final DtxManager currDtxMgr : upToDateTable.row(resUuid).keySet()) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(2 * SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        // delay verification by a few milliseconds to let things settle down
        Thread.sleep(PRE_VERIFICATION_LATENCY_MS);

        final int nbOfTx = Long.valueOf(targetTxId - firstTxId).intValue();
        checkJournalSync(resUuid, txMgrList, nbOfTx);

        // verify a minimum of transactions actually got replayed
        for (final DtxManager lateDtxMgr : lateDtxMgrs) {
            verify(lateDtxMgr.getRegisteredResourceManager(resUuid), atLeast(NB_OF_TEST_TX)).prepare(
                    any(DtxResourceManagerContext.class));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(syncListener);
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }

        upToDateListener.checkForAssertErrors(LOGGER);
        syncListener.checkForAssertErrors(LOGGER);
    }

    /**
     * Tests successful synchronization of nodes including a post-sync callback upon registering on a started cluster.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for completion, not part of this test
     */
    @Test
    public final void testSyncWithPostSync() throws IllegalStateException, IOException, XAException,
            InterruptedException {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();

        long targetTxId = DtxTestHelper.nextTxId();
        final long firstTxId = targetTxId;

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> postProcTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            postProcTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener postProcListener = new SeparateStateCountListener(postProcTable,
                POST_SYNC_PROCESSING);

        final ArrayList<TransactionManager> txMgrList = new ArrayList<TransactionManager>();

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        // initializes and starts all nodes
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            currDtxMgr.init();
            LOGGER.debug("Initialized; node=" + currDtxMgr.getNodeId());

            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));

            txMgrList.add(currDtxMgr.getTxManager());
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);

                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerDtxEventListener(postProcListener);

                currDtxMgr.registerResourceManager(currResMgr);
            }
        }

        // waits for all nodes to be post-sync processing, then up-to-date and verifies post-sync processing was called
        for (final DtxManager currDtxMgr : upToDateTable.row(resUuid).keySet()) {
            assertTrue(postProcTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
            try {
                verify(currDtxMgr.getRegisteredResourceManager(resUuid), atLeast(1)).processPostSync();
            }
            catch (final Exception e) {
                LOGGER.warn("Post-sync processing threw exception", e);
            }
        }

        final int nbOfTx = Long.valueOf(targetTxId - firstTxId).intValue();
        checkJournalSync(resUuid, txMgrList, nbOfTx);

        // verify a minimum of transactions actually got replayed
        for (final Long currIndex : testTable.rowKeySet()) {
            final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
            final int expectedDiff = Long.valueOf(targetTxId - currIndex.longValue()).intValue();
            verify(currDtxMgr.getRegisteredResourceManager(resUuid), atLeast(expectedDiff)).prepare(
                    any(DtxResourceManagerContext.class));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterDtxEventListener(postProcListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }

        upToDateListener.checkForAssertErrors(LOGGER);
        postProcListener.checkForAssertErrors(LOGGER);
    }

    /**
     * Tests successful synchronization of nodes including a post-sync callback upon registering on a started cluster.
     * 
     * @throws Exception
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSyncWithTxDuringPostSync() throws Exception {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();

        long targetTxId = DtxTestHelper.nextTxId();
        final long firstTxId = targetTxId;

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
        }

        final Table<DtxResourceManager, Long, Path> resMgrMap = prepareExistingJournals(testTable, DTX_MGR_JOURNAL_MAP,
                SETUP_ROT_MGR);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            upToDateTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new SeparateStateCountListener(upToDateTable, UP_TO_DATE);

        final HashBasedTable<UUID, DtxManager, CountDownLatch> postProcTable = HashBasedTable.create();
        for (final Long currIndex : testTable.rowKeySet()) {
            postProcTable.put(resUuid, testTable.get(currIndex, resUuid), new CountDownLatch(1));
        }
        final SeparateStateCountListener postProcListener = new SeparateStateCountListener(postProcTable,
                POST_SYNC_PROCESSING);

        final ArrayList<TransactionManager> txMgrList = new ArrayList<TransactionManager>();

        // gets the set of DtxManagers ordered by increasing last tx ID
        final Set<DtxManager> ordDtxMgrSet = upToDateTable.row(resUuid).keySet();

        // initializes and starts all nodes
        for (final DtxManager currDtxMgr : ordDtxMgrSet) {
            currDtxMgr.init();
            LOGGER.debug("Initialized; node=" + currDtxMgr.getNodeId());

            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));

            txMgrList.add(currDtxMgr.getTxManager());
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);

                currDtxMgr.registerDtxEventListener(upToDateListener);
                currDtxMgr.registerDtxEventListener(postProcListener);

                final DtxResourceManager postSyncResMgr = new DtxDummyRmFactory.DtxResourceManagerBuilder()
                        .setId(currResId).setPostSync(null, new Answer<Void>() {

                            @Override
                            public Void answer(final InvocationOnMock invocation) throws Throwable {
                                final UUID taskId = currDtxMgr.submit(currResId, DtxDummyRmFactory.DEFAULT_PAYLOAD);

                                final DtxTaskStatus targetStatus;
                                if (currDtxMgr.isUpToDateOnClusterMapQuorum(currResId)) {
                                    targetStatus = COMMITTED;
                                }
                                else {
                                    targetStatus = ROLLED_BACK;
                                }
                                DtxTaskStatus currStatus;
                                final ArrayList<DtxTaskStatus> statusList = new ArrayList<DtxTaskStatus>();
                                statusList.add(UNKNOWN);
                                do {
                                    Thread.sleep(TX_WAIT_TIME_MS);
                                    currStatus = currDtxMgr.getTask(taskId).getStatus();
                                    if (currStatus != statusList.get(statusList.size() - 1)) {
                                        statusList.add(currStatus);
                                        LOGGER.debug("taskId=" + taskId + ", status evolution=" + statusList
                                                + ", expected final=" + targetStatus);
                                    }
                                } while (targetStatus != currStatus);
                                return null;
                            }
                        }).build();

                currDtxMgr.registerResourceManager(postSyncResMgr);
            }
        }

        // waits for all nodes to be post-sync processing
        for (final DtxManager currDtxMgr : upToDateTable.row(resUuid).keySet()) {
            assertTrue(postProcTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
        }

        // waits for all nodes to be up-to-date and verifies post-sync processing was called
        for (final DtxManager currDtxMgr : upToDateTable.row(resUuid).keySet()) {
            assertTrue(upToDateTable.get(resUuid, currDtxMgr).await(SYNC_WAIT_TIME_S, TimeUnit.SECONDS));
            try {
                verify(currDtxMgr.getRegisteredResourceManager(resUuid), atLeast(1)).processPostSync();
            }
            catch (final Exception e) {
                LOGGER.warn("Post-sync processing threw exception", e);
            }
        }

        // delay verification by a few milliseconds to let things settle down
        Thread.sleep(PRE_VERIFICATION_LATENCY_MS);

        final int nbOfTx = Long.valueOf(targetTxId - firstTxId).intValue();
        checkJournalSync(resUuid, txMgrList, nbOfTx);

        // verify a minimum of transactions actually got replayed
        for (final Long currIndex : testTable.rowKeySet()) {
            final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
            final int expectedDiff = Long.valueOf(targetTxId - currIndex.longValue()).intValue();
            verify(currDtxMgr.getRegisteredResourceManager(resUuid), atLeast(expectedDiff)).prepare(
                    any(DtxResourceManagerContext.class));
        }

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterDtxEventListener(postProcListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }

        upToDateListener.checkForAssertErrors(LOGGER);
        postProcListener.checkForAssertErrors(LOGGER);
    }

}
