package io.eguan.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import static io.eguan.dtx.DtxResourceManagerState.LATE;
import static io.eguan.dtx.DtxResourceManagerState.POST_SYNC_PROCESSING;
import static io.eguan.dtx.DtxResourceManagerState.SYNCHRONIZING;
import static io.eguan.dtx.DtxResourceManagerState.UNDETERMINED;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerState;
import io.eguan.dtx.DtxEventListeners.StateCountListener;
import io.eguan.dtx.DtxEventListeners.StateSetCountListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Tests covering the discovery phase between nodes upon joining the cluster.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestDtxManagerDiscoveryL extends AbstractCommonClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDtxManagerDiscoveryL.class);

    private static final int NB_OF_TEST_TX = 10;

    private static final int DISCOVERY_WAIT_TIME_MS = 10000;

    private static final List<DtxResourceManagerState> INITIAL_STATES = Arrays.asList(new DtxResourceManagerState[] {
            UNDETERMINED, SYNCHRONIZING, LATE });

    private static final List<DtxResourceManagerState> LATE_STATES = Arrays.asList(new DtxResourceManagerState[] {
            LATE, SYNCHRONIZING, POST_SYNC_PROCESSING, UP_TO_DATE });

    /**
     * Tests successful discovery of nodes upon startup with pre-registered {@link DtxResourceManager}s.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for results
     */
    @Test
    public final void testDiscoveryBetweenNodesOnInit() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        final CountDownLatch allDoneLatch = new CountDownLatch(NB_OF_NODES);
        final HashMultimap<UUID, DtxManager> lateMap = HashMultimap.create();
        final HashMultimap<UUID, DtxManager> uptodateMap = HashMultimap.create();

        for (final Iterator<DtxManager> dtxIter = dtxMgrs.iterator(); dtxIter.hasNext();) {
            final DtxManager currDtxMgr = dtxIter.next();
            currDtxMgr.stop();
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
            if (dtxIter.hasNext()) {
                lateMap.put(resUuid, currDtxMgr);
            }
            else {
                uptodateMap.put(resUuid, currDtxMgr);
            }
        }

        final StateSetCountListener lateListener = new StateSetCountListener(allDoneLatch, LATE_STATES, lateMap);
        final StateCountListener upToDateListener = new StateCountListener(allDoneLatch, UP_TO_DATE, lateMap);

        final Table<DtxResourceManager, Long, Path> resMgrMap = DtxTestHelper.prepareExistingJournals(testTable,
                DTX_MGR_JOURNAL_MAP, SETUP_ROT_MGR);

        // initializes all nodes
        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID currResId = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, currResId);
                currDtxMgr.init();
                LOGGER.debug("Initialized; node=" + currDtxMgr.getNodeId());
                currDtxMgr.registerResourceManager(currResMgr);
                assertTrue(INITIAL_STATES.contains(currDtxMgr.getResourceManagerState(resUuid)));

                currDtxMgr.registerDtxEventListener(lateListener);
                currDtxMgr.registerDtxEventListener(upToDateListener);
            }
        }

        // starts all nodes
        for (final DtxManager currDtxMgr : dtxMgrs) {
            LOGGER.debug("Starting; node=" + currDtxMgr.getNodeId());
            currDtxMgr.start();
            LOGGER.debug("Started; node=" + currDtxMgr.getNodeId() + ", last txId="
                    + currDtxMgr.getLastCompleteTxIdForResMgr(resUuid));
        }

        // waits for all nodes to be up to date
        assertTrue(allDoneLatch.await(DISCOVERY_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(lateListener);
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }
    }

    /**
     * Tests successful discovery of nodes upon registering each with already started nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for results
     */
    @Test
    public final void testDiscoveryBetweenNodesOnRegister() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final UUID resUuid = UUID.randomUUID();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        final CountDownLatch allDoneLatch = new CountDownLatch(NB_OF_NODES);
        final HashMultimap<UUID, DtxManager> lateMap = HashMultimap.create();
        final HashMultimap<UUID, DtxManager> uptodateMap = HashMultimap.create();

        for (final Iterator<DtxManager> dtxIter = dtxMgrs.iterator(); dtxIter.hasNext();) {
            final DtxManager currDtxMgr = dtxIter.next();
            targetTxId += NB_OF_TEST_TX;
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
            if (dtxIter.hasNext()) {
                lateMap.put(resUuid, currDtxMgr);
            }
            else {
                uptodateMap.put(resUuid, currDtxMgr);
            }
        }

        final StateSetCountListener lateListener = new StateSetCountListener(allDoneLatch, LATE_STATES, lateMap);
        final StateCountListener upToDateListener = new StateCountListener(allDoneLatch, UP_TO_DATE, lateMap);

        final Table<DtxResourceManager, Long, Path> resMgrMap = DtxTestHelper.prepareExistingJournals(testTable,
                DTX_MGR_JOURNAL_MAP, SETUP_ROT_MGR);

        // registers listeners on all nodes
        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.registerDtxEventListener(lateListener);
            currDtxMgr.registerDtxEventListener(upToDateListener);
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
                currDtxMgr.registerResourceManager(currResMgr);
                assertTrue(INITIAL_STATES.contains(currDtxMgr.getResourceManagerState(resUuid)));
            }
        }

        // waits for all nodes to be up to date
        assertTrue(allDoneLatch.await(DISCOVERY_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(lateListener);
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
            currDtxMgr.unregisterResourceManager(resUuid);
        }
    }

    /**
     * Tests the discovery process with resource managers only registered on one node and absent on all other nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IOException
     *             if writing transactions to journals fails, not part of this test
     * @throws IllegalStateException
     *             if setting up existing journals fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for results
     */
    @Test
    public final void testDiscoverResMgrOnlyOnOneNode() throws XAException, IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final TreeBasedTable<Long, UUID, DtxManager> testTable = TreeBasedTable.create();
        long targetTxId = DtxTestHelper.nextTxId();

        final CountDownLatch allUpToDateLatch = new CountDownLatch(NB_OF_NODES);
        final HashMultimap<UUID, DtxManager> upToDateMap = HashMultimap.create();

        for (final DtxManager currDtxMgr : dtxMgrs) {
            targetTxId += NB_OF_TEST_TX;
            final UUID resUuid = UUID.randomUUID();
            testTable.put(Long.valueOf(targetTxId), resUuid, currDtxMgr);
            upToDateMap.put(resUuid, currDtxMgr);
        }

        final StateCountListener upToDateListener = new StateCountListener(allUpToDateLatch, UP_TO_DATE, upToDateMap);

        final Table<DtxResourceManager, Long, Path> resMgrMap = DtxTestHelper.prepareExistingJournals(testTable,
                DTX_MGR_JOURNAL_MAP, SETUP_ROT_MGR);

        // registers listener on all nodes
        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.registerDtxEventListener(upToDateListener);
        }

        for (final DtxResourceManager currResMgr : resMgrMap.rowKeySet()) {
            final Map<Long, Path> currRow = resMgrMap.row(currResMgr);
            for (final Long currIndex : currRow.keySet()) {
                final UUID resUuid = currResMgr.getId();
                final DtxManager currDtxMgr = testTable.get(currIndex, resUuid);
                currDtxMgr.registerResourceManager(currResMgr);
                assertTrue(INITIAL_STATES.contains(currDtxMgr.getResourceManagerState(resUuid)));
            }
        }

        // waits for all nodes to be up to date
        assertTrue(allUpToDateLatch.await(DISCOVERY_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

        for (final DtxManager currDtxMgr : dtxMgrs) {
            currDtxMgr.unregisterDtxEventListener(upToDateListener);
        }
    }

}
