package io.eguan.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import static io.eguan.dtx.DtxDummyRmFactory.DEFAULT_PAYLOAD;
import static io.eguan.dtx.DtxDummyRmFactory.DEFAULT_RES_UUID;
import static io.eguan.dtx.DtxMockUtils.verifyRollbackOnTx;
import static io.eguan.dtx.DtxMockUtils.verifySuccessfulTxExecution;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static io.eguan.dtx.DtxTestHelper.newDtxManagerConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.TransactionManager;
import io.eguan.dtx.DtxDummyRmFactory.DtxResourceManagerBuilder;
import io.eguan.dtx.DtxDummyRmFactory.NodeShutdownPrepareAnswer;
import io.eguan.dtx.DtxDummyRmFactory.NodeShutdownStartAnswer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.InitializationError;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * {@link Parameterized} test for scenarios where a part of the cluster shuts down during transaction execution.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@RunWith(Parameterized.class)
public final class TestHazelcastFailuresL {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHazelcastFailuresL.class);
    private static final int VERIFY_TIMEOUT_MS = 10000;

    private static final int MIN_NB_OF_NODES = 2;
    // maximum number of cluster nodes to test - increment if you've got time to spare
    private static final int MAX_NB_OF_NODES = 4;

    private final int nbOfNodes;
    private final int nbOfShutdowns;

    private final ArrayList<DtxManager> dtxManagers = new ArrayList<DtxManager>();
    private final ArrayList<Path> tmpJournalDirs = new ArrayList<Path>();

    private final ArrayList<DtxResourceManager> failResMgrList = new ArrayList<DtxResourceManager>();
    private final ArrayList<DtxResourceManager> successResMgrList = new ArrayList<DtxResourceManager>();

    /**
     * Constructs a test instance.
     * 
     * @param nbOfNodes
     *            the total number of nodes to add to the test cluster
     * @param nbOfShutdowns
     *            the number of nodes to be shut down during transaction execution
     */
    public TestHazelcastFailuresL(final Integer nbOfNodes, final Integer nbOfShutdowns) {
        this.nbOfNodes = nbOfNodes.intValue();
        this.nbOfShutdowns = nbOfShutdowns.intValue();
        if (this.nbOfShutdowns >= this.nbOfNodes) {
            throw new IllegalArgumentException("Number of shutdowns greater or equal than node count; nbOfNodes="
                    + nbOfNodes + ", nbOfShutdowns=" + nbOfShutdowns);
        }
        if (this.nbOfShutdowns < 1) {
            throw new IllegalArgumentException("Number of shutdowns must be at least 1; nbOfNodes=" + nbOfNodes
                    + ", nbOfShutdowns=" + nbOfShutdowns);
        }
    }

    /**
     * Produces the parameter arrays with numbers of nodes and numbers of nodes to shut down.
     * 
     * Note: Shutdown numbers always stop short of shutting down the whole cluster (taking care not to isolate the
     * initiator).
     * 
     * @return the list of parameter arrays
     */
    @Parameters
    public static final List<Object[]> data() {
        final ArrayList<Object[]> result = new ArrayList<Object[]>();
        for (int nbNodes = MIN_NB_OF_NODES; nbNodes <= MAX_NB_OF_NODES; nbNodes++) {
            for (int nbShutdowns = 1; nbShutdowns < nbNodes; nbShutdowns++) {
                result.add(new Object[] { Integer.valueOf(nbNodes), Integer.valueOf(nbShutdowns) });
            }
        }
        return result;
    }

    /**
     * Sets up the test cluster.
     * 
     * @throws InitializationError
     *             if temporary directory creation fails
     */
    @Before
    public final void setUp() throws InitializationError {

        final Set<DtxNode> peerList = DtxTestHelper.newRandomCluster(nbOfNodes);

        // creates configurations and constructs DtxManager instances
        for (final DtxNode currPeer : peerList) {
            final Path journalDir;
            try {
                journalDir = Files.createTempDirectory(TestHazelcastFailuresL.class.getSimpleName());
                tmpJournalDirs.add(journalDir);
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
            final ArrayList<DtxNode> otherPeers = new ArrayList<DtxNode>(peerList);
            otherPeers.remove(currPeer);
            final DtxManagerConfig dtxConfig = newDtxManagerConfig(currPeer, journalDir,
                    otherPeers.toArray(new DtxNode[otherPeers.size()]));
            dtxManagers.add(new DtxManager(dtxConfig));
        }

        // initializes and starts the cluster nodes
        int peerCount = 0;
        for (final DtxManager currMgr : dtxManagers) {
            currMgr.init();
            currMgr.start();
            peerCount++;
            final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(currMgr.getNodeId().toString());
            assertNotNull(hzInstance);
            assertEquals(peerCount, hzInstance.getCluster().getMembers().size());
        }

    }

    /**
     * Tears down the test cluster.
     * 
     * @throws InitializationError
     *             if file cleanup fails
     */
    @After
    public final void tearDown() throws InitializationError {
        final ArrayList<Throwable> exceptionList = new ArrayList<Throwable>();
        for (final DtxManager currMgr : dtxManagers) {
            assertEquals(0, currMgr.getNbOfPendingRequests());
            currMgr.stop();
            currMgr.fini();
        }

        for (final Path currDir : tmpJournalDirs) {
            try {
                io.eguan.utils.Files.deleteRecursive(currDir);
            }
            catch (final IOException e) {
                exceptionList.add(e);
            }
        }
        if (!exceptionList.isEmpty()) {
            throw new InitializationError(exceptionList);
        }
    }

    /**
     * Tests one transaction rolled back or partially committed due to a variable number of peers being shut down during
     * the start phase.
     * 
     * @throws XAException
     *             if setting up mock fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxWithPeersShutdownOnStart() throws XAException {
        LOGGER.info("Executing; nbOfNodes=" + this.nbOfNodes + ", nbOfShutdowns=" + this.nbOfShutdowns);
        assertFalse(dtxManagers.isEmpty());

        // adds resource managers with a failing proportion
        int failCount = 0;
        final CyclicBarrier syncBarrier = new CyclicBarrier(nbOfShutdowns);
        for (final DtxManager currMgr : dtxManagers) {
            final DtxResourceManager dtxResMgr;
            if (failCount < this.nbOfShutdowns) {
                final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(currMgr.getNodeId()
                        .toString());
                assertNotNull(hzInstance);

                dtxResMgr = new DtxResourceManagerBuilder().setId(DEFAULT_RES_UUID)
                        .setStart(null, new NodeShutdownStartAnswer(hzInstance.getLifecycleService(), syncBarrier))
                        .build();
                failResMgrList.add(dtxResMgr);
                failCount++;
            }
            else {
                dtxResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(DEFAULT_RES_UUID);
                successResMgrList.add(dtxResMgr);
            }

            currMgr.registerResourceManager(dtxResMgr);
            final TransactionManager currTxMgr = currMgr.getTxManager();
            currTxMgr.setResManagerSyncState(DEFAULT_RES_UUID, UP_TO_DATE);
        }

        // submits to the last node, i.e. the one that will not be shut down
        final DtxManager submitDtxMgr = dtxManagers.get(dtxManagers.size() - 1);

        submitDtxMgr.submit(DEFAULT_RES_UUID, DEFAULT_PAYLOAD);

        for (final DtxResourceManager failingResMgr : failResMgrList) {
            verify(failingResMgr, timeout(VERIFY_TIMEOUT_MS)).start(any(byte[].class));
            final InOrder inOrderFail = inOrder(failingResMgr);
            inOrderFail.verify(failingResMgr).start(any(byte[].class));
            inOrderFail.verifyNoMoreInteractions();
        }

        final boolean quorumOnline = submitDtxMgr.countsAsQuorum(nbOfNodes - failCount);

        for (final DtxResourceManager succeedingResMgr : successResMgrList) {
            if (!quorumOnline) {
                LOGGER.debug("verifying rollback on res. manager nb " + successResMgrList.indexOf(succeedingResMgr));
                verifyRollbackOnTx(succeedingResMgr, 1, false);
            }
            else {
                LOGGER.debug("verifying commit on res. manager nb " + successResMgrList.indexOf(succeedingResMgr));
                verifySuccessfulTxExecution(succeedingResMgr, 1);
            }
        }
    }

    /**
     * Tests one transaction rolled back or partially committed due to a variable number of peers being shut down during
     * the prepare phase.
     * 
     * @throws XAException
     *             if setting up mock fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxWithPeersShutdownOnPrepare() throws XAException {
        LOGGER.info("Executing; nbOfNodes=" + this.nbOfNodes + ", nbOfShutdowns=" + this.nbOfShutdowns);
        assertFalse(dtxManagers.isEmpty());

        // adds resource managers with a failing proportion
        int failCount = 0;
        final CyclicBarrier syncBarrier = new CyclicBarrier(nbOfShutdowns);
        for (final DtxManager currMgr : dtxManagers) {
            final DtxResourceManager dtxResMgr;
            if (failCount < this.nbOfShutdowns) {
                final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(currMgr.getNodeId()
                        .toString());
                assertNotNull(hzInstance);

                dtxResMgr = new DtxResourceManagerBuilder().setId(DEFAULT_RES_UUID)
                        .setPrepare(null, new NodeShutdownPrepareAnswer(hzInstance.getLifecycleService(), syncBarrier))
                        .build();
                failResMgrList.add(dtxResMgr);
                failCount++;
            }
            else {
                dtxResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(DEFAULT_RES_UUID);
                successResMgrList.add(dtxResMgr);
            }

            currMgr.registerResourceManager(dtxResMgr);
            final TransactionManager currTxMgr = currMgr.getTxManager();
            currTxMgr.setResManagerSyncState(DEFAULT_RES_UUID, UP_TO_DATE);
        }

        // submits to the last node, i.e. the one that will not be shut down
        final DtxManager submitDtxMgr = dtxManagers.get(dtxManagers.size() - 1);

        submitDtxMgr.submit(DEFAULT_RES_UUID, DEFAULT_PAYLOAD);

        for (final DtxResourceManager failingResMgr : failResMgrList) {
            verify(failingResMgr, timeout(VERIFY_TIMEOUT_MS)).prepare(any(DtxResourceManagerContext.class));
            final InOrder inOrderFail = inOrder(failingResMgr);
            inOrderFail.verify(failingResMgr).start(any(byte[].class));
            inOrderFail.verify(failingResMgr).prepare(any(DtxResourceManagerContext.class));
            inOrderFail.verifyNoMoreInteractions();
        }

        final boolean quorumOnline = submitDtxMgr.countsAsQuorum(nbOfNodes - failCount);

        for (final DtxResourceManager succeedingResMgr : successResMgrList) {
            if (!quorumOnline) {
                LOGGER.debug("verifying rollback on res. manager nb " + successResMgrList.indexOf(succeedingResMgr));
                verifyRollbackOnTx(succeedingResMgr, 1, true);
            }
            else {
                LOGGER.debug("verifying commit on res. manager nb " + successResMgrList.indexOf(succeedingResMgr));
                verifySuccessfulTxExecution(succeedingResMgr, 1);
            }
        }
    }
}
