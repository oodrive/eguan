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
import static io.eguan.dtx.DtxDummyRmFactory.newResMgrFailingOnPrepare;
import static io.eguan.dtx.DtxDummyRmFactory.newResMgrThatDoesEverythingRight;
import static io.eguan.dtx.DtxMockUtils.verifyRollbackOnTx;
import static io.eguan.dtx.DtxMockUtils.verifySuccessfulTxExecution;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static io.eguan.dtx.DtxTaskStatus.COMMITTED;
import static io.eguan.dtx.DtxTaskStatus.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import io.eguan.dtx.DtxConstants;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskStatus;
import io.eguan.dtx.TransactionInitiator;
import io.eguan.dtx.DtxDummyRmFactory.DtxResourceManagerBuilder;
import io.eguan.dtx.DtxEventListeners.SeparateStateCountListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.junit.runners.model.InitializationError;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Test for complete distributed transaction executions using the {@link TransactionInitiator} class.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TestTransactionInitiator {

    private static final class BadUnserializableException extends Exception {

        private static final long serialVersionUID = 9058313653883783904L;

        private final UnserializableExceptionPayload serializeThis;

        public BadUnserializableException(final UnserializableExceptionPayload noSerialiseran) {
            this.serializeThis = noSerialiseran;
            assertEquals(serializeThis, noSerialiseran);
        }
    }

    private static final class UnserializableExceptionPayload {
        private final byte[] payload;

        public UnserializableExceptionPayload(final byte[] payload) {
            this.payload = payload;
            assertTrue(Arrays.equals(this.payload, payload));
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTransactionInitiator.class);

    private static final int NUMBER_OF_NODES = 3;

    private static final int TIMEOUT = 10000; // ms

    private static final int TASK_MONITOR_TIMEOUT_MS = 30000;

    private static final int NB_TEST_TX = 10;

    private static final int LONG_START_TIME_MS = 5000;

    private static DtxManager dtxManager1;
    private static DtxManager dtxManager2;
    private static DtxManager dtxManager3;
    private static Path journalDir1;
    private static Path journalDir2;
    private static Path journalDir3;
    private static List<DtxManager> dtxManagers;

    private static ArrayList<DtxManagerConfig> dtxConfigs;

    /**
     * Creates a cluster with 3 nodes on which to execute all test transactions.
     * 
     * This also creates temporary directories for each of the node's journal files.
     * 
     * @throws InitializationError
     *             if temporary directory creation fails
     */
    @BeforeClass
    public static final void init3NodesCluster() throws InitializationError {

        try {
            journalDir1 = Files.createTempDirectory(TestTransactionInitiator.class.getSimpleName());
            journalDir2 = Files.createTempDirectory(TestTransactionInitiator.class.getSimpleName());
            journalDir3 = Files.createTempDirectory(TestTransactionInitiator.class.getSimpleName());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        // init cluster of three nodes
        final DtxNode[] cluster = DtxTestHelper.newRandomCluster(NUMBER_OF_NODES).toArray(new DtxNode[NUMBER_OF_NODES]);
        final DtxNode p1 = cluster[0];
        final DtxNode p2 = cluster[1];
        final DtxNode p3 = cluster[2];

        dtxConfigs = new ArrayList<DtxManagerConfig>();
        final DtxManagerConfig dtxConfig1 = DtxTestHelper.newDtxManagerConfig(p1, journalDir1, p2, p3);
        dtxManager1 = new DtxManager(dtxConfig1);
        dtxConfigs.add(dtxConfig1);

        final DtxManagerConfig dtxConfig2 = DtxTestHelper.newDtxManagerConfig(p2, journalDir2, p1, p3);
        dtxManager2 = new DtxManager(dtxConfig2);
        dtxConfigs.add(dtxConfig2);

        final DtxManagerConfig dtxConfig3 = DtxTestHelper.newDtxManagerConfig(p3, journalDir3, p1, p2);
        dtxManager3 = new DtxManager(dtxConfig3);
        dtxConfigs.add(dtxConfig3);

        dtxManagers = new ArrayList<DtxManager>();
        dtxManagers.add(dtxManager1);
        dtxManagers.add(dtxManager2);
        dtxManagers.add(dtxManager3);
        int peerCount = 0;

        dtxManager1.init();
        dtxManager2.init();
        dtxManager3.init();

        dtxManager1.start();
        peerCount++;
        final HazelcastInstance hzInstance1 = Hazelcast.getHazelcastInstanceByName(dtxManager1.getNodeId().toString());
        assertNotNull(hzInstance1);
        assertEquals(peerCount, hzInstance1.getCluster().getMembers().size());

        dtxManager2.start();
        peerCount++;
        final HazelcastInstance hzInstance2 = Hazelcast.getHazelcastInstanceByName(dtxManager2.getNodeId().toString());
        assertNotNull(hzInstance2);
        assertEquals(peerCount, hzInstance1.getCluster().getMembers().size());
        assertEquals(peerCount, hzInstance2.getCluster().getMembers().size());

        dtxManager3.start();
        peerCount++;
        final HazelcastInstance hzInstance3 = Hazelcast.getHazelcastInstanceByName(dtxManager3.getNodeId().toString());
        assertNotNull(hzInstance3);
        assertEquals(peerCount, hzInstance1.getCluster().getMembers().size());
        assertEquals(peerCount, hzInstance2.getCluster().getMembers().size());
        assertEquals(peerCount, hzInstance3.getCluster().getMembers().size());

    }

    /**
     * Shuts down the test cluster and cleans up temporary files.
     * 
     * @throws InitializationError
     *             if removing temporary data files fails
     */
    @AfterClass
    public static final void fini3NodesCluster() throws InitializationError {

        Assert.assertNotNull(dtxManager1);
        dtxManager1.stop();
        dtxManager1.fini();
        dtxManagers.remove(dtxManager1);

        Assert.assertNotNull(dtxManager2);
        dtxManager2.stop();
        dtxManager2.fini();
        dtxManagers.remove(dtxManager2);

        Assert.assertNotNull(dtxManager3);
        dtxManager3.stop();
        dtxManager3.fini();
        dtxManagers.remove(dtxManager3);

        try {
            io.eguan.utils.Files.deleteRecursive(journalDir1);
            io.eguan.utils.Files.deleteRecursive(journalDir2);
            io.eguan.utils.Files.deleteRecursive(journalDir3);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    private UUID resUuid;

    /**
     * Sets up common fixture for each test.
     */
    @Before
    public final void setUp() {
        // checks if Hazelcast peers are running and restarts them if not
        final ArrayList<DtxManager> restartPeers = new ArrayList<DtxManager>();
        for (final DtxManager currDtxMgr : dtxManagers) {
            final String hzNodeId = currDtxMgr.getNodeId().toString();
            final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(hzNodeId);
            if (hzInstance == null) {
                restartPeers.add(currDtxMgr);
            }
        }

        int peerCount = dtxManagers.size() - restartPeers.size();
        if (restartPeers.size() == dtxManagers.size()) {
            // shutdown the whole cluster
            for (final DtxManager restartPeer : restartPeers) {
                restartPeer.fini();
            }
            // reinit and start the whole cluster
            for (final DtxManager restartPeer : restartPeers) {
                restartPeer.init();
                for (final DtxManagerConfig currConfig : dtxConfigs) {
                    restartPeer.registerPeer(currConfig.getLocalPeer());
                }
                restartPeer.start();
                peerCount++;
                final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(restartPeer.getNodeId()
                        .toString());
                assertNotNull(hzInstance);
                assertEquals(peerCount, hzInstance.getCluster().getMembers().size());
            }
        }
        else {
            for (final DtxManager restartPeer : restartPeers) {
                restartPeer.stop();
                restartPeer.start();
                peerCount++;
                final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(restartPeer.getNodeId()
                        .toString());
                assertNotNull(hzInstance);
                assertEquals(peerCount, hzInstance.getCluster().getMembers().size());
            }
        }
        assertEquals(dtxManagers.size(), peerCount);

        // assign a new resource manager ID
        resUuid = UUID.randomUUID();
    }

    /**
     * Tears down common fixture and verifies invariants to isolate tests.
     */
    @After
    public final void tearDown() {
        for (final DtxManager currDtxMgr : dtxManagers) {
            // check there are no pending requests
            currDtxMgr.unregisterResourceManager(resUuid);
            assertEquals(0, currDtxMgr.getNbOfPendingRequests());
        }
    }

    /**
     * Tests successful execution of a distributed transaction.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxOnOneNodetWithoutErrors() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager[] resMgrs = new DtxResourceManager[dtxManagers.size()];
        for (int i = 0; i < resMgrs.length; i++) {
            resMgrs[i] = newResMgrThatDoesEverythingRight(resUuid);
        }
        registerResMgrsWithDtxMgrs(dtxManagers, resMgrs);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        for (final DtxManager currDtxMgr : dtxManagers) {
            verifySuccessfulTxExecution(currDtxMgr.getRegisteredResourceManager(resUuid), 1);
        }
    }

    /**
     * Tests successful execution of a distributed transaction on each of the cluster nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxOnEachNodeWithoutErrors() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager[] resMgrs = new DtxResourceManager[dtxManagers.size()];
        for (int i = 0; i < resMgrs.length; i++) {
            resMgrs[i] = newResMgrThatDoesEverythingRight(resUuid);
        }
        registerResMgrsWithDtxMgrs(dtxManagers, resMgrs);

        for (final DtxManager currSubmitNode : dtxManagers) {

            currSubmitNode.submit(resUuid, DEFAULT_PAYLOAD);

            for (final DtxManager currDtxMgr : dtxManagers) {
                verifySuccessfulTxExecution(currDtxMgr.getRegisteredResourceManager(resUuid),
                        dtxManagers.indexOf(currSubmitNode) + 1);
            }
        }
    }

    /**
     * Tests successful execution of several distributed transactions submitted to the same node.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitSeveralTxOnOneNodeWithoutErrors() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager[] resMgrs = new DtxResourceManager[dtxManagers.size()];

        for (int i = 0; i < resMgrs.length; i++) {
            resMgrs[i] = newResMgrThatDoesEverythingRight(resUuid);
        }
        registerResMgrsWithDtxMgrs(dtxManagers, resMgrs);

        for (int i = 1; i <= NB_TEST_TX; i++) {
            dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

            for (final DtxResourceManager currResMgr : resMgrs) {
                verifySuccessfulTxExecution(currResMgr, i);
            }
        }

    }

    /**
     * Tests successful execution of several distributed transactions submitted to all nodes while monitoring their
     * execution state.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws ExecutionException
     *             if a monitoring thread fails, not part of this test
     * @throws InterruptedException
     *             if monitoring threads are interrupted, not part of this test
     */
    @Test
    public final void testSubmitSeveralTxOnAllNodesWithStateMonitoring() throws XAException, InterruptedException,
            ExecutionException {
        LOGGER.info("Executing");

        final int nbOfDtxMgrs = dtxManagers.size();

        final ExecutorService executor = Executors.newFixedThreadPool(NB_TEST_TX * nbOfDtxMgrs, new ThreadFactory() {

            @Override
            public final Thread newThread(final Runnable r) {
                final Thread result = new Thread(r, TestTransactionInitiator.class.getName() + " - "
                        + UUID.randomUUID().toString());
                result.setDaemon(true);
                return result;
            }
        });

        final DtxResourceManager[] targetResMgrs = new DtxResourceManager[nbOfDtxMgrs];
        for (int i = 0; i < targetResMgrs.length; i++) {
            targetResMgrs[i] = newResMgrThatDoesEverythingRight(resUuid);
        }
        registerResMgrsWithDtxMgrs(dtxManagers, targetResMgrs);

        final ArrayList<Callable<Boolean>> monitorTaskList = new ArrayList<Callable<Boolean>>();
        for (int i = 1; i <= NB_TEST_TX; i++) {
            for (final DtxManager currDtxMgr : dtxManagers) {
                final UUID taskId = currDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);

                monitorTaskList.add(new Callable<Boolean>() {

                    @Override
                    public final Boolean call() throws Exception {
                        DtxTaskStatus currStatus;
                        final long timeLimit = System.currentTimeMillis() + TASK_MONITOR_TIMEOUT_MS;
                        boolean timeout = false;
                        do {
                            currStatus = currDtxMgr.getTask(taskId).getStatus();
                            if (DtxTaskStatus.UNKNOWN == currStatus) {
                                return Boolean.FALSE;
                            }
                            timeout = (System.currentTimeMillis() > timeLimit);
                        } while (COMMITTED != currStatus && !timeout);
                        return timeout ? Boolean.FALSE : Boolean.TRUE;
                    }
                });
            }
        }

        final List<Future<Boolean>> futureList = executor.invokeAll(monitorTaskList, NB_TEST_TX * nbOfDtxMgrs
                * TASK_MONITOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        for (final Future<Boolean> currFut : futureList) {
            assertTrue("Monitoring thread terminated on committed state", currFut.get().booleanValue());
        }

        executor.shutdown();

        if (!executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS)) {
            assertTrue(executor.shutdownNow().isEmpty());
        }
    }

    /**
     * Tests successful execution of several distributed transactions submitted to different same nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitSeveralTxOnDifferentNodesWithoutErrors() throws XAException {
        LOGGER.info("Executing");

        final int nbDtxMgrs = dtxManagers.size();
        final DtxResourceManager[] resMgrs = new DtxResourceManager[dtxManagers.size()];

        for (int i = 0; i < resMgrs.length; i++) {
            resMgrs[i] = newResMgrThatDoesEverythingRight(resUuid);
        }

        registerResMgrsWithDtxMgrs(dtxManagers, resMgrs);

        final ArrayList<DtxManager> dtxMgrMix = new ArrayList<DtxManager>(dtxManagers);
        for (int i = 1; i <= NB_TEST_TX * nbDtxMgrs; i++) {

            for (final DtxManager currDtxMgr : dtxMgrMix) {
                currDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);
            }

            Collections.shuffle(dtxMgrMix);

            final int verifMultiplicity = i * nbDtxMgrs;
            for (final DtxResourceManager currResMgr : resMgrs) {
                DtxMockUtils.verifySuccessfulTxExecution(currResMgr, verifMultiplicity);
            }
        }
    }

    /**
     * Tests failure of one distributed transaction due to a failure to start on all nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testStartErrorOnAllNodes() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager dtxResourceManager1 = DtxDummyRmFactory.newResMgrFailingOnStart(resUuid,
                new XAException(1));

        final DtxResourceManager dtxResourceManager2 = DtxDummyRmFactory.newResMgrFailingOnStart(resUuid,
                new XAException(1));

        final DtxResourceManager dtxResourceManager3 = DtxDummyRmFactory.newResMgrFailingOnStart(resUuid,
                new XAException(1));

        registerResMgrsWithDtxMgrs(dtxManagers, dtxResourceManager1, dtxResourceManager2, dtxResourceManager3);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        // Check the order of method calls
        verify(dtxResourceManager1, timeout(TIMEOUT)).start(any(byte[].class));
        verify(dtxResourceManager2, timeout(TIMEOUT)).start(any(byte[].class));
        verify(dtxResourceManager3, timeout(TIMEOUT)).start(any(byte[].class));

        // verifies there are no prepares on resource managers
        verify(dtxResourceManager1, never()).prepare(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager2, never()).prepare(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager3, never()).prepare(any(DtxResourceManagerContext.class));

        // verifies there are no commits on resource managers
        verify(dtxResourceManager1, never()).commit(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager2, never()).commit(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager3, never()).commit(any(DtxResourceManagerContext.class));

        // verifies there are no rollbacks on resource managers
        verify(dtxResourceManager1, never()).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager2, never()).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager3, never()).rollback(any(DtxResourceManagerContext.class));

    }

    /**
     * Tests failure of one distributed transaction due to a failure to prepare on all nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testPrepareErrorOnAllNodes() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager dtxResourceManager1 = newResMgrFailingOnPrepare(resUuid, new XAException(1));

        final DtxResourceManager dtxResourceManager2 = newResMgrFailingOnPrepare(resUuid, new XAException(1));

        final DtxResourceManager dtxResourceManager3 = newResMgrFailingOnPrepare(resUuid, new XAException(1));

        registerResMgrsWithDtxMgrs(dtxManagers, dtxResourceManager1, dtxResourceManager2, dtxResourceManager3);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        // Check the order of method calls
        verify(dtxResourceManager1, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager2, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager3, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder1 = inOrder(dtxResourceManager1);
        inOrder1.verify(dtxResourceManager1).start(any(byte[].class));
        inOrder1.verify(dtxResourceManager1).prepare(any(DtxResourceManagerContext.class));
        inOrder1.verify(dtxResourceManager1).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder2 = inOrder(dtxResourceManager2);
        inOrder2.verify(dtxResourceManager2).start(any(byte[].class));
        inOrder2.verify(dtxResourceManager2).prepare(any(DtxResourceManagerContext.class));
        inOrder2.verify(dtxResourceManager2).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder3 = inOrder(dtxResourceManager3);
        inOrder3.verify(dtxResourceManager3).start(any(byte[].class));
        inOrder3.verify(dtxResourceManager3).prepare(any(DtxResourceManagerContext.class));
        inOrder3.verify(dtxResourceManager3).rollback(any(DtxResourceManagerContext.class));
    }

    /**
     * Tests failure of one distributed transaction due to a prepare returning {@link Boolean#FALSE} on two out of three
     * nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testPrepareReturningFalseOnTwoNodes() throws XAException {
        LOGGER.info("Executing");

        final Answer<Boolean> wrongAnswer = new Answer<Boolean>() {

            @Override
            public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                return Boolean.FALSE;
            }
        };

        final DtxResourceManager[] resMgrs = new DtxResourceManager[] {
                new DtxResourceManagerBuilder().setId(resUuid).setPrepare(null, wrongAnswer).build(),
                new DtxResourceManagerBuilder().setId(resUuid).setPrepare(null, wrongAnswer).build(),
                newResMgrThatDoesEverythingRight(resUuid) };

        registerResMgrsWithDtxMgrs(dtxManagers, resMgrs);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        // Check the order of method calls
        for (final DtxResourceManager currResMgr : resMgrs) {
            verify(currResMgr, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));
        }

        for (final DtxResourceManager currResMgr : resMgrs) {
            final InOrder inOrderVerifier = inOrder(currResMgr);
            inOrderVerifier.verify(currResMgr).start(any(byte[].class));
            inOrderVerifier.verify(currResMgr).prepare(any(DtxResourceManagerContext.class));
            inOrderVerifier.verify(currResMgr).rollback(any(DtxResourceManagerContext.class));
        }
    }

    /**
     * Tests failure of one distributed transaction due to a prepare throwing a {@link RuntimeException} on two out of
     * three nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testPrepareThrowingUnserializableExceptionOnTwoNodes() throws XAException {
        LOGGER.info("Executing");

        final Answer<Boolean> nastyAnswer = new Answer<Boolean>() {

            @Override
            public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                final BadUnserializableException badException = new BadUnserializableException(
                        new UnserializableExceptionPayload("go away".getBytes()));
                throw new IllegalStateException("This is not good", badException);
            }
        };

        final DtxResourceManager dtxResourceManager1 = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, nastyAnswer).build();

        final DtxResourceManager dtxResourceManager2 = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, nastyAnswer).build();

        final DtxResourceManager dtxResourceManager3 = newResMgrThatDoesEverythingRight(resUuid);

        registerResMgrsWithDtxMgrs(dtxManagers, dtxResourceManager1, dtxResourceManager2, dtxResourceManager3);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        // Check the order of method calls
        verify(dtxResourceManager1, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager2, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));
        verify(dtxResourceManager3, timeout(TIMEOUT)).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder1 = inOrder(dtxResourceManager1);
        inOrder1.verify(dtxResourceManager1).start(any(byte[].class));
        inOrder1.verify(dtxResourceManager1).prepare(any(DtxResourceManagerContext.class));
        inOrder1.verify(dtxResourceManager1).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder2 = inOrder(dtxResourceManager2);
        inOrder2.verify(dtxResourceManager2).start(any(byte[].class));
        inOrder2.verify(dtxResourceManager2).prepare(any(DtxResourceManagerContext.class));
        inOrder2.verify(dtxResourceManager2).rollback(any(DtxResourceManagerContext.class));

        final InOrder inOrder3 = inOrder(dtxResourceManager3);
        inOrder3.verify(dtxResourceManager3).start(any(byte[].class));
        inOrder3.verify(dtxResourceManager3).prepare(any(DtxResourceManagerContext.class));
        inOrder3.verify(dtxResourceManager3).rollback(any(DtxResourceManagerContext.class));
    }

    /**
     * Tests failure of one distributed transaction due to a failure to start on one node.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    // FIXME: may fail, according to test order (add 01 to run it earlier)
    @Test
    public final void test01SubmitOneTxWithOneNodeFailingOnStart() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager dtxResourceManager1 = newResMgrThatDoesEverythingRight(resUuid);

        final DtxResourceManager dtxResourceManager2 = newResMgrThatDoesEverythingRight(resUuid);

        // this resource manager fails on starting the transaction, so the transaction must fail
        final DtxResourceManager dtxResourceManager3 = DtxDummyRmFactory.newResMgrFailingOnStart(resUuid,
                new XAException(XAException.XAER_RMERR));

        registerResMgrsWithDtxMgrs(dtxManagers, dtxResourceManager1, dtxResourceManager2, dtxResourceManager3);

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        for (final DtxManager currDtxMgr : dtxManagers) {
            if (currDtxMgr.equals(dtxManager3)) {
                final DtxResourceManager targetResMgr = currDtxMgr.getRegisteredResourceManager(resUuid);
                // verify start, exclude all other operations as even sync must go through successful start first
                verify(targetResMgr, timeout(TIMEOUT)).start(any(byte[].class));
                verify(targetResMgr, never()).prepare(any(DtxResourceManagerContext.class));
                verify(targetResMgr, never()).commit(any(DtxResourceManagerContext.class));
                verify(targetResMgr, never()).rollback(any(DtxResourceManagerContext.class));
            }
            else {
                // the quorum's responding, expect commit
                verifySuccessfulTxExecution(currDtxMgr.getRegisteredResourceManager(resUuid), 1);
            }
        }
    }

    /**
     * Tests failure of one distributed transaction due to a failure to prepare on one node.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxWithOneNodeFailingOnPrepare() throws XAException {
        LOGGER.info("Executing");

        registerResMgrsWithDtxMgrs(dtxManagers, newResMgrThatDoesEverythingRight(resUuid),
                newResMgrThatDoesEverythingRight(resUuid),
                newResMgrFailingOnPrepare(resUuid, new XAException(XAException.XAER_RMERR)));

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        for (final DtxManager currDtxMgr : dtxManagers) {
            verifySuccessfulTxExecution(currDtxMgr.getRegisteredResourceManager(resUuid), 1);
        }
    }

    /**
     * Tests failure of one distributed transaction due to a failure to start on two nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    // FIXME: may fail, according to test order (add 02 to run it earlier)
    @Test
    public final void test02SubmitOneTxWithTwoNodesFailingOnStart() throws XAException {
        LOGGER.info("Executing");

        registerResMgrsWithDtxMgrs(dtxManagers, newResMgrThatDoesEverythingRight(resUuid),
                DtxDummyRmFactory.newResMgrFailingOnStart(resUuid, new XAException(XAException.XAER_RMERR)),
                DtxDummyRmFactory.newResMgrFailingOnStart(resUuid, new XAException(XAException.XAER_RMERR)));

        final List<DtxManager> failNodes = Arrays.asList(new DtxManager[] { dtxManager2, dtxManager3 });

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        for (final DtxManager currDtxMgr : dtxManagers) {
            if (failNodes.contains(currDtxMgr)) {
                final DtxResourceManager targetResMgr = currDtxMgr.getRegisteredResourceManager(resUuid);
                verify(targetResMgr, timeout(TIMEOUT)).start(any(byte[].class));
                verify(targetResMgr, never()).prepare(any(DtxResourceManagerContext.class));
                verify(targetResMgr, never()).commit(any(DtxResourceManagerContext.class));
                verify(targetResMgr, never()).rollback(any(DtxResourceManagerContext.class));
            }
            else {
                verifyRollbackOnTx(currDtxMgr.getRegisteredResourceManager(resUuid), 1, false);
            }
        }
    }

    /**
     * Tests failure of one distributed transaction due to a failure to prepare on two nodes.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxWithTwoNodesFailingOnPrepare() throws XAException {
        LOGGER.info("Executing");

        registerResMgrsWithDtxMgrs(dtxManagers, newResMgrThatDoesEverythingRight(resUuid),
                newResMgrFailingOnPrepare(resUuid, new XAException(XAException.XAER_RMERR)),
                newResMgrFailingOnPrepare(resUuid, new XAException(XAException.XAER_RMERR)));

        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        for (final DtxManager currDtxMgr : dtxManagers) {
            verifyRollbackOnTx(currDtxMgr.getRegisteredResourceManager(resUuid), 1, true);
        }
    }

    /**
     * Tests failure of one distributed transaction due to the initiator shutting down in the middle of it.
     * 
     * Note: This test relies on the synchronization mechanism to recover the partially executed transaction on the
     * initiator node.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws InterruptedException
     *             if waiting for the late node is interrupted, not part of this test
     * @throws TimeoutException
     *             if waiting for the late node times out, not part of this test
     * @throws BrokenBarrierException
     *             if sync'ing after restart of the failing initiator fails, not part of this test
     */
    @Test
    public final void testSubmitOneTxWithTxInitFailResync() throws XAException, InterruptedException, TimeoutException,
            BrokenBarrierException {
        LOGGER.info("Executing");

        final ExecutorService controlExecutor = Executors.newSingleThreadExecutor();

        final Answer<DtxResourceManagerContext> delayedStartAnswer = new Answer<DtxResourceManagerContext>() {

            @Override
            public final DtxResourceManagerContext answer(final InvocationOnMock invocation) throws Throwable {
                Thread.sleep(LONG_START_TIME_MS);
                return DtxDummyRmFactory.DefaultStartAnswer.doAnswer(invocation, resUuid);
            }
        };

        registerResMgrsWithDtxMgrs(dtxManagers,
                new DtxResourceManagerBuilder().setId(resUuid).setStart(null, delayedStartAnswer).build(),
                new DtxResourceManagerBuilder().setId(resUuid).setStart(null, delayedStartAnswer).build(),
                newResMgrThatDoesEverythingRight(resUuid));

        final UUID taskId = dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);

        final CyclicBarrier restartBarrier = new CyclicBarrier(2);

        controlExecutor.submit(new Callable<Void>() {

            @Override
            public final Void call() throws Exception {
                DtxTaskStatus currState = dtxManager1.getTask(taskId).getStatus();
                while (STARTED.compareTo(currState) > 0) {
                    currState = dtxManager1.getTask(taskId).getStatus();
                }
                // kill all current initiator activity
                dtxManager1.stop();
                // wait until the transaction must have timed out
                Thread.sleep(dtxConfigs.get(0).getTransactionTimeout());
                // restart the DtxManager
                dtxManager1.start();
                // break the restart barrier to continue the main test thread
                restartBarrier.await();
                return null;
            }
        });

        // wait until the control thread has restarted the DtxManager
        restartBarrier.await();

        // wait for synchronization to complete and verify that startup synchronization took care of things on the
        // initiator node
        for (final DtxManager currDtxMgr : dtxManagers) {
            DtxTestHelper.awaitStateUpdate(currDtxMgr, resUuid, UP_TO_DATE);
            verifyRollbackOnTx(currDtxMgr.getRegisteredResourceManager(resUuid), 1, false);
            assertEquals(DtxTaskStatus.ROLLED_BACK, currDtxMgr.getTask(taskId).getStatus());
        }

        // write another transaction to check if everything's back to normal
        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);
        for (final DtxManager currDtxMgr : dtxManagers) {
            verifySuccessfulTxExecution(currDtxMgr.getRegisteredResourceManager(resUuid), 1);
        }

        // shut down executor
        controlExecutor.shutdown();
        if (!controlExecutor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS)) {
            assertTrue(controlExecutor.shutdownNow().isEmpty());
        }
    }

    /**
     * Tests getting a {@link DtxTaskAdm task's} status after it has been committed back.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testGetJournaledTask() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager resMgr1 = newResMgrThatDoesEverythingRight(resUuid);

        registerResMgrsWithDtxMgrs(dtxManagers, resMgr1, newResMgrThatDoesEverythingRight(resUuid),
                newResMgrThatDoesEverythingRight(resUuid));

        final UUID taskId = dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);
        verifySuccessfulTxExecution(resMgr1, 1);

        // writes another transaction to evict the first one
        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);
        verifySuccessfulTxExecution(resMgr1, 2);

        // checks the status of the first task on every node
        assertEquals(COMMITTED, dtxManager1.getTask(taskId).getStatus());
        assertEquals(COMMITTED, dtxManager2.getTask(taskId).getStatus());
        assertEquals(COMMITTED, dtxManager3.getTask(taskId).getStatus());

        // check the timestamp of the firs task on every node
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxManager1.getTaskTimestamp(taskId));
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxManager2.getTaskTimestamp(taskId));
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxManager3.getTaskTimestamp(taskId));
    }

    /**
     * Tests getting a {@link DtxTaskAdm tasks} status after it has been committed back.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testGetJournaledTaskAfterRestart() throws XAException {
        LOGGER.info("Executing");

        final DtxResourceManager resMgr1 = newResMgrThatDoesEverythingRight(resUuid);

        registerResMgrsWithDtxMgrs(dtxManagers, resMgr1, newResMgrThatDoesEverythingRight(resUuid),
                newResMgrThatDoesEverythingRight(resUuid));

        final UUID taskId = dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);
        verifySuccessfulTxExecution(resMgr1, 1);

        // writes another transaction to evict the first one
        dtxManager1.submit(resUuid, DEFAULT_PAYLOAD);
        verifySuccessfulTxExecution(resMgr1, 2);

        // checks the status of the first task
        assertEquals(COMMITTED, dtxManager1.getTask(taskId).getStatus());
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxManager1.getTaskTimestamp(taskId));

        dtxManager1.stop();
        dtxManager1.fini();
        dtxManagers.remove(dtxManager1);

        dtxManager1 = new DtxManager(dtxConfigs.get(0));

        dtxManager1.init();
        dtxManager1.start();

        dtxManagers.add(dtxManager1);

        assertEquals(DtxTaskStatus.UNKNOWN, dtxManager1.getTask(taskId).getStatus());

        registerResMgrsWithDtxMgrs(Arrays.asList(new DtxManager[] { dtxManager1 }), resMgr1);

        assertEquals(DtxTaskStatus.COMMITTED, dtxManager1.getTask(taskId).getStatus());

        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxManager1.getTaskTimestamp(taskId));
    }

    /**
     * Registers the given {@link DtxResourceManager}s with the {@link DtxManager}s at the same position in the list and
     * waits for them to be considered UP_TO_DATE.
     * 
     * @param dtxMgrs
     *            the {@link List}of DtxManagers to register with
     * @param dtxResMgrs
     *            the {@link DtxResourceManager}s to register
     */
    private static final void registerResMgrsWithDtxMgrs(final List<DtxManager> dtxMgrs,
            final DtxResourceManager... dtxResMgrs) {

        final HashBasedTable<UUID, DtxManager, CountDownLatch> upToDateTable = HashBasedTable.create();
        for (int i = 0; i < dtxResMgrs.length; i++) {
            final DtxManager currDtxMgr = dtxMgrs.get(i);
            final DtxResourceManager currResMgr = dtxResMgrs[i];
            upToDateTable.put(currResMgr.getId(), currDtxMgr, new CountDownLatch(1));
        }
        final SeparateStateCountListener upToDateListener = new DtxEventListeners.SeparateStateCountListener(
                upToDateTable, UP_TO_DATE);

        for (int i = 0; i < dtxResMgrs.length; i++) {
            final DtxManager currDtxMgr = dtxMgrs.get(i);
            final DtxResourceManager currResMgr = dtxResMgrs[i];
            currDtxMgr.registerDtxEventListener(upToDateListener);
            currDtxMgr.registerResourceManager(currResMgr);
        }

        for (int i = 0; i < dtxResMgrs.length; i++) {
            final DtxManager currDtxMgr = dtxMgrs.get(i);
            final DtxResourceManager currResMgr = dtxResMgrs[i];
            try {
                assertTrue(upToDateTable.get(currResMgr.getId(), currDtxMgr).await(TIMEOUT, TimeUnit.MILLISECONDS));
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
        }
    }
}
