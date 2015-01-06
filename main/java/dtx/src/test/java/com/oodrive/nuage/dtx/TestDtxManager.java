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
import static com.oodrive.nuage.dtx.DtxDummyRmFactory.newResMgrThatDoesEverythingRight;
import static com.oodrive.nuage.dtx.DtxNodeState.INITIALIZED;
import static com.oodrive.nuage.dtx.DtxNodeState.NOT_INITIALIZED;
import static com.oodrive.nuage.dtx.DtxNodeState.STARTED;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;
import static com.oodrive.nuage.dtx.DtxTaskStatus.COMMITTED;
import static com.oodrive.nuage.dtx.DtxTaskStatus.ROLLED_BACK;
import static com.oodrive.nuage.dtx.DtxTaskStatus.UNKNOWN;
import static com.oodrive.nuage.dtx.DtxTestHelper.DEFAULT_HZ_PORT;
import static com.oodrive.nuage.dtx.DtxTestHelper.awaitStateUpdate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.oodrive.nuage.dtx.DtxDummyRmFactory.DefaultPrepareAnswer;
import com.oodrive.nuage.dtx.DtxDummyRmFactory.DtxResourceManagerBuilder;
import com.oodrive.nuage.dtx.DtxEventListeners.ErrorGeneratingEventListener;
import com.oodrive.nuage.dtx.DtxEventListeners.HazelcastNodeCountListener;
import com.oodrive.nuage.dtx.DtxEventListeners.ResMgrStateCountListener;
import com.oodrive.nuage.dtx.DtxResourceManagerAdm.DtxJournalStatus;
import com.oodrive.nuage.dtx.events.DtxClusterEvent;
import com.oodrive.nuage.dtx.events.DtxEvent;
import com.oodrive.nuage.dtx.journal.JournalRotationManager;
import com.oodrive.nuage.dtx.journal.WritableTxJournal;

/**
 * Tests for the complete lifecycle of the {@link DtxManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class TestDtxManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDtxManager.class);

    private static final int NB_OF_PEERS = 10;

    private static final int NB_OF_RES_MGR = 20;

    private static final int DEFAULT_PEER_PORT = 22222;

    private static final String DEFAULT_PEER_ADDR = "127.0.0.1";

    private static final long DEFAULT_TX_TIMEOUT_MS = 10000L;

    private static final int TASK_WAIT_RETRY_COUNT = 10;

    private static final int TASK_WAIT_TIME_MS = 1000;

    private static final int TEST_NB_OF_TEST_ENTRIES = 30;

    private static final long TEST_ROTATION_THRESHOLD = 3221225472L; // keep this high to avoid rotation

    private static final int TASK_MIN_DURATION_MS = 500;

    private DtxManager targetDtxMgr;

    private Path tmpJournalDir;

    private DtxManagerConfig dtxConfig;

    /**
     * Set up instance fixture.
     * 
     * This initializes the {@link #targetDtxMgr} field with a functional {@link DtxManager} instance.
     * 
     * @throws InitializationError
     *             if setting up fails
     * 
     * @see #targetDtxMgr
     */
    @Before
    public final void setUp() throws InitializationError {
        try {
            this.tmpJournalDir = Files.createTempDirectory(TestDtxManager.class.getSimpleName());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
        dtxConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        targetDtxMgr = new DtxManager(dtxConfig);
        assertEquals(dtxConfig.getLocalPeer().getNodeId(), targetDtxMgr.getNodeId());
    }

    /**
     * Tears down instance fixture.
     * 
     * This shuts down the {@link #targetDtxMgr} {@link DtxManager} instance properly.
     * 
     * @throws InitializationError
     *             if tearing down fails
     * 
     * @see #setUp()
     * @see #targetDtxMgr
     */
    @After
    public final void tearDown() throws InitializationError {
        targetDtxMgr.stop();
        assert (STARTED != targetDtxMgr.getStatus());
        targetDtxMgr.fini();

        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tests failed construction of a {@link DtxManager} due to a <code>null</code> argument.
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>, expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testConstructionFailConfigNull() throws NullPointerException {
        LOGGER.info("Executing");
        targetDtxMgr = new DtxManager(null);
    }

    /**
     * Tests the {@link DtxManager#init()} method.
     */
    @Test
    public final void testInit() {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        // tests repeated init
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());
    }

    /**
     * Tests the {@link DtxManager#init()} method's failure due to a bad configuration (group name is <code>null</code>
     * ).
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testInitFailBadConfigGroupNull() throws NullPointerException {
        LOGGER.info("Executing");

        final DtxManagerConfig defaultConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        final List<DtxNode> peerList = defaultConfig.getPeers();
        final DtxManagerConfig badConfig = new DtxManagerConfig(DtxTestHelper.getDefaultConfiguration(), tmpJournalDir,
                null, "asecret", defaultConfig.getLocalPeer(), peerList.toArray(new DtxNode[peerList.size()]));
        targetDtxMgr = new DtxManager(badConfig);

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
    }

    /**
     * Tests the {@link DtxManager#init()} method's failure due to a bad configuration (secret is <code>null</code> ).
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testInitFailBadConfigSecretNull() {
        LOGGER.info("Executing");

        final DtxManagerConfig defaultConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        final List<DtxNode> peerList = defaultConfig.getPeers();
        final DtxManagerConfig badConfig = new DtxManagerConfig(DtxTestHelper.getDefaultConfiguration(), tmpJournalDir,
                "testCluster", null, defaultConfig.getLocalPeer(), peerList.toArray(new DtxNode[peerList.size()]));
        targetDtxMgr = new DtxManager(badConfig);

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
    }

    /**
     * Tests the {@link DtxManager#init()} method's failure due to a <code>null</code>
     * {@link com.oodrive.nuage.configuration.MetaConfiguration}.
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testInitFailBadConfigMetaConfNull() {
        LOGGER.info("Executing");

        final DtxManagerConfig defaultConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        final List<DtxNode> peerList = defaultConfig.getPeers();
        final DtxManagerConfig badConfig = new DtxManagerConfig(null, tmpJournalDir, "testCluster", "asecret",
                defaultConfig.getLocalPeer(), peerList.toArray(new DtxNode[peerList.size()]));
        targetDtxMgr = new DtxManager(badConfig);

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
    }

    /**
     * Tests the {@link DtxManager#fini()} method.
     */
    @Test
    public final void testFini() {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.fini();
        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        // test idempotence of fini
        targetDtxMgr.fini();
        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
    }

    /**
     * Tests the correct purge of peers upon stopping and restarting the {@link DtxManager}.
     */
    @Test
    public final void testInitFiniResidue() {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final DtxNode newPeer = new DtxNode(UUID.randomUUID(), new InetSocketAddress(DEFAULT_PEER_ADDR,
                DEFAULT_PEER_PORT));

        targetDtxMgr.registerPeer(newPeer);
        assertTrue(targetDtxMgr.getRegisteredPeers().contains(newPeer));

        targetDtxMgr.fini();
        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        assertFalse(targetDtxMgr.getRegisteredPeers().contains(newPeer));
    }

    /**
     * Tests the {@link DtxManager#start()} method.
     */
    @Test
    public final void testStart() {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());
    }

    /**
     * Tests the {@link DtxManager#start()} method's failure due to a bad configuration (local peer address is
     * <code>null</code>.
     */
    @Test(expected = NullPointerException.class)
    public final void testStartFailBadConfigHostNull() {
        LOGGER.info("Executing");

        final DtxManagerConfig defaultConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        final List<DtxNode> peerList = defaultConfig.getPeers();
        final DtxManagerConfig badConfig = new DtxManagerConfig(DtxTestHelper.getDefaultConfiguration(), tmpJournalDir,
                "testCluster", "asecret", null, peerList.toArray(new DtxNode[peerList.size()]));
        targetDtxMgr = new DtxManager(badConfig);

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        try {
            targetDtxMgr.start();
        }
        catch (final IllegalArgumentException ie) {
            assertEquals(DtxNodeState.FAILED, targetDtxMgr.getStatus());
            throw ie;
        }
    }

    /**
     * Tests the {@link DtxManager#stop()} method.
     */
    @Test
    public final void testStop() {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.stop();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        // test idempotence of stop
        targetDtxMgr.stop();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());
    }

    /**
     * Tests the triggering of lifecycle events on {@link DtxManager#start()} and {@link DtxManager#stop()}.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, not part of this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, not part of this test
     * @throws InterruptedException
     *             if waiting for synchronization is interrupted, not part of this test
     */
    @Test
    public final void testRegisterDtxEventListenerAndStop() throws NullPointerException, IllegalStateException,
            InterruptedException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final CountDownLatch latch = new CountDownLatch(4);

        final Object shutdownListener = new HazelcastNodeCountListener(latch);

        targetDtxMgr.registerDtxEventListener(shutdownListener);

        // start doesn't trigger lifecycle events as construction of the hazelcast instance starts it
        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.stop();
        assertTrue("Latch counted down before timeout", latch.await(DEFAULT_TX_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        targetDtxMgr.unregisterDtxEventListener(shutdownListener);
    }

    /**
     * Tests the {@link DtxManager#registerDtxEventListener(Object)} method's failure due to a <code>null</code>
     * argument.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, expected for this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, not part of this test
     */
    @Test(expected = NullPointerException.class)
    public final void testRegisterDtxEventListenerFailNull() throws NullPointerException, IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.registerDtxEventListener(null);
    }

    /**
     * Tests the {@link DtxManager#registerDtxEventListener(Object)} method's failure due to a <code>null</code>.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, not part of this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testRegisterDtxEventListenerFailNotInitialized() throws NullPointerException,
            IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        final ErrorGeneratingEventListener badListener = new ErrorGeneratingEventListener(DtxEvent.class);

        try {
            targetDtxMgr.registerDtxEventListener(badListener);
        }
        catch (final IllegalStateException ie) {
            badListener.checkForAssertErrors(LOGGER);
            throw ie;
        }
    }

    /**
     * Tests the absence of triggered events on {@link DtxManager#stop()} after a listener is unregistered.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, not part of this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, not part of this test
     */
    @Test
    public final void testUnregisterDtxEventListener() throws NullPointerException, IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final ErrorGeneratingEventListener badClusterListener = new ErrorGeneratingEventListener(DtxClusterEvent.class);

        targetDtxMgr.registerDtxEventListener(badClusterListener);

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.unregisterDtxEventListener(badClusterListener);

        targetDtxMgr.stop();

        assertEquals(INITIALIZED, targetDtxMgr.getStatus());
        assertEquals(1, badClusterListener.getAssertErrors().size());
    }

    /**
     * Tests the {@link DtxManager#unregisterDtxEventListener(Object)} method's failure due to a <code>null</code>
     * listener.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, expected for this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, not part of this test
     */
    @Test(expected = NullPointerException.class)
    public final void testUnregisterDtxEventListenerFailNull() throws NullPointerException, IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.registerDtxEventListener(new ErrorGeneratingEventListener());

        targetDtxMgr.unregisterDtxEventListener(null);
    }

    /**
     * Tests the {@link DtxManager#unregisterDtxEventListener(Object)} method's failure due to an unregistered listener.
     * 
     * @throws NullPointerException
     *             if the registered listener is <code>null</code>, not part of this test
     * @throws IllegalStateException
     *             if the {@link DtxManager} is not initialized, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testUnregisterDtxEventListenerFailNotRegd() throws NullPointerException, IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.registerDtxEventListener(new ErrorGeneratingEventListener());

        targetDtxMgr.unregisterDtxEventListener(new ErrorGeneratingEventListener());
    }

    /**
     * Tests the {@link DtxManager#registerPeer(DtxNode)}, {@link DtxManager#getRegisteredPeers()} and
     * {@link DtxManager#unregisterPeer(DtxNode)} methods.
     */
    @Test
    public final void testRegisterUnregisterPeer() {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final ArrayList<DtxNode> registerPeers = new ArrayList<DtxNode>();

        final int nbConfPeers = targetDtxMgr.getRegisteredPeers().size();
        for (int i = nbConfPeers + 1; i <= nbConfPeers + NB_OF_PEERS; i++) {
            final UUID newUuid = UUID.randomUUID();
            final InetSocketAddress newAddr = new InetSocketAddress(DEFAULT_PEER_ADDR, DEFAULT_PEER_PORT + i);
            final DtxNode newPeer = new DtxNode(newUuid, newAddr);
            targetDtxMgr.registerPeer(newPeer);
            final Set<DtxNode> registeredPeers = targetDtxMgr.getRegisteredPeers();
            assertTrue(registeredPeers.contains(newPeer));
            assertEquals(i, registeredPeers.size());

            registerPeers.add(newPeer);

            // test idempotence of registerPeer
            targetDtxMgr.registerPeer(newPeer);
            assertTrue(targetDtxMgr.getRegisteredPeers().contains(newPeer));
        }

        assertEquals(nbConfPeers + NB_OF_PEERS, targetDtxMgr.getRegisteredPeers().size());

        for (final DtxNode currPeer : registerPeers) {
            targetDtxMgr.unregisterPeer(currPeer);
            assertFalse(targetDtxMgr.getRegisteredPeers().contains(currPeer));

            // test idempotence of unregisterPeer
            targetDtxMgr.unregisterPeer(currPeer);
            assertFalse(targetDtxMgr.getRegisteredPeers().contains(currPeer));
        }
    }

    /**
     * Tests the {@link DtxManager#registerPeer(DtxNode)} method's failure due to a non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws NullPointerException
     *             if the argument is <code>null</code>, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testRegisterPeerFailNotInitialized() throws IllegalStateException, NullPointerException {
        LOGGER.info("Executing");

        final DtxNode newPeer = new DtxNode(UUID.randomUUID(), new InetSocketAddress(DEFAULT_PEER_ADDR,
                DEFAULT_PEER_PORT));

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.registerPeer(newPeer);
    }

    /**
     * Tests the {@link DtxManager#registerPeer(DtxNode)} method's failure due to <code>null</code> argument.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>, expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testRegisterPeerFailPeerNull() throws IllegalStateException, NullPointerException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.registerPeer(null);
    }

    /**
     * Tests the {@link DtxManager#unregisterPeer(DtxNode)} method's failure due to a non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws NullPointerException
     *             if the argument is <code>null</code>, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testUnregisterPeerFailNotInitialized() throws IllegalStateException, NullPointerException {
        LOGGER.info("Executing");

        final DtxNode newPeer = new DtxNode(UUID.randomUUID(), new InetSocketAddress(DEFAULT_PEER_ADDR,
                DEFAULT_PEER_PORT));

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.unregisterPeer(newPeer);
    }

    /**
     * Tests the {@link DtxManager#unregisterPeer(DtxNode)} method's failure due to <code>null</code> argument.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * 
     * @throws NullPointerException
     *             if the argument is <code>null</code>, expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testUnregisterPeerFailPeerNull() throws IllegalStateException, NullPointerException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.unregisterPeer(null);
    }

    /**
     * Tests the {@link DtxManager#getRegisteredPeers()} method's failure due to a non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testGetRegisteredPeersFailNotInitialized() throws IllegalStateException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.getRegisteredPeers();
    }

    /**
     * Tests the {@link DtxManager#registerResourceManager(DtxResourceManager)},
     * {@link DtxManager#getRegisteredResourceManager(UUID)} and {@link DtxManager#unregisterResourceManager(UUID)}
     * methods.
     * 
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test
    public final void testRegisterUnregisterResourceManager() throws XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final ArrayList<DtxResourceManager> registerResMgrs = new ArrayList<>();

        for (int i = 0; i < NB_OF_RES_MGR; i++) {
            final UUID resourceId = UUID.randomUUID();
            final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
            assertNotNull(newResMgr);

            targetDtxMgr.registerResourceManager(newResMgr);
            assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(resourceId));
            registerResMgrs.add(newResMgr);
        }

        for (final DtxResourceManager currResMgr : registerResMgrs) {
            final UUID currId = currResMgr.getId();
            targetDtxMgr.unregisterResourceManager(currId);
            assertNull(targetDtxMgr.getRegisteredResourceManager(currId));
        }
    }

    /**
     * Tests the {@link DtxManager#registerResourceManager(DtxResourceManager)},
     * {@link DtxManager#getRegisteredResourceManager(UUID)} and {@link DtxManager#unregisterResourceManager(UUID)}
     * methods on a {@link DtxManager#start() started} instance.
     * 
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for events to occur
     */
    @Test
    public final void testRegisterUnregisterResourceManagerStarted() throws XAException, InterruptedException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());
        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        final CountDownLatch stateLatch = new CountDownLatch(2 * NB_OF_RES_MGR);
        final ResMgrStateCountListener stateListener = new ResMgrStateCountListener(stateLatch, null);

        targetDtxMgr.registerDtxEventListener(stateListener);

        final ArrayList<DtxResourceManager> registerResMgrs = new ArrayList<>();

        for (int i = 0; i < NB_OF_RES_MGR; i++) {
            final UUID resourceId = UUID.randomUUID();
            final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
            assertNotNull(newResMgr);

            targetDtxMgr.registerResourceManager(newResMgr);
            assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(resourceId));
            registerResMgrs.add(newResMgr);
        }

        for (final DtxResourceManager currResMgr : registerResMgrs) {
            final UUID currId = currResMgr.getId();
            targetDtxMgr.unregisterResourceManager(currId);
            assertNull(targetDtxMgr.getRegisteredResourceManager(currId));
        }

        assertTrue("Latch counted down before timeout", stateLatch.await(DEFAULT_TX_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        stateListener.checkForAssertErrors(LOGGER);
    }

    /**
     * Tests the {@link DtxManager#registerResourceManager(DtxResourceManager)} method's failure on duplicate
     * registration.
     * 
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     * @throws IllegalArgumentException
     *             expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testRegisterResourceManagerFailDuplicates() throws XAException, IllegalArgumentException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final UUID resourceId = UUID.randomUUID();
        final DtxResourceManager firstResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
        assertNotNull(firstResMgr);

        targetDtxMgr.registerResourceManager(firstResMgr);
        assertEquals(firstResMgr, targetDtxMgr.getRegisteredResourceManager(resourceId));

        // construct another instance
        final DtxResourceManager secondResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
        assertNotNull(secondResMgr);
        assertFalse(firstResMgr.equals(secondResMgr));

        targetDtxMgr.registerResourceManager(secondResMgr);

    }

    /**
     * Tests the {@link DtxManager#registerResourceManager(DtxResourceManager)} method's failure due to a
     * <code>null</code> argument.
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testRegisterResourceManagerFailNullArgument() throws NullPointerException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.registerResourceManager(null);
    }

    /**
     * Tests the {@link DtxManager#registerResourceManager(DtxResourceManager)} method's failure due to a
     * non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws NullPointerException
     *             if the argument is <code>null</code>, not part of this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testRegisterResourceManagerFailNotInitialized() throws IllegalStateException,
            NullPointerException, XAException {
        LOGGER.info("Executing");

        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
    }

    /**
     * Tests the {@link DtxManager#unregisterResourceManager(UUID)} method's failure due to a <code>null</code>
     * argument.
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testUnregisterResourceManagerFailNullArgument() throws NullPointerException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.unregisterResourceManager(null);
    }

    /**
     * Tests the {@link DtxManager#unregisterResourceManager(UUID)} method's failure due to a non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws NullPointerException
     *             if the argument is <code>null</code>, not part of this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testUnregisterResourceManagerFailNotInitialized() throws IllegalStateException,
            NullPointerException, XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        targetDtxMgr.fini();
        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.unregisterResourceManager(newResMgr.getId());
    }

    /**
     * Tests the {@link DtxManager#getRegisteredResourceManager(UUID)} method's failure due to a non-initialized state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws NullPointerException
     *             if the argument is <code>null</code>, not part of this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testGetregisteredResourceManagerFailNotInitialized() throws IllegalStateException,
            NullPointerException, XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        targetDtxMgr.fini();
        assertEquals(NOT_INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.getRegisteredResourceManager(newResMgr.getId());
    }

    /**
     * Tests registering the same {@link DtxResourceManager} concurrently from multiple threads.
     * 
     * @throws InterruptedException
     *             if execution of the registering threads is interrupted, not part of this test
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testConcurrentResourceManagerRegistrations() throws InterruptedException, XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        assertNotNull(newResMgr);

        final ExecutorService executor = Executors.newCachedThreadPool();

        final ArrayList<Callable<Void>> taskList = new ArrayList<Callable<Void>>();

        // adds tasks concurrently trying to register the same resource manager
        for (int i = 0; i < NB_OF_PEERS; i++) {
            taskList.add(new Callable<Void>() {

                @Override
                public final Void call() throws Exception {
                    targetDtxMgr.registerResourceManager(newResMgr);
                    return null;
                }
            });
        }

        // executes the tasks and counts the error cases.
        int errorCount = 0;
        for (final Future<Void> currResult : executor.invokeAll(taskList)) {
            try {
                currResult.get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                assertNotNull(cause);
                assertEquals(IllegalArgumentException.class, cause.getClass());
                errorCount++;
            }
        }
        // only one may have succeeded
        assertEquals(NB_OF_PEERS - 1, errorCount);
    }

    /**
     * Tests registering and unregistering the same {@link DtxResourceManager} concurrently from multiple threads.
     * 
     * @throws InterruptedException
     *             if execution of the registering or unregistering threads is interrupted, not part of this test
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testConcurrentResourceManagerUnRegistrations() throws InterruptedException, XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        final ExecutorService executor = Executors.newCachedThreadPool();

        final ArrayList<Callable<Void>> taskList = new ArrayList<Callable<Void>>();

        // adds tasks concurrently trying to register the same resource manager
        for (int i = 0; i < NB_OF_PEERS; i++) {
            taskList.add(new Callable<Void>() {

                @Override
                public final Void call() throws Exception {
                    targetDtxMgr.registerResourceManager(newResMgr);
                    return null;
                }
            });
        }

        // adds tasks concurrently trying to unregister the same resource manager
        for (int i = 0; i < NB_OF_PEERS; i++) {
            taskList.add(new Callable<Void>() {

                @Override
                public final Void call() throws Exception {
                    targetDtxMgr.unregisterResourceManager(resUuid);
                    return null;
                }
            });
        }

        Collections.shuffle(taskList);

        // executes the tasks and verifies no unusual exceptions are thrown
        for (final Future<Void> currResult : executor.invokeAll(taskList)) {
            try {
                currResult.get();
            }
            catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                assertNotNull(cause);
                assertEquals(IllegalArgumentException.class, cause.getClass());
            }
        }
    }

    /**
     * Tests the successful execution of the {@link DtxManager#submit(UUID, byte[])} method.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testSubmit() throws XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager newResMgr = newResMgrThatDoesEverythingRight(resUuid);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        final UUID taskId = targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);
        assertNotNull(taskId);

        assertFalse(UNKNOWN == targetDtxMgr.getTask(taskId).getStatus());
    }

    /**
     * Tests the successful execution of the {@link DtxManager#submit(UUID, byte[])} method on a non-up-to-date resource
     * manager with additional verification of task status updates.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting, not part of this test
     */
    @Test
    public final void testSubmitCheckTaskStatus() throws XAException, InterruptedException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);

        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);

        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        final UUID taskId = targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);
        assertNotNull(taskId);

        DtxTaskStatus currStatus = targetDtxMgr.getTask(taskId).getStatus();
        assertFalse(UNKNOWN == currStatus);

        int retryCount = TASK_WAIT_RETRY_COUNT;
        for (; retryCount > 0 && COMMITTED != currStatus && ROLLED_BACK != currStatus; retryCount--) {
            Thread.sleep(TASK_WAIT_TIME_MS);
            currStatus = targetDtxMgr.getTask(taskId).getStatus();
        }
        assertTrue(retryCount > 0);

        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == targetDtxMgr.getTaskTimestamp(taskId));
    }

    /**
     * Tests the {@link DtxManager#submit(UUID, byte[])} method's failure due to not being started.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testSubmitFailNotStarted() throws XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager newResMgr = newResMgrThatDoesEverythingRight(resUuid);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);

    }

    /**
     * Tests the {@link DtxManager#submit(UUID, byte[])} method's failure due to a missing quorum of online peers.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testSubmitFailNoQuorum() throws XAException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager newResMgr = newResMgrThatDoesEverythingRight(resUuid);
        assertNotNull(newResMgr);

        targetDtxMgr.registerResourceManager(newResMgr);
        assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(newResMgr.getId()));

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.registerPeer(new DtxNode(UUID.randomUUID(),
                new InetSocketAddress("127.0.0.1", DEFAULT_HZ_PORT + 1)));

        targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);

    }

    /**
     * Tests the {@link DtxManager#submit(UUID, byte[])} method's failure due to a missing quorum of online peers.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting, not part of this test
     * @throws TimeoutException
     *             if waiting for the resource manager to be up-to-date times out, not part of this test
     */
    @Test
    public final void testFailStopFromWithinTransaction() throws XAException, InterruptedException, TimeoutException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        assertEquals(INITIALIZED, targetDtxMgr.getStatus());

        final UUID resUuid = UUID.randomUUID();

        final DtxResourceManager naughtyResMgr = new DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {

                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        targetDtxMgr.stop();
                        return DefaultPrepareAnswer.doAnswer(invocation);
                    }
                }).build();

        targetDtxMgr.registerResourceManager(naughtyResMgr);
        assertEquals(naughtyResMgr, targetDtxMgr.getRegisteredResourceManager(naughtyResMgr.getId()));

        targetDtxMgr.start();
        assertEquals(STARTED, targetDtxMgr.getStatus());

        awaitStateUpdate(targetDtxMgr, resUuid, UP_TO_DATE);

        final UUID taskId = targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);

        DtxTaskStatus currStatus = targetDtxMgr.getTask(taskId).getStatus();
        assertFalse(UNKNOWN == currStatus);

        int retryCount = TASK_WAIT_RETRY_COUNT;
        for (; retryCount > 0 && ROLLED_BACK != currStatus; retryCount--) {
            Thread.sleep(TASK_WAIT_TIME_MS);
            currStatus = targetDtxMgr.getTask(taskId).getStatus();
        }
        assertTrue(retryCount > 0);
        assertEquals(STARTED, targetDtxMgr.getStatus());

        targetDtxMgr.stop();

    }

    /**
     * Tests reading tasks on startup previously written to a transaction journal.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IllegalStateException
     *             if initializing or starting fails, not part of this test
     * @throws IOException
     *             if writing the journal fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for resource manager startup, not part of this test
     * @throws TimeoutException
     *             after timing out on waiting for resource manager startup, not part of this test
     */
    @Test
    public final void testDtxManagerVerifyTasksAfterRegistration() throws XAException, IllegalStateException,
            IOException, InterruptedException, TimeoutException {
        LOGGER.info("Executing");
        final UUID resUuid = UUID.randomUUID();
        final DtxResourceManager targetResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);

        final ArrayList<UUID> writtenTasks = writeTaskToJournal(resUuid);

        targetDtxMgr.init();
        targetDtxMgr.start();

        targetDtxMgr.registerResourceManager(targetResMgr);
        awaitStateUpdate(targetDtxMgr, resUuid, UP_TO_DATE);

        // Test if task list is complete (using getTasks)
        final DtxTaskAdm[] currentTasks = targetDtxMgr.getTasks();

        assertEquals(writtenTasks.size(), currentTasks.length);

        for (final DtxTaskAdm task : currentTasks) {
            assertTrue(writtenTasks.contains(UUID.fromString(task.getTaskId())));
        }

        // Test each task status (using getTask)
        int i = 0;
        for (final UUID taskId : writtenTasks) {
            if (i % 2 == 0) {
                assertTrue(DtxTaskStatus.ROLLED_BACK == targetDtxMgr.getTask(taskId).getStatus());
                assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == targetDtxMgr.getTaskTimestamp(taskId));
            }
            else {
                assertTrue(DtxTaskStatus.COMMITTED == targetDtxMgr.getTask(taskId).getStatus());
                assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == targetDtxMgr.getTaskTimestamp(taskId));
            }
            i++;
        }
    }

    /**
     * Tests reading of the resource manager list.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws InterruptedException
     *             if interrupted while waiting for resource manager startup, not part of this test
     * @throws TimeoutException
     *             after timing out on waiting for resource manager startup, not part of this test
     */
    @Test
    public final void testDtxManagerVerifyResourceManagerList() throws XAException, InterruptedException,
            TimeoutException {
        LOGGER.info("Executing");

        targetDtxMgr.init();
        targetDtxMgr.start();

        final ArrayList<DtxResourceManager> registerResMgrs = new ArrayList<>();

        for (int i = 0; i < NB_OF_RES_MGR; i++) {
            final UUID resourceId = UUID.randomUUID();
            final DtxResourceManager newResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
            assertNotNull(newResMgr);

            targetDtxMgr.registerResourceManager(newResMgr);
            awaitStateUpdate(targetDtxMgr, resourceId, UP_TO_DATE);

            assertEquals(newResMgr, targetDtxMgr.getRegisteredResourceManager(resourceId));
            registerResMgrs.add(newResMgr);
        }

        final DtxResourceManagerAdm[] resMgrs = targetDtxMgr.getResourceManagers();
        assertEquals(NB_OF_RES_MGR, resMgrs.length);

        boolean ok = false;
        for (final DtxResourceManager currResMgr : registerResMgrs) {
            for (final DtxResourceManagerAdm resAdm : resMgrs) {
                if (resAdm.getUuid().equals(currResMgr.getId().toString())) {
                    assertEquals(UP_TO_DATE, resAdm.getStatus());
                    assertFalse(resAdm.getJournalPath().equals(""));
                    assertEquals(DtxJournalStatus.STARTED, resAdm.getJournalStatus());

                    ok = true;
                }
            }
            assertTrue(ok);
            ok = false;
        }
        for (final DtxResourceManager currResMgr : registerResMgrs) {
            final UUID currId = currResMgr.getId();
            targetDtxMgr.unregisterResourceManager(currId);
            assertNull(targetDtxMgr.getRegisteredResourceManager(currId));
        }
    }

    /**
     * Tests reading of the request queue.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws TimeoutException
     *             after timing out on waiting for resource manager startup, not part of this test
     * @throws InterruptedException
     *             if thread sleep is interrupted, not part of this test
     * 
     */
    @Test
    public final void testDtxManagerVerifyRequestQueue() throws XAException, InterruptedException, TimeoutException {
        LOGGER.info("Executing");

        final UUID resUuid = UUID.randomUUID();

        targetDtxMgr.init();
        targetDtxMgr.start();

        final DtxResourceManager targetResMgr = new DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {
                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        Thread.sleep(TASK_MIN_DURATION_MS);
                        return DefaultPrepareAnswer.doAnswer(invocation);
                    }
                }).build();
        targetDtxMgr.registerResourceManager(targetResMgr);
        awaitStateUpdate(targetDtxMgr, resUuid, UP_TO_DATE);

        final UUID firstTaskId = targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);
        final UUID secondTaskId = targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);
        targetDtxMgr.submit(resUuid, DEFAULT_PAYLOAD);

        final DtxRequestQueueAdm queue = targetDtxMgr.getRequestQueue();
        assertTrue(2 <= queue.getNbOfPendingRequests());

        assertTrue(Lists.newArrayList(firstTaskId.toString(), secondTaskId.toString()).contains(queue.getNextTaskID()));
        assertEquals(resUuid.toString(), queue.getNextResourceManagerID());
    }

    private final ArrayList<UUID> writeTaskToJournal(final UUID resourceId) throws IOException {

        final JournalRotationManager fixtureRotMgr = new JournalRotationManager(0);

        fixtureRotMgr.start();

        final File journalDir = dtxConfig.getJournalDirectory().toFile();
        if (!journalDir.exists()) {
            journalDir.mkdirs();
        }

        final String journalFilename = TransactionManager.newJournalFilePrefix(targetDtxMgr.getNodeId(), resourceId);

        final WritableTxJournal targetJournal = new WritableTxJournal(journalDir, journalFilename,
                TEST_ROTATION_THRESHOLD, fixtureRotMgr);
        targetJournal.start();

        DtxTestHelper.writeCompleteTransactions(targetJournal, TEST_NB_OF_TEST_ENTRIES, null,
                DtxTestHelper.newRandomParticipantsSet());

        final ArrayList<UUID> tasksList = DtxTestHelper.readCompleteTransactions(targetJournal);

        fixtureRotMgr.stop();
        targetJournal.stop();

        return tasksList;
    }

}
