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

import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static com.oodrive.nuage.dtx.DtxNodeState.INITIALIZED;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;
import static com.oodrive.nuage.dtx.DtxTestHelper.DEFAULT_TX_MESSAGE;
import static com.oodrive.nuage.dtx.DtxTestHelper.buildDefaultTransaction;
import static com.oodrive.nuage.dtx.DtxTestHelper.newDtxManagerConfig;
import static com.oodrive.nuage.dtx.DtxTestHelper.registerResMgrWithTxMgr;
import static com.oodrive.nuage.dtx.proto.TxProtobufUtils.fromUuid;
import static com.oodrive.nuage.dtx.proto.TxProtobufUtils.toUuid;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
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

import com.google.protobuf.ByteString;
import com.oodrive.nuage.dtx.DtxTaskApiAbstract.TaskLoader;
import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxMessage;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxNode;

/**
 * Test class for {@link TransactionManager}'s methods with a few special error cases.
 * 
 * Protocol and {@link DtxResourceManager}-related error cases are tested separately.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class TestTransactionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTransactionManager.class);

    private static final int FUTURE_TIMEOUT_S = 10;

    private static final Set<TxNode> PARTICIPANTS = DtxTestHelper.newRandomParticipantsSet();
    private static final int NB_OF_TRANSACTIONS = 30;

    private DtxManager dtxManager;
    private TransactionManager target;
    private Path tmpJournalDir;

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if setup fails
     */
    @Before
    public final void setUp() throws InitializationError {
        try {
            this.tmpJournalDir = Files.createTempDirectory(TestTransactionInitiator.class.getSimpleName());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
        final DtxManagerConfig dtxConfig = newDtxManagerConfig(tmpJournalDir);
        this.dtxManager = new DtxManager(dtxConfig);
        dtxManager.init();
        assertEquals(INITIALIZED, dtxManager.getStatus());
        target = new TransactionManager(dtxConfig, dtxManager);
        target.startUp(null);
    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if teardown fails
     */
    @After
    public final void tearDown() throws InitializationError {
        target.shutdown();
        dtxManager.fini();
        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tests the failure to construct a {@link TransactionManager} due to a <code>null</code> dtx manager configuration.
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testConstructionFailNullDtxManagerConfig() {
        new TransactionManager(null, dtxManager);
    }

    /**
     * Tests the failure to construct a {@link TransactionManager} due to a <code>null</code> {@link DtxManager}.
     * 
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testConstructionFailNullDtxManager() {
        new TransactionManager(newDtxManagerConfig(tmpJournalDir), null);
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#start(TxMessage, Iterable)} method.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testStart() throws XAException {

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        target.start(defTx, PARTICIPANTS);

    }

    /**
     * Tests failure of the {@link TransactionManager#start(TxMessage, Iterable)} method due to a bad payload format.
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testStartFailBadPayload() throws XAException {
        final UUID resUuid = registerResMgrWithTxMgr(target,
                DtxDummyRmFactory.newResMgrFailingOnStart(null, new XAException(XAException.XAER_INVAL)), null);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage badTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resUuid)).setPayload(ByteString.copyFrom(DtxDummyRmFactory.BAD_PAYLOAD))
                .build();

        try {
            target.start(badTx, PARTICIPANTS);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_INVAL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests failure of the {@link TransactionManager#start(TxMessage, Iterable)} method due to a shut down instance.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testStartFailShutdown() throws XAException, IllegalStateException {

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        target.shutdown();
        assertTrue(target.isShutdown());

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.start(defTx, PARTICIPANTS);

    }

    /**
     * Tests failure of the {@link TransactionManager#start(TxMessage, Iterable)} method due to an unavailable resource
     * manager.
     * 
     * @throws XAException
     *             expected for this test
     * @throws IllegalStateException
     *             if the instance is shut down, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testStartFailResMgrUnavailable() throws XAException, IllegalStateException {
        final UUID resId = UUID.randomUUID();
        assertNull(target.getRegisteredResourceManager(resId));

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resId)).build();

        try {
            target.start(defTx, PARTICIPANTS);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#prepare(long)} method.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testPrepare() throws XAException {
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        final UUID resUuid = resMgr.getId();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resMgr.getId())).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.commit(txId, PARTICIPANTS);
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#prepare(long)} method while unregistering a
     * non-existent resource manager, thus potentially skewing the lock count held by the transaction thread.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testPrepareLockingUnregResMgr() throws XAException {

        final UUID resUuid = UUID.randomUUID();
        final TransactionManager txMgr = target;
        final DtxResourceManager resMgr = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {

                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        final boolean answer = DtxDummyRmFactory.DefaultPrepareAnswer.doAnswer(invocation)
                                .booleanValue();
                        final boolean preHold = txMgr.holdsTransactionLock();
                        final UUID newResId = UUID.randomUUID();
                        txMgr.unregisterResourceManager(newResId);
                        return Boolean.valueOf(answer && preHold && txMgr.holdsTransactionLock());
                    }
                }).build();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resMgr.getId())).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.commit(txId, PARTICIPANTS);
    }

    /**
     * Tests the {@link TransactionManager#prepare(long)} method's failure while registering an already registered
     * resource manager, verifying there is no skew in the lock count held by the transaction thread.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalArgumentException
     *             expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testPrepareLockingRegResMgr() throws XAException, IllegalArgumentException {

        final UUID resUuid = UUID.randomUUID();
        final TransactionManager txMgr = target;
        final DtxResourceManager resMgr = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {

                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        final boolean answer = DtxDummyRmFactory.DefaultPrepareAnswer.doAnswer(invocation)
                                .booleanValue();
                        final boolean preHold = txMgr.holdsTransactionLock();
                        final DtxResourceManager newResId = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);
                        txMgr.registerResourceManager(newResId, null);
                        return Boolean.valueOf(answer && preHold && txMgr.holdsTransactionLock());
                    }
                }).build();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resMgr.getId())).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());
    }

    /**
     * Tests the {@link TransactionManager#prepare(long)} method's failure on trying to
     * {@link TransactionManager#shutdown()} the {@link TransactionManager} from within a transaction.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalThreadStateException
     *             if trying to alter the {@link TransactionManager}'s state from within a transaction fails, expected
     *             for this test
     */
    @Test(expected = IllegalThreadStateException.class)
    public final void testPrepareFailToShutdown() throws XAException, IllegalThreadStateException {

        final UUID resUuid = UUID.randomUUID();
        final TransactionManager txMgr = target;
        final DtxResourceManager resMgr = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {

                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        assertTrue(DtxDummyRmFactory.DefaultPrepareAnswer.doAnswer(invocation).booleanValue());
                        assertTrue(txMgr.holdsTransactionLock());
                        txMgr.shutdown();
                        return Boolean.FALSE;
                    }
                }).build();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resMgr.getId())).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());
    }

    /**
     * Tests the {@link TransactionManager#prepare(long)} method's failure on trying to startup the
     * {@link TransactionManager} from within a transaction.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalThreadStateException
     *             if trying to alter the {@link TransactionManager}'s state from within a transaction fails, expected
     *             for this test
     */
    @Test(expected = IllegalThreadStateException.class)
    public final void testPrepareFailToStartUp() throws XAException, IllegalThreadStateException {

        final UUID resUuid = UUID.randomUUID();
        final TransactionManager txMgr = target;
        final DtxResourceManager resMgr = new DtxDummyRmFactory.DtxResourceManagerBuilder().setId(resUuid)
                .setPrepare(null, new Answer<Boolean>() {

                    @Override
                    public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                        assertTrue(DtxDummyRmFactory.DefaultPrepareAnswer.doAnswer(invocation).booleanValue());
                        assertTrue(txMgr.holdsTransactionLock());
                        txMgr.startUp(null);
                        return Boolean.FALSE;
                    }
                }).build();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resMgr.getId())).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());
    }

    /**
     * Tests failure of the {@link TransactionManager#prepare(long)} method due to a shut down instance.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testPrepareFailShutdown() throws XAException, IllegalStateException {
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        final UUID resUuid = resMgr.getId();

        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resUuid)).build();

        target.start(defTx, PARTICIPANTS);

        target.shutdown();
        assertTrue(target.isShutdown());

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.prepare(txId);

    }

    /**
     * Tests failure of the {@link TransactionManager#prepare(long)} method due to an unavailable resource manager.
     * 
     * @throws XAException
     *             expected for this test
     * @throws IllegalStateException
     *             if the instance is shut down, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailResMgrUnavailable() throws XAException, IllegalStateException {
        final UUID resUuid = UUID.randomUUID();
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resUuid));

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resUuid)).build();

        target.start(defTx, PARTICIPANTS);

        target.unregisterResourceManager(resUuid);
        assertNull(target.getRegisteredResourceManager(resUuid));
        try {
            target.prepare(txId);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#commit(long)} method.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testCommit() throws XAException {

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.commit(txId, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link TransactionManager#commit(long)} method due to a shut down instance.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testCommitFailShutdown() throws XAException, IllegalStateException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.shutdown();
        assertTrue(target.isShutdown());

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.commit(txId, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link TransactionManager#commit(long)} method due to an unavailable resource manager.
     * 
     * @throws XAException
     *             expected for this test
     * @throws IllegalStateException
     *             if the instance is shut down, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailResMgrUnavailable() throws XAException, IllegalStateException {
        final UUID resId = UUID.randomUUID();
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resId);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resId));

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resId)).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.unregisterResourceManager(resId);
        assertNull(target.getRegisteredResourceManager(resId));
        try {
            target.commit(txId, PARTICIPANTS);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#rollback(long)} method on a started transaction.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testRollbackStarted() throws XAException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.start(defTx, PARTICIPANTS);

        target.rollback(txId, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link TransactionManager#rollback(long)} method on a started transaction of a shut down
     * instance.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testRollbackStartedFailShutdown() throws XAException, IllegalStateException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.start(defTx, PARTICIPANTS);

        target.shutdown();
        assertTrue(target.isShutdown());

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.rollback(txId, PARTICIPANTS);
    }

    /**
     * Tests the successful execution of the {@link TransactionManager#rollback(long)} method on a prepared transaction.
     * 
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testRollbackPrepared() throws XAException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.rollback(txId, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link TransactionManager#rollback(long)} method on a prepared transaction of a shut down
     * instance.
     * 
     * @throws XAException
     *             not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testRollbackPreparedFailShutdown() throws XAException, IllegalStateException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final TxMessage defTx = buildDefaultTransaction(resUuid);

        final long txId = defTx.getTxId();

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.shutdown();
        assertTrue(target.isShutdown());

        target.setResManagerSyncState(resUuid, UP_TO_DATE);

        target.rollback(txId, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link TransactionManager#rollback(long)} method due to an unavailable resource manager.
     * 
     * @throws XAException
     *             expected for this test
     * @throws IllegalStateException
     *             if the instance is shut down, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testRollbackFailResMgrUnavailable() throws XAException, IllegalStateException {
        final UUID resId = UUID.randomUUID();
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resId);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resId));

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resId)).build();

        target.start(defTx, PARTICIPANTS);

        assertTrue(target.prepare(txId).booleanValue());

        target.unregisterResourceManager(resId);
        assertNull(target.getRegisteredResourceManager(resId));
        try {
            target.rollback(txId, PARTICIPANTS);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests failure of the {@link TransactionManager#rollback(long)} method due to an resource manager having
     * unregistered itself during prepare.
     * 
     * Note: this simply reproduces a deadlock situation if the {@link TransactionManager} does not allow un/registering
     * during transaction phase execution. While this would seem like sane behavior, in practice un/registration must be
     * able to proceed regardless of any currently executing transactions, as unregistering does not prevent running
     * transaction phases from proceeding.
     * 
     * @throws XAException
     *             expected for this test
     * @throws TimeoutException
     *             if the prepare operation times out, considered a test failure
     * @throws ExecutionException
     *             if the prepare operation execution fails, considered a test failure
     * @throws InterruptedException
     *             if the thread executing the prepare operation is interrupted, not part of this test
     * @throws IllegalStateException
     *             if the instance is shut down, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testInTransactionResMgrRegistrations() throws XAException, InterruptedException,
            ExecutionException, TimeoutException {
        final UUID resId = UUID.randomUUID();
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrUnregisteringOnPrepare(resId, target);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resId));

        final long txId = DtxTestHelper.nextTxId();

        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(txId).setResId(toUuid(resId)).build();

        target.start(defTx, PARTICIPANTS);

        final ExecutorService executor = Executors.newFixedThreadPool(1);

        final Future<Boolean> prepFuture = executor.submit(new Callable<Boolean>() {

            @Override
            public final Boolean call() throws Exception {
                return target.prepare(txId);
            }
        });

        try {
            final boolean result = prepFuture.get(FUTURE_TIMEOUT_S, TimeUnit.SECONDS).booleanValue();
            assertTrue(result);
        }
        finally {
            // this only works if locks are acquired in an interruptible way
            executor.shutdownNow();
        }

        assertNull(target.getRegisteredResourceManager(resId));

        try {
            target.rollback(txId, PARTICIPANTS);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_RMFAIL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the execution of {@value #NB_OF_TRANSACTIONS} transactions out of order in the start phase, then in order
     * for the rest.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testMultipleTxInOrder() throws XAException {
        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        // create a strictly growing sequence of transaction IDs
        final ArrayList<Long> txIdList = new ArrayList<Long>(NB_OF_TRANSACTIONS);
        for (int i = 0; i < NB_OF_TRANSACTIONS; i++) {
            txIdList.add(Long.valueOf(i ^ 2 + 1));
        }

        // starts all transactions in a random order
        Collections.shuffle(txIdList);
        for (final Long currTxId : txIdList) {
            target.start(
                    TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                            .setTxId(currTxId.longValue()).setResId(toUuid(resUuid)).build(), PARTICIPANTS);
        }

        // prepares and commits/rolls back in order
        Collections.sort(txIdList);
        for (final Long currTxId : txIdList) {
            final long txId = currTxId.longValue();
            assertEquals(Boolean.TRUE, target.prepare(txId));
            if (txId % 2 == 0) {
                target.commit(txId, PARTICIPANTS);
            }
            else {
                target.rollback(txId, PARTICIPANTS);
            }
        }
    }

    /**
     * Tests the successful (and repeated) execution of the
     * {@link TransactionManager#getLastCompleteTxIdForResMgr(UUID)} method.
     * 
     * @throws IllegalStateException
     *             if the [@link TransactionManager} is not started, not part of this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test
    public final void testGetLastCompleteTxId() throws XAException {
        assertFalse(target.isShutdown());

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        // inits and writes the first 'last transaction'
        long lastTxId = DtxTestHelper.nextTxId();
        target.start(TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(lastTxId)
                .setResId(toUuid(resUuid)).build(), PARTICIPANTS);
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        assertTrue(target.prepare(lastTxId).booleanValue());
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        target.commit(lastTxId, PARTICIPANTS);
        assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

        // writes more partial and complete transactions
        for (int i = 0; i < NB_OF_TRANSACTIONS; i++) {
            final long nextTxId = DtxTestHelper.nextTxId();
            // writes start and checks if the old value's still returned
            target.start(
                    TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(nextTxId)
                            .setResId(toUuid(resUuid)).build(), PARTICIPANTS);
            assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

            assertTrue(target.prepare(nextTxId).booleanValue());
            assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

            // checks if the next ID is returned after completing the transaction
            if (nextTxId % 2 == 0) {
                target.commit(nextTxId, PARTICIPANTS);
            }
            else {
                target.rollback(nextTxId, PARTICIPANTS);
            }
            assertEquals(nextTxId, target.getLastCompleteTxIdForResMgr(resUuid));
            lastTxId = nextTxId;
        }
    }

    /**
     * Tests the possibility to call the {@link TransactionManager#start(TxMessage, Iterable)} with out-of-order
     * transaction IDs.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testStartOutOfOrder() throws XAException {

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        final long firstTxId = DtxTestHelper.nextTxId();
        final long secondTxId = DtxTestHelper.nextTxId();

        final TxMessage firstTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(firstTxId).setResId(toUuid(resUuid)).build();
        final TxMessage secondTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(secondTxId).setResId(toUuid(resUuid)).build();

        target.start(secondTx, PARTICIPANTS);

        target.start(firstTx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(firstTxId));

        target.commit(firstTxId, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(secondTxId));

        target.commit(secondTxId, PARTICIPANTS);
    }

    /**
     * Tests the ability to read a given task in the journal with different status.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test
    public final void testReadTask() throws XAException {

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        // Create a new task with status committed
        long txId = DtxTestHelper.nextTxId();
        TxMessage tx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setResId(toUuid(resUuid)).setTaskId(toUuid(UUID.randomUUID())).build();

        target.start(tx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(txId));

        target.commit(txId, PARTICIPANTS);

        UUID taskId = fromUuid(tx.getTaskId());
        TaskLoader task = target.readTask(taskId);

        assertEquals(taskId, UUID.fromString(task.getDtxTaskAdm().getTaskId()));
        assertEquals(DtxTaskStatus.COMMITTED, task.getDtxTaskAdm().getStatus());
        assertEquals(resUuid, UUID.fromString(task.getDtxTaskAdm().getResourceId()));

        // Create a new task with status rollback
        txId = DtxTestHelper.nextTxId();
        tx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setResId(toUuid(resUuid)).setTaskId(toUuid(UUID.randomUUID())).build();

        target.start(tx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(txId));

        target.rollback(txId, PARTICIPANTS);

        taskId = fromUuid(tx.getTaskId());
        task = target.readTask(taskId);

        assertEquals(taskId, UUID.fromString(task.getDtxTaskAdm().getTaskId()));
        assertEquals(DtxTaskStatus.ROLLED_BACK, task.getDtxTaskAdm().getStatus());
        assertEquals(resUuid, UUID.fromString(task.getDtxTaskAdm().getResourceId()));

        // Create a new task with status started
        txId = DtxTestHelper.nextTxId();
        tx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setResId(toUuid(resUuid)).setTaskId(toUuid(UUID.randomUUID())).build();

        target.start(tx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(txId));

        taskId = fromUuid(tx.getTaskId());
        task = target.readTask(taskId);

        assertEquals(taskId, UUID.fromString(task.getDtxTaskAdm().getTaskId()));
        assertEquals(DtxTaskStatus.STARTED, task.getDtxTaskAdm().getStatus());
        assertEquals(resUuid, UUID.fromString(task.getDtxTaskAdm().getResourceId()));
    }

    /**
     * Tests the {@link TransactionManager#prepare(long)} method's failure before the preceding transaction has been
     * committed.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailBeforePrecedingCommit() throws XAException {
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resMgr.getId()));

        final long firstTxId = DtxTestHelper.nextTxId();
        final long secondTxId = DtxTestHelper.nextTxId();

        final TxMessage firstTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(firstTxId).setResId(toUuid(resMgr.getId())).build();
        final TxMessage secondTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(secondTxId).setResId(toUuid(resMgr.getId())).build();

        target.start(firstTx, PARTICIPANTS);

        target.start(secondTx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(firstTxId));

        try {
            target.prepare(secondTxId);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the successful repeated execution of the {@link TransactionManager#getLastCompleteTxIdForResMgr(UUID)}
     * method, shutting down the {@link TransactionManager} in between invocations.
     * 
     * @throws IllegalStateException
     *             if the [@link TransactionManager} is not started, not part of this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test
    public final void testGetLastCompleteTxIdAfterRestart() throws XAException {
        assertFalse(target.isShutdown());

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        // inits and writes one 'last transaction'
        long lastTxId = DtxTestHelper.nextTxId();
        target.start(TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(lastTxId)
                .setResId(toUuid(resUuid)).build(), PARTICIPANTS);
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        assertTrue(target.prepare(lastTxId).booleanValue());
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        target.commit(lastTxId, PARTICIPANTS);
        assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

        // writes more partial and complete transactions
        for (int i = 0; i < NB_OF_TRANSACTIONS; i++) {
            final long nextTxId = DtxTestHelper.nextTxId();

            try {
                // writes start and checks if the old value's still returned
                target.start(
                        TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                                .setTxId(nextTxId).setResId(toUuid(resUuid)).build(), PARTICIPANTS);
                assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

                assertTrue(target.prepare(nextTxId).booleanValue());
                assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));
            }
            catch (final XAException xe) {
                LOGGER.error("XAException during first transaction phase; code=" + xe.errorCode);
                throw xe;
            }

            // shuts down and starts up again
            target.shutdown();
            assertTrue(target.isShutdown());
            target.startUp(null);
            assertFalse(target.isShutdown());
            target.setResManagerSyncState(resUuid, UP_TO_DATE);
            assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

            // checks if the next ID is returned after completing the transaction
            try {
                if (nextTxId % 2 == 0) {
                    target.commit(nextTxId, PARTICIPANTS);
                }
                else {
                    target.rollback(nextTxId, PARTICIPANTS);
                }
            }
            catch (final XAException xe) {
                LOGGER.error("XAException while completing transaction; code=" + xe.errorCode);
                throw xe;
            }
            assertEquals(nextTxId, target.getLastCompleteTxIdForResMgr(resUuid));

            // shuts down and starts up again
            target.shutdown();
            assertTrue(target.isShutdown());
            target.startUp(null);
            assertFalse(target.isShutdown());
            target.setResManagerSyncState(resUuid, UP_TO_DATE);

            assertEquals(nextTxId, target.getLastCompleteTxIdForResMgr(resUuid));

            lastTxId = nextTxId;
        }
    }

    /**
     * Tests the {@link TransactionManager#prepare(long)} method's failure after a following transaction was prepared.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailAfterFollowingPrepared() throws XAException {
        final DtxResourceManager resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
        target.registerResourceManager(resMgr, null);
        assertEquals(resMgr, target.getRegisteredResourceManager(resMgr.getId()));

        final long firstTxId = DtxTestHelper.nextTxId();
        final long secondTxId = DtxTestHelper.nextTxId();

        final TxMessage firstTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(firstTxId).setResId(toUuid(resMgr.getId())).build();
        final TxMessage secondTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(secondTxId).setResId(toUuid(resMgr.getId())).build();

        target.start(firstTx, PARTICIPANTS);

        target.start(secondTx, PARTICIPANTS);

        assertEquals(Boolean.TRUE, target.prepare(secondTxId));

        try {
            target.prepare(firstTxId);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link TransactionManager#getLastCompleteTxIdForResMgr(UUID)} method's failure due to a non-started
     * state.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws XAException
     *             if construction of mock {@link DtxResourceManager}s fails, not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testGetLastCompleteTxIdFailNotStarted() throws XAException {
        assertFalse(target.isShutdown());

        final UUID resUuid = registerResMgrWithTxMgr(target, null, null);

        // inits and writes one 'last transaction'
        final long lastTxId = DtxTestHelper.nextTxId();
        target.start(TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(lastTxId)
                .setResId(toUuid(resUuid)).build(), PARTICIPANTS);
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        assertTrue(target.prepare(lastTxId).booleanValue());
        assertEquals(DEFAULT_LAST_TX_VALUE, target.getLastCompleteTxIdForResMgr(resUuid));

        target.commit(lastTxId, PARTICIPANTS);
        assertEquals(lastTxId, target.getLastCompleteTxIdForResMgr(resUuid));

        target.shutdown();
        assertTrue(target.isShutdown());

        target.getLastCompleteTxIdForResMgr(resUuid);
    }

}
