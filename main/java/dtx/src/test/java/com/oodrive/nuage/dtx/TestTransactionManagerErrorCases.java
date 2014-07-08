package com.oodrive.nuage.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import static com.oodrive.nuage.dtx.DtxNodeState.INITIALIZED;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;
import static com.oodrive.nuage.dtx.DtxTestHelper.DEFAULT_TX_MESSAGE;
import static com.oodrive.nuage.dtx.proto.TxProtobufUtils.toUuid;
import static javax.transaction.xa.XAException.XAER_INVAL;
import static javax.transaction.xa.XAException.XAER_NOTA;
import static javax.transaction.xa.XAException.XAER_PROTO;
import static javax.transaction.xa.XAException.XAER_RMERR;
import static javax.transaction.xa.XAException.XAER_RMFAIL;
import static javax.transaction.xa.XAException.XA_RBINTEGRITY;
import static javax.transaction.xa.XAException.XA_RBPROTO;
import static javax.transaction.xa.XAException.XA_RBROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxMessage;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxNode;

/**
 * Tests for all error cases included in the transaction execution protocol implemented by the
 * {@link TransactionManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
@RunWith(Parameterized.class)
public final class TestTransactionManagerErrorCases {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTransactionManagerErrorCases.class);

    private static final Set<TxNode> PARTICIPANTS = DtxTestHelper.newRandomParticipantsSet();

    private DtxManager dtxManager;

    private Path tmpJournalDir;

    /**
     * Operation identifiers.
     * 
     * 
     */
    public enum TestOp {
        /** execute the {@link TransactionManager#start(TxMessage, Iterable)} method. */
        START,
        /** execute the {@link TransactionManager#prepare(long)} method. */
        PREPARE,
        /** execute the {@link TransactionManager#commit(long)} method. */
        COMMIT,
        /** execute the {@link TransactionManager#rollback(long)} method. */
        ROLLBACK;
    };

    private static final int[] TM_PROTO = new int[] { XAER_PROTO };
    private static final int[] TM_NOTA = new int[] { XAER_NOTA };
    private static final int[] TM_INTERNAL = new int[] { XAER_NOTA, XAER_PROTO };
    private static final int[] RM_INTERNAL = { XAER_RMERR, XAER_INVAL, XAER_RMFAIL };
    private static final int[] RM_ROLLBACK = { XA_RBROLLBACK, XA_RBINTEGRITY, XA_RBPROTO };

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if setup fails
     */
    @Before
    public final void setUp() throws InitializationError {

        try {
            this.tmpJournalDir = Files.createTempDirectory(TestTransactionManagerErrorCases.class.getSimpleName());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        final DtxManagerConfig dtxConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
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
        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Parameter generator method for the {@link Parameterized} test.
     * 
     * @return a {@link List} of parameter combinations
     */
    @Parameters
    public static List<Object[]> data() {
        final ArrayList<Object[]> result = new ArrayList<Object[]>();
        for (final DtxTaskStatus currStatus : DtxTaskStatus.values()) {
            final HashMap<TestOp, int[]> opErrMap = new HashMap<TestOp, int[]>();
            switch (currStatus) {
            case PENDING:
                opErrMap.put(TestOp.START, concatAll(RM_INTERNAL, RM_ROLLBACK));
                opErrMap.put(TestOp.PREPARE, TM_NOTA);
                opErrMap.put(TestOp.COMMIT, TM_NOTA);
                opErrMap.put(TestOp.ROLLBACK, TM_NOTA);
                break;
            case STARTED:
                opErrMap.put(TestOp.START, TM_PROTO);
                opErrMap.put(TestOp.PREPARE, concatAll(RM_INTERNAL, RM_ROLLBACK));
                opErrMap.put(TestOp.COMMIT, TM_PROTO);
                opErrMap.put(TestOp.ROLLBACK, concatAll(RM_INTERNAL, RM_INTERNAL));
                break;
            case PREPARED:
                opErrMap.put(TestOp.START, TM_PROTO);
                opErrMap.put(TestOp.PREPARE, TM_PROTO);
                opErrMap.put(TestOp.COMMIT, concatAll(RM_INTERNAL, RM_ROLLBACK));
                opErrMap.put(TestOp.ROLLBACK, RM_INTERNAL);
                break;
            case COMMITTED:
                opErrMap.put(TestOp.START, TM_NOTA);
                opErrMap.put(TestOp.PREPARE, TM_NOTA);
                opErrMap.put(TestOp.COMMIT, TM_NOTA);
                opErrMap.put(TestOp.ROLLBACK, TM_NOTA);
                break;
            case ROLLED_BACK:
                opErrMap.put(TestOp.START, TM_NOTA);
                opErrMap.put(TestOp.PREPARE, TM_NOTA);
                opErrMap.put(TestOp.COMMIT, TM_NOTA);
                opErrMap.put(TestOp.ROLLBACK, TM_NOTA);
                break;
            default:
                break;
            }

            for (final TestOp currOp : opErrMap.keySet()) {
                final int[] currErrList = opErrMap.get(currOp);
                for (int i = 0; i < currErrList.length; i++) {
                    result.add(new Object[] { currOp, currStatus, new XAException(currErrList[i]) });
                }
            }
        }
        return result;
    }

    private final DtxTaskStatus initialStatus;
    private final XAException checkException;
    private final TestOp testOp;

    private TransactionManager target;

    /**
     * Parameterized constructor.
     * 
     * @param operation
     *            the {@link TestOp operation} to test
     * @param status
     *            the status of the transaction upon calling the operation
     * @param xe
     *            the exception to throw
     */
    public TestTransactionManagerErrorCases(final TestOp operation, final DtxTaskStatus status, final XAException xe) {
        this.testOp = operation;
        this.initialStatus = status;
        this.checkException = xe;
    }

    /**
     * Test a precise failure case for one of the {@link TransactionManager} methods.
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testOperationFailure() throws XAException {
        LOGGER.debug("Executing operation failure test; status=" + initialStatus + ", operation=" + testOp
                + ", expected=" + checkException.errorCode);

        final DtxResourceManager initialResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);

        final UUID resourceId = initialResMgr.getId();

        dtxManager.init();
        assertEquals(DtxNodeState.INITIALIZED, dtxManager.getStatus());

        target.registerResourceManager(initialResMgr, null);
        assertEquals(initialResMgr, target.getRegisteredResourceManager(resourceId));

        final TxMessage startTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1)
                .setTxId(DtxTestHelper.nextTxId()).setResId(toUuid(resourceId)).build();
        final long txId = startTx.getTxId();

        switch (this.initialStatus) {
        case STARTED:
            target.start(startTx, PARTICIPANTS);
            break;
        case PREPARED:
            target.start(startTx, PARTICIPANTS);
            target.prepare(txId);
            break;
        case COMMITTED:
            target.start(startTx, PARTICIPANTS);
            target.prepare(txId);
            target.commit(txId, PARTICIPANTS);
            break;
        case ROLLED_BACK:
            target.start(startTx, PARTICIPANTS);
            target.rollback(txId, PARTICIPANTS);
            break;
        case PENDING:
        default:
        }

        // exclude exceptions generated by the transaction manager from the mock initialization
        if (Arrays.binarySearch(TM_INTERNAL, checkException.errorCode) < 0) {

            // unregister the initial resource manager and replace with a faulty one
            target.unregisterResourceManager(initialResMgr.getId());
            assertNull(target.getRegisteredResourceManager(resourceId));

            final DtxResourceManager resMgr;
            switch (this.testOp) {
            case START:
                resMgr = DtxDummyRmFactory.newResMgrFailingOnStart(resourceId, checkException);
                break;
            case PREPARE:
                resMgr = DtxDummyRmFactory.newResMgrFailingOnPrepare(resourceId, checkException);
                break;
            case COMMIT:
                resMgr = DtxDummyRmFactory.newResMgrFailingOnCommit(resourceId, checkException);
                break;
            case ROLLBACK:
                resMgr = DtxDummyRmFactory.newResMgrFailingOnRollback(resourceId, checkException);
                break;
            default:
                resMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resourceId);
            }

            target.registerResourceManager(resMgr, null);
            assertEquals(resMgr, target.getRegisteredResourceManager(resMgr.getId()));
        }

        target.setResManagerSyncState(resourceId, UP_TO_DATE);

        // execute the target operation and analyze the thrown exception
        try {
            switch (this.testOp) {
            case START:
                target.start(startTx, PARTICIPANTS);
                break;
            case PREPARE:
                target.prepare(txId);
                break;
            case COMMIT:
                target.commit(txId, PARTICIPANTS);
                break;
            case ROLLBACK:
                target.rollback(txId, PARTICIPANTS);
                break;
            default:
                break;
            }
            LOGGER.error("No exception thrown; expected=" + checkException.errorCode + ", operation=" + this.testOp
                    + ", initialState=" + this.initialStatus);
        }
        catch (final XAException xe) {
            if (checkException.errorCode != xe.errorCode) {
                LOGGER.error("Unexpected error; code=" + xe.errorCode + ", expected=" + checkException.errorCode
                        + ", operation=" + this.testOp + ", initialState=" + this.initialStatus);
            }
            assertEquals(checkException.errorCode, xe.errorCode);
            throw xe;
        }
    }

    private static int[] concatAll(final int[] first, final int[]... rest) {
        int totalLength = first.length;
        for (final int[] currArray : rest) {
            totalLength += currArray.length;
        }
        final int[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (final int[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }
}
