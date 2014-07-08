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

import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static com.oodrive.nuage.dtx.DtxDummyRmFactory.DEFAULT_PAYLOAD;
import static com.oodrive.nuage.dtx.DtxDummyRmFactory.newResMgrThatDoesEverythingRight;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;
import static com.oodrive.nuage.dtx.DtxTaskStatus.COMMITTED;
import static com.oodrive.nuage.dtx.DtxTaskStatus.PENDING;
import static com.oodrive.nuage.dtx.DtxTestHelper.awaitStateUpdate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import javax.transaction.xa.XAException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.dtx.journal.JournalRotationManager;
import com.oodrive.nuage.dtx.journal.WritableTxJournal;

/**
 * Tests for the initialization phase of the DtxManager, including journal recovery for resource managers.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestDtxManagerInitL {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDtxManagerInitL.class);

    private static final int NB_OF_SHUTDOWN_LOOPS = 5;
    private static final int NB_OF_RESSOURCE_MANAGERS = 4;

    private static final int TX_WAIT_RETRIES = 5;
    private static final int TX_WAIT_MS = 100;

    private static final int TEST_NB_OF_TEST_ENTRIES = 30;
    private static final long TEST_ROTATION_THRESHOLD = 3221225472L; // keep this high to avoid rotation

    private Path tmpJournalDir;
    private DtxManager dtxManager;
    private JournalRotationManager fixtureRotMgr;

    private DtxManagerConfig dtxConfig;

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if creating temporary directories fails
     */
    @Before
    public final void setUp() throws InitializationError {
        try {
            tmpJournalDir = Files.createTempDirectory(TestDtxManager.class.getSimpleName());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        fixtureRotMgr = new JournalRotationManager(0);
        fixtureRotMgr.start();

        this.dtxConfig = DtxTestHelper.newDtxManagerConfig(tmpJournalDir);
        this.dtxManager = new DtxManager(dtxConfig);
    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if removing temporary files fails
     */
    @After
    public final void tearDown() throws InitializationError {

        dtxManager.fini();

        fixtureRotMgr.stop();

        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tests the initialization with repeated start/stop cycles for a single {@link DtxManager} instance with one
     * existing journal.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IllegalStateException
     *             if starting and/or writing fails, not part of this test
     * @throws IOException
     *             if any I/O from/to disk fails, not part of this test
     * @throws InterruptedException
     *             if waiting for the {@link DtxManager}'s start times out, not part of this test
     * @throws TimeoutException
     *             if waiting on the {@link DtxResourceManager}s sync state times out, not part of this test
     */
    @Test
    public final void testDtxManagerInitOneExistingJournal() throws XAException, IllegalStateException, IOException,
            InterruptedException, TimeoutException {
        LOGGER.info("Executing");
        final UUID resUuid = UUID.randomUUID();
        final DtxResourceManager targetResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);

        dtxManager.init();
        for (int i = 0; i < NB_OF_SHUTDOWN_LOOPS; i++) {
            final long lastTxId = writeTxToJournal(resUuid);

            dtxManager.registerResourceManager(targetResMgr);

            awaitStateUpdate(dtxManager, resUuid, UP_TO_DATE);

            try {
                assertEquals(lastTxId, dtxManager.getLastCompleteTxIdForResMgr(resUuid));
                dtxManager.unregisterResourceManager(resUuid);

                dtxManager.registerResourceManager(targetResMgr);

                assertEquals(lastTxId, dtxManager.getLastCompleteTxIdForResMgr(resUuid));

                dtxManager.unregisterResourceManager(resUuid);
            }
            finally {
                dtxManager.stop();
            }
        }
    }

    /**
     * Tests the initialization with repeated start/stop cycles for a single {@link DtxManager} instance, with added
     * transaction executions and verification of the updated transaction ID after each cycle.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IllegalStateException
     *             if starting and/or writing fails, not part of this test
     * @throws IOException
     *             if any I/O from/to disk fails, not part of this test
     * @throws InterruptedException
     *             if waiting for the {@link DtxManager}'s start times out, not part of this test
     * @throws TimeoutException
     *             if waiting on the {@link DtxResourceManager}s sync state times out, not part of this test
     */
    @Test
    public final void testDtxManagerInitVerifyTxIdAfterRegistration() throws XAException, IllegalStateException,
            IOException, InterruptedException, TimeoutException {
        LOGGER.info("Executing");
        final UUID resUuid = UUID.randomUUID();
        final DtxResourceManager targetResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);

        dtxManager.init();

        for (int i = 1; i <= NB_OF_SHUTDOWN_LOOPS; i++) {

            final long lastTxId = writeTxToJournal(resUuid);

            dtxManager.registerResourceManager(targetResMgr);

            awaitStateUpdate(dtxManager, resUuid, UP_TO_DATE);

            try {
                assertEquals(lastTxId, dtxManager.getLastCompleteTxIdForResMgr(resUuid));

                final UUID taskId = dtxManager.submit(resUuid, DtxDummyRmFactory.DEFAULT_PAYLOAD);

                DtxMockUtils.verifySuccessfulTxExecution(targetResMgr, i);

                int retryCount = 0;
                while (!COMMITTED.equals(dtxManager.getTask(taskId).getStatus()) && retryCount < TX_WAIT_RETRIES) {
                    try {
                        Thread.sleep(TX_WAIT_MS);
                    }
                    catch (final InterruptedException e) {
                        LOGGER.warn("Interrupted during sleep");
                    }
                    retryCount++;
                }

                assertEquals(lastTxId + 1, dtxManager.getLastCompleteTxIdForResMgr(resUuid));

                dtxManager.unregisterResourceManager(resUuid);

            }
            finally {
                dtxManager.stop();
            }
        }
    }

    /**
     * Tests the initialization with repeated start/stop cycles for a single {@link DtxManager} instance, with added
     * transaction executions and verifies merging of the modified resource manager's state with running transactions.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IllegalStateException
     *             if the target resource manager enters an invalid state, not part of this test
     * @throws IOException
     *             if any I/O from/to disk fails, not part of this test
     * @throws InterruptedException
     *             if waiting for the {@link DtxManager}'s start times out, not part of this test
     * @throws TimeoutException
     *             if waiting on the {@link DtxResourceManager}s sync state times out, not part of this test
     */
    @Test
    public final void testDtxManagerInitMergeLastTxId() throws XAException, IllegalStateException, IOException,
            InterruptedException, TimeoutException {
        LOGGER.info("Executing");
        final UUID resUuid = UUID.randomUUID();
        final DtxResourceManager targetResMgr = DtxDummyRmFactory.newResMgrThatDoesEverythingRight(resUuid);

        dtxManager.init();

        dtxManager.registerResourceManager(targetResMgr);

        awaitStateUpdate(dtxManager, resUuid, UP_TO_DATE);

        long lastTxId = DEFAULT_LAST_TX_VALUE;
        try {
            for (int i = 1; i <= NB_OF_SHUTDOWN_LOOPS; i++) {

                assertEquals(lastTxId, dtxManager.getLastCompleteTxIdForResMgr(resUuid));

                final UUID taskId = dtxManager.submit(resUuid, DEFAULT_PAYLOAD);

                DtxMockUtils.verifySuccessfulTxExecution(targetResMgr, i);

                int retryCount = 0;
                while (!COMMITTED.equals(dtxManager.getTask(taskId).getStatus()) && retryCount < TX_WAIT_RETRIES) {
                    try {
                        Thread.sleep(TX_WAIT_MS);
                    }
                    catch (final InterruptedException e) {
                        LOGGER.warn("Interrupted during sleep");
                    }
                    retryCount++;
                }
                lastTxId = dtxManager.getLastCompleteTxIdForResMgr(resUuid);
            }
            dtxManager.unregisterResourceManager(resUuid);

            // write more to the journal
            lastTxId = writeTxToJournal(resUuid);

            // submit some transactions to have them execute while we try to merge
            for (int i = 1; i <= NB_OF_SHUTDOWN_LOOPS; i++) {
                final UUID taskId = dtxManager.submit(resUuid, DEFAULT_PAYLOAD);
                int retryCount = 0;
                while (PENDING != dtxManager.getTask(taskId).getStatus() && retryCount < TX_WAIT_RETRIES) {
                    try {
                        Thread.sleep(TX_WAIT_MS);
                    }
                    catch (final InterruptedException e) {
                        LOGGER.warn("Interrupted during sleep");
                    }
                    retryCount++;
                }
            }

            dtxManager.registerResourceManager(targetResMgr);
            assertTrue(lastTxId <= dtxManager.getLastCompleteTxIdForResMgr(resUuid));

        }
        finally {
            dtxManager.stop();
        }
    }

    /**
     * Tests the initialization of a single {@link DtxManager} instance with multiple existing resource manager
     * journals.
     * 
     * @throws XAException
     *             if mock setup fails, not part of this test
     * @throws IllegalStateException
     *             , if starting and/or writing fails, not part of this test
     * @throws IOException
     *             if any I/O from/to disk fails, not part of this test
     * @throws InterruptedException
     *             if waiting for the {@link DtxManager}'s start times out, not part of this test
     * @throws TimeoutException
     *             if waiting on the {@link DtxResourceManager}s sync state times out, not part of this test
     */
    @Test
    public final void testDtxManagerInitMultipleExistingJournals() throws XAException, IllegalStateException,
            IOException, InterruptedException, TimeoutException {
        LOGGER.info("Executing");

        final ArrayList<DtxResourceManager> resourceMgrs = new ArrayList<DtxResourceManager>(NB_OF_RESSOURCE_MANAGERS);
        for (int i = 0; i < NB_OF_RESSOURCE_MANAGERS; i++) {
            resourceMgrs.add(newResMgrThatDoesEverythingRight(null));
        }

        final HashMap<UUID, Long> lastTxIds = new HashMap<UUID, Long>(NB_OF_RESSOURCE_MANAGERS);
        long maxWrittenTxId = 0;
        for (final DtxResourceManager currResMgr : resourceMgrs) {
            final long lastWrittenTxId = writeTxToJournal(currResMgr.getId());
            maxWrittenTxId = Math.max(maxWrittenTxId, lastWrittenTxId);
            lastTxIds.put(currResMgr.getId(), Long.valueOf(lastWrittenTxId));
        }

        Collections.shuffle(resourceMgrs);

        dtxManager.init();
        for (final DtxResourceManager currResMgr : resourceMgrs) {
            dtxManager.registerResourceManager(currResMgr);
            awaitStateUpdate(dtxManager, currResMgr.getId(), UP_TO_DATE);
        }

        try {
            long maxReadTxId = 0;
            for (final DtxResourceManager currResMgr : resourceMgrs) {
                final UUID resUuid = currResMgr.getId();
                final long lastTxId = lastTxIds.get(currResMgr.getId()).longValue();

                assertEquals(lastTxId, dtxManager.getLastCompleteTxIdForResMgr(resUuid));
                dtxManager.unregisterResourceManager(resUuid);

                dtxManager.registerResourceManager(currResMgr);

                final long lastCompleteTxId = dtxManager.getLastCompleteTxIdForResMgr(resUuid);
                assertEquals(lastTxId, lastCompleteTxId);
                maxReadTxId = Math.max(maxReadTxId, lastCompleteTxId);
            }
            assertEquals(maxWrittenTxId, maxReadTxId);
        }
        finally {
            dtxManager.stop();
        }
    }

    // TODO: add tests for error cases and initialization states

    private final long writeTxToJournal(final UUID resourceId) throws IOException {
        if (!fixtureRotMgr.isStarted()) {
            fixtureRotMgr.start();
        }
        final String journalFilename = TransactionManager.newJournalFilePrefix(dtxManager.getNodeId(), resourceId);

        final File journalDir = dtxConfig.getJournalDirectory().toFile();
        if (!journalDir.exists()) {
            journalDir.mkdirs();
        }

        final WritableTxJournal targetJournal = new WritableTxJournal(journalDir, journalFilename,
                TEST_ROTATION_THRESHOLD, fixtureRotMgr);
        targetJournal.start();

        final long lastTxId = DtxTestHelper.writeCompleteTransactions(targetJournal, TEST_NB_OF_TEST_ENTRIES, null,
                DtxTestHelper.newRandomParticipantsSet());

        fixtureRotMgr.stop();
        targetJournal.stop();

        return lastTxId;

    }

}
