package com.oodrive.nuage.dtx.journal;

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

import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_JOURNAL_FILE_PREFIX;
import static com.oodrive.nuage.dtx.DtxTestHelper.writeCompleteTransactions;
import static com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent.RotationStage.ROTATE_SUCCESS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.dtx.DtxTestHelper;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationListener;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxNode;

/**
 * Tests the rotation methods of the {@link WritableTxJournal} class.
 * 
 * Consider running these tests using the {@link TmpToRamJUnitRunner}, as they last very long due to large amounts of
 * transactions being written synchronously to disk.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestJournalRotation {

    /**
     * A {@link RotationListener} counting events with {@link RotationEvent.RotationStage#ROTATE_SUCCESS}.
     * 
     * 
     */
    static final class RotationSuccessCounter implements RotationListener {

        private final AtomicInteger rotationCounter = new AtomicInteger();

        @Override
        public void rotationEventOccured(final RotationEvent rotevt) {
            if (ROTATE_SUCCESS.equals(rotevt.getStage())) {
                rotationCounter.incrementAndGet();
            }
        }

        /**
         * Gets the current successful rotation event count.
         * 
         * @return a positive or zero integer
         */
        final int getCount() {
            return rotationCounter.get();
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestJournalRotation.class);

    private static final int NB_TEST_ROTATIONS = 5;
    private static final int NB_CONCURRENT_JOURNALS = 5;

    private static final int ROTATION_WAIT_DELAY_RETRIES = 10;
    private static final int ROTATION_WAIT_DELAY_MS = 1000;
    private static final int ROTATOR_THREADCOUNT = 3;

    private static final Set<TxNode> PARTICIPANTS = DtxTestHelper.newRandomParticipantsSet();

    private static final Path RAMDISK_PATH = FileSystems.getDefault().getPath("/run/shm");

    private static final long ROTATION_THRESHOLD_RAMDISK = 2097152L;
    private static final long ROTATION_THRESHOLD_HDD = 6144L;

    private boolean tmpToRamActive = false;

    private long rotationThresholdBytes = 0;

    private JournalRotationManager journalRotMgr;
    private File tmpJournalDir;

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if setting up temporary storage fails
     */
    @Before
    public final void setUp() throws InitializationError {

        // redirect temporary file directory to ramdisk, if possible
        // TODO: this should be factorized as a custom runner or abstract superclass, but those solutions don't work
        // with maven and/or coverage tools
        tmpToRamActive = Files.exists(RAMDISK_PATH);

        try {
            if (tmpToRamActive) {
                this.tmpJournalDir = Files.createTempDirectory(RAMDISK_PATH, TestJournalRotation.class.getSimpleName())
                        .toFile();
                rotationThresholdBytes = ROTATION_THRESHOLD_RAMDISK;
            }
            else {
                this.tmpJournalDir = Files.createTempDirectory(TestJournalRotation.class.getSimpleName()).toFile();
                rotationThresholdBytes = ROTATION_THRESHOLD_HDD;
            }
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
        this.journalRotMgr = new JournalRotationManager(ROTATOR_THREADCOUNT);
        journalRotMgr.start();
    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if deleting temporary data fails
     */
    @After
    public final void tearDown() throws InitializationError {
        journalRotMgr.stop();
        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir.toPath());
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tests {@link JournalRotationManager#submitRotation(WritableTxJournal)}'s failure on a stopped instance.
     */
    @Test(expected = IllegalStateException.class)
    public final void testRotationFailStoppedMgr() {
        LOGGER.info("Executing");
        journalRotMgr.stop();
        assertFalse(journalRotMgr.isStarted());
        journalRotMgr.submitRotation(new WritableTxJournal(tmpJournalDir, null, rotationThresholdBytes, journalRotMgr));
    }

    /**
     * Tests a single, automatically triggered rotation.
     * 
     * @throws IllegalStateException
     *             if the journal is not started, not part of this test
     * @throws IOException
     *             if writing to the journal fails, not part of this test
     * @throws InterruptedException
     *             if the test is interrupted while waiting, not part of this test
     */
    @Test
    public final void testSingleImplicitRotation() throws IllegalStateException, IOException, InterruptedException {
        LOGGER.info("Executing");

        final WritableTxJournal targetJrnl = new WritableTxJournal(tmpJournalDir, null, rotationThresholdBytes,
                journalRotMgr);
        assertNotNull(targetJrnl);
        targetJrnl.start();
        assertTrue(targetJrnl.isStarted());

        final String journalFilename = targetJrnl.getJournalFilename();

        final RotationSuccessCounter rotationCounter = new RotationSuccessCounter();

        final File journalFile = new File(journalFilename);
        assertTrue(journalFile.exists());

        journalRotMgr.addRotationEventListener(rotationCounter, journalFilename);

        // write just enough transactions to get over the rotation threshold
        long lengthBefore = journalFile.length();
        while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
            // write one complete transaction
            writeCompleteTransactions(targetJrnl, 1, null, PARTICIPANTS);
            lengthBefore = journalFile.length();
        }

        int retryCounter = 0;
        while (1 > rotationCounter.getCount() && retryCounter < ROTATION_WAIT_DELAY_RETRIES) {
            Thread.sleep(ROTATION_WAIT_DELAY_MS);
            if (1 > rotationCounter.getCount()) {
                // write two complete transactions
                writeCompleteTransactions(targetJrnl, 2, null, PARTICIPANTS);
            }
            retryCounter++;
        }

        final NavigableMap<Integer, File> backupFileList = JournalFileUtils.getInverseBackupMap(tmpJournalDir,
                journalFile.getName());
        assertFalse(backupFileList.isEmpty());
        assertTrue(backupFileList.firstKey().intValue() >= 1);
        assertTrue(1 <= rotationCounter.getCount());

        journalRotMgr.removeRotationEventListener(rotationCounter);
    }

    /**
     * Tests multiple ({@value #NB_TEST_ROTATIONS}), automatically triggered rotations.
     * 
     * @throws IllegalStateException
     *             if the journal is not started, not part of this test
     * @throws IOException
     *             if writing to the journal fails, not part of this test
     * @throws InterruptedException
     *             if the test is interrupted while waiting, not part of this test
     */
    @Test
    public final void testMultipleImplicitRotations() throws IllegalStateException, IOException, InterruptedException {
        LOGGER.info("Executing");

        final WritableTxJournal targetJrnl = new WritableTxJournal(tmpJournalDir, DEFAULT_JOURNAL_FILE_PREFIX,
                rotationThresholdBytes, journalRotMgr);
        assertNotNull(targetJrnl);
        targetJrnl.start();
        assertTrue(targetJrnl.isStarted());

        final RotationSuccessCounter rotationCounter = new RotationSuccessCounter();

        final String journalFilename = targetJrnl.getJournalFilename();

        journalRotMgr.addRotationEventListener(rotationCounter, journalFilename);

        final File journalFile = new File(journalFilename);
        assertTrue(journalFile.exists());

        writeCompleteTransactions(targetJrnl, 2, null, PARTICIPANTS);

        for (int j = 1; j <= NB_TEST_ROTATIONS; j++) {

            // write just enough transactions to get over the rotation threshold
            long lengthBefore = journalFile.length();
            while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
                // write two complete transactions
                writeCompleteTransactions(targetJrnl, 2, null, PARTICIPANTS);
                lengthBefore = journalFile.length();
            }

            int retryCounter = 0;
            while (j > rotationCounter.getCount() && retryCounter < ROTATION_WAIT_DELAY_RETRIES) {
                Thread.sleep(ROTATION_WAIT_DELAY_MS);
                if (j > rotationCounter.getCount()) {
                    // write two complete transactions
                    writeCompleteTransactions(targetJrnl, 2, null, PARTICIPANTS);
                }
                retryCounter++;
            }

            final NavigableMap<Integer, File> backupFileList = JournalFileUtils.getInverseBackupMap(tmpJournalDir,
                    journalFile.getName());
            assertFalse(backupFileList.isEmpty());
            LOGGER.debug("Checking for rotation count; highestBackup=" + backupFileList.firstKey().intValue()
                    + ", expected=" + j);
            assertTrue(backupFileList.firstKey().intValue() >= j);
            assertTrue(j <= rotationCounter.getCount());
            // verifying the non-existence of the i+1th backup file does not provide reliable results and is therefore
            // omitted pending a better solution
        }

        journalRotMgr.removeRotationEventListener(rotationCounter);
    }

    /**
     * Tests rotation operations performed by one {@link JournalRotationManager} with {@value #NB_CONCURRENT_JOURNALS}
     * threads simultaneously writing into their own journal.
     * 
     * @throws IllegalStateException
     *             if the journal is not started, not part of this test
     * @throws IOException
     *             if writing to any of the journals fails, not part of this test
     * @throws InterruptedException
     *             if the test is interrupted while waiting, not part of this test
     */
    @Test
    public final void testMultipleConcurrentImplicitRotations() throws IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final ExecutorService executor = Executors.newFixedThreadPool(NB_CONCURRENT_JOURNALS, new ThreadFactory() {

            private int serial;

            @Override
            public Thread newThread(final Runnable r) {
                serial++;
                final Thread result = new Thread(r);
                result.setDaemon(true);
                result.setName(TestJournalRotation.class.getSimpleName() + " - " + serial);
                return result;
            }
        });

        final ArrayList<WritableTxJournal> journalList = new ArrayList<WritableTxJournal>();
        final HashMap<String, AtomicInteger> rotationCounters = new HashMap<String, AtomicInteger>();

        final ArrayList<Callable<Void>> callableList = new ArrayList<Callable<Void>>();
        for (int i = 0; i < NB_CONCURRENT_JOURNALS; i++) {
            final WritableTxJournal targetJournal = new WritableTxJournal(tmpJournalDir, DEFAULT_JOURNAL_FILE_PREFIX
                    + i, rotationThresholdBytes, journalRotMgr);
            journalList.add(targetJournal);
            final String journalFilename = targetJournal.getJournalFilename();
            rotationCounters.put(journalFilename, new AtomicInteger());
            targetJournal.start();

            final JournalTestWriter journalWriter = new JournalTestWriter(targetJournal, rotationThresholdBytes,
                    rotationCounters.get(journalFilename), journalRotMgr);
            callableList.add(journalWriter);
        }

        final RotationListener rotListener = new RotationListener() {

            @Override
            public void rotationEventOccured(final RotationEvent rotevt) {
                if (RotationEvent.RotationStage.ROTATE_SUCCESS.equals(rotevt.getStage())) {
                    rotationCounters.get(rotevt.getFilename()).incrementAndGet();
                }
            }

        };
        journalRotMgr.addRotationEventListener(rotListener,
                rotationCounters.keySet().toArray(new String[NB_CONCURRENT_JOURNALS]));

        executor.invokeAll(callableList);

        // shutdown to try and complete running rotations
        journalRotMgr.stop();

        journalRotMgr.removeRotationEventListener(rotListener);

        for (final String currFile : rotationCounters.keySet()) {
            final AtomicInteger currCounter = rotationCounters.get(currFile);
            assertTrue(NB_TEST_ROTATIONS <= currCounter.get());
        }

        executor.shutdown();
    }

    /**
     * Autonomous task writing to a given {@link WritableTxJournal}.
     * 
     * 
     */
    private static final class JournalTestWriter implements Callable<Void> {
        private final WritableTxJournal targetJournal;
        private final long rotationThreshold;
        private final AtomicInteger rotationCounter;

        JournalTestWriter(final WritableTxJournal target, final long rotationThreshold,
                final AtomicInteger rotationCounter, final JournalRotationManager journalRotMgr) {
            this.targetJournal = target;
            this.rotationThreshold = rotationThreshold;
            this.rotationCounter = rotationCounter;
        }

        @Override
        public Void call() throws Exception {
            for (int j = 1; j <= NB_TEST_ROTATIONS; j++) {

                final File journalFile = new File(targetJournal.getJournalFilename());
                // write just enough transactions to get over the rotation threshold
                long lengthBefore = journalFile.length();
                while ((journalFile.length() < rotationThreshold) && (lengthBefore <= journalFile.length())) {
                    // write two complete transactions
                    writeCompleteTransactions(targetJournal, 2, null, PARTICIPANTS);
                    lengthBefore = journalFile.length();
                }

                // waits until the rotation counter increases
                int retryCounter = 0;
                while (j > rotationCounter.get() && retryCounter < ROTATION_WAIT_DELAY_RETRIES) {
                    Thread.sleep(ROTATION_WAIT_DELAY_MS);
                    if (j > rotationCounter.get()) {
                        // write two complete transactions to trigger missing rotations
                        writeCompleteTransactions(targetJournal, 2, null, PARTICIPANTS);
                    }
                    retryCounter++;
                }
            }
            return null;
        }
    }
}
