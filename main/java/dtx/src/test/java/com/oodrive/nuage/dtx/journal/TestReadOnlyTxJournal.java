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
import static com.oodrive.nuage.dtx.DtxConstants.JOURNAL_FILE_EXTENSION;
import static com.oodrive.nuage.dtx.DtxTestHelper.writeCompleteTransactions;
import static com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent.RotationStage.PRE_ROTATE;
import static com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent.RotationStage.ROTATE_FAILURE;
import static com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode.COMMIT;
import static com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode.ROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import com.oodrive.nuage.dtx.journal.TestJournalRotation.RotationSuccessCounter;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxNode;

/**
 * Tests for the {@link ReadOnlyTxJournal} class and methods.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class TestReadOnlyTxJournal {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestReadOnlyTxJournal.class);

    private static final Path RAMDISK_PATH = FileSystems.getDefault().getPath("/run/shm");

    private static final long ROTATION_THRESHOLD_RAMDISK = 2097152L;
    private static final long ROTATION_THRESHOLD_HDD = 6144L;

    private static final int NB_TEST_ROTATIONS = 5;

    private static final int ROTATOR_THREADCOUNT = 3;

    private static final int ROTATION_WAIT_DELAY_MS = 1000;
    private static final int EXCHANGE_TIMEOUT_S = 5;

    private static final int TASK_TIMEOUT_S = 20;
    private static final int EXECUTOR_SHUTDOWN_WAIT_S = 10;

    private static final int ROTATIONLESS_TX_NUMBER = 20;

    private static final Set<TxNode> PARTICIPANTS = DtxTestHelper.newRandomParticipantsSet();

    private Path tmpJournalDir;

    private long rotationThresholdBytes;

    private JournalRotationManager journalRotMgr;

    private WritableTxJournal writableJournal;

    private Path unreadableFile;

    private Path missingFile;

    /**
     * Sets up common fixture. TODO: refactor to share with other tests.
     * 
     * @throws InitializationError
     *             if setting up temporary files fails
     */
    @Before
    public final void setUp() throws InitializationError {

        // redirect temporary file directory to ramdisk, if possible
        // TODO: this should be factorized as a custom runner or abstract superclass, but those solutions don't work
        // with maven and/or coverage tools
        try {
            if (Files.exists(RAMDISK_PATH)) {
                this.tmpJournalDir = Files.createTempDirectory(RAMDISK_PATH, TestJournalRotation.class.getSimpleName());
                rotationThresholdBytes = ROTATION_THRESHOLD_RAMDISK;
            }
            else {
                this.tmpJournalDir = Files.createTempDirectory(TestJournalRotation.class.getSimpleName());
                rotationThresholdBytes = ROTATION_THRESHOLD_HDD;
            }
            this.journalRotMgr = new JournalRotationManager(ROTATOR_THREADCOUNT);
            journalRotMgr.start();
            this.writableJournal = new WritableTxJournal(tmpJournalDir.toFile(),
                    TestReadOnlyTxJournal.class.getSimpleName(), rotationThresholdBytes, journalRotMgr);
            this.writableJournal.start();

            this.unreadableFile = Files.createTempFile(tmpJournalDir, DEFAULT_JOURNAL_FILE_PREFIX,
                    JOURNAL_FILE_EXTENSION,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("---------")));

            this.missingFile = Files.createTempFile(tmpJournalDir, DEFAULT_JOURNAL_FILE_PREFIX, JOURNAL_FILE_EXTENSION);
            Files.delete(missingFile);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if deleting temporary data fails
     */
    @After
    public final void tearDown() throws InitializationError {
        try {
            writableJournal.stop();
            journalRotMgr.stop();
            Files.setPosixFilePermissions(unreadableFile, PosixFilePermissions.fromString("rwxr-x---"));
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpJournalDir);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
    }

    /**
     * Tests failure of the construction due to a missing journal file.
     * 
     * @throws IllegalArgumentException
     *             if the journal file cannot be found, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateReadOnlyJournalFailFileNotFound() throws IllegalArgumentException,
            IllegalStateException, IOException {
        LOGGER.info("Executing");

        new ReadOnlyTxJournal(this.missingFile.toFile(), journalRotMgr);
    }

    /**
     * Tests failure of the construction due to an unreadable journal file.
     * 
     * @throws IllegalArgumentException
     *             if the journal file cannot be read, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateReadOnlyJournalFailFileNotReadable() throws IllegalArgumentException,
            IllegalStateException, IOException {
        LOGGER.info("Executing");

        new ReadOnlyTxJournal(this.unreadableFile.toFile(), journalRotMgr);
    }

    /**
     * Tests reading a transaction journal, trying to remove each element and verifying all attempts fail.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     */
    @Test
    public final void testIteratorRemoveFailure() throws IllegalStateException, IOException {
        LOGGER.info("Executing");

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(new File(writableJournal.getJournalFilename()),
                journalRotMgr);
        long currentTxId = DtxTestHelper.nextTxId() + 1;
        final long lastTxId = writeCompleteTransactions(writableJournal, ROTATIONLESS_TX_NUMBER, null, PARTICIPANTS);

        int exceptionCounter = 0;
        for (final Iterator<JournalRecord> jrnlIter = targetRoJournal.iterator(); jrnlIter.hasNext();) {
            final JournalRecord currRecord = jrnlIter.next();
            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            assertEquals(currentTxId, currEntry.getTxId());
            final TxOpCode currOp = currEntry.getOp();
            if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                currentTxId++;
            }
            try {
                jrnlIter.remove();
            }
            catch (final UnsupportedOperationException e) {
                exceptionCounter++;
            }
        }
        assertEquals(lastTxId + 1, currentTxId);
        assertEquals(ROTATIONLESS_TX_NUMBER * 2, exceptionCounter);
    }

    /**
     * Tests failure to read a transaction journal with invalid data.
     * 
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws NoSuchElementException
     *             if trying to iterate over invalid journal data fails, expected for this test
     * 
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorFailBadContent() throws IOException, NoSuchElementException {
        LOGGER.info("Executing");

        final File journalFile = new File(writableJournal.getJournalFilename());

        // write bad data
        try (FileOutputStream output = new FileOutputStream(journalFile)) {
            output.write(DEFAULT_JOURNAL_FILE_PREFIX.getBytes());
        }

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(journalFile, journalRotMgr);

        targetRoJournal.iterator().next();
    }

    /**
     * Tests failure to read a transaction journal that has been deleted.
     * 
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws NoSuchElementException
     *             if trying to iterate over a deleted journal file fails, expected for this test
     * 
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorFailFileDeleted() throws IOException, NoSuchElementException {
        LOGGER.info("Executing");

        final File journalFile = Files.createTempFile(tmpJournalDir, DEFAULT_JOURNAL_FILE_PREFIX,
                JOURNAL_FILE_EXTENSION).toFile();

        writeCompleteTransactions(writableJournal, ROTATIONLESS_TX_NUMBER, null, PARTICIPANTS);

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(journalFile, journalRotMgr);

        final Iterator<JournalRecord> jrnlIter = targetRoJournal.iterator();

        assertTrue(journalFile.delete());

        jrnlIter.next();

    }

    /**
     * Tests failure to iterate on a transaction journal beyond its limit.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws NoSuchElementException
     *             if trying to iterate beyond the limit of a journal file fails, expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorFailBeyondLimit() throws IllegalStateException, IOException, NoSuchElementException {
        LOGGER.info("Executing");

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(new File(writableJournal.getJournalFilename()),
                journalRotMgr);
        long currentTxId = DtxTestHelper.nextTxId() + 1;
        final long lastTxId = writeCompleteTransactions(writableJournal, ROTATIONLESS_TX_NUMBER, null, PARTICIPANTS);

        final Iterator<JournalRecord> jrnlIter = targetRoJournal.iterator();
        for (; jrnlIter.hasNext();) {
            final JournalRecord currRecord = jrnlIter.next();
            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            assertEquals(currentTxId, currEntry.getTxId());
            final TxOpCode currOp = currEntry.getOp();
            if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                currentTxId++;
            }
        }
        assertEquals(lastTxId + 1, currentTxId);

        jrnlIter.next();
    }

    /**
     * Tests reading a transaction journal that hasn't been rotated through a {@link ReadOnlyTxJournal}.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     */
    @Test
    public final void testReadingWithoutRotation() throws IllegalStateException, IOException {
        LOGGER.info("Executing");

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(new File(writableJournal.getJournalFilename()),
                journalRotMgr);
        long currentTxId = DtxTestHelper.nextTxId() + 1;
        final long lastTxId = writeCompleteTransactions(writableJournal, ROTATIONLESS_TX_NUMBER, null, PARTICIPANTS);

        for (final JournalRecord currRecord : targetRoJournal) {
            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            assertEquals(currentTxId, currEntry.getTxId());
            final TxOpCode currOp = currEntry.getOp();
            if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                currentTxId++;
            }
        }
        assertEquals(lastTxId + 1, currentTxId);
    }

    /**
     * Tests reading a transaction journal while simulating a failed rotation between reading records.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws InterruptedException
     *             if rotation event handling is interrupted, not part of this test
     */
    @Test
    public final void testReadingWithSimulatedFailedRotations() throws IllegalStateException, IOException,
            InterruptedException {
        LOGGER.info("Executing");

        final String journalFilename = writableJournal.getJournalFilename();
        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(new File(journalFilename), journalRotMgr);
        long currentTxId = DtxTestHelper.nextTxId() + 1;
        final long lastTxId = writeCompleteTransactions(writableJournal, ROTATIONLESS_TX_NUMBER, null, PARTICIPANTS);

        final Iterator<JournalRecord> jrnlIter = targetRoJournal.iterator();
        final RotationListener jrnlRotListener = (RotationListener) jrnlIter;
        for (; jrnlIter.hasNext();) {
            final JournalRecord currRecord = jrnlIter.next();
            jrnlRotListener.rotationEventOccured(new RotationEvent(journalFilename, PRE_ROTATE));
            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            assertEquals(currentTxId, currEntry.getTxId());
            final TxOpCode currOp = currEntry.getOp();
            if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                currentTxId++;
            }
            jrnlRotListener.rotationEventOccured(new RotationEvent(journalFilename, ROTATE_FAILURE));
        }
        assertEquals(lastTxId + 1, currentTxId);
    }

    /**
     * Tests reading a complete journal after it has been rotated once.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws InterruptedException
     *             if the thread is interrupted while waiting, not part of this test
     */
    @Test
    public final void testReadingAfterRotation() throws IllegalStateException, IOException, InterruptedException {
        LOGGER.info("Executing");

        final String journalFilename = writableJournal.getJournalFilename();
        final File journalFile = new File(journalFilename);
        assertTrue(journalFile.exists());

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(journalFile, journalRotMgr);
        long currentTxId = DtxTestHelper.nextTxId() + 1;

        assertTrue(journalFile.exists());
        final RotationSuccessCounter rotationCounter = new RotationSuccessCounter();

        journalRotMgr.addRotationEventListener(rotationCounter, journalFilename);

        long lastTxId = currentTxId;

        // write just enough transactions to get over the rotation threshold
        long lengthBefore = journalFile.length();
        while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
            // write one complete transaction
            lastTxId = writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
            lengthBefore = journalFile.length();
        }

        while (1 > rotationCounter.getCount()) {
            Thread.sleep(ROTATION_WAIT_DELAY_MS);
            if (1 > rotationCounter.getCount()) {
                // write two complete transactions
                lastTxId = writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
            }
        }

        journalRotMgr.removeRotationEventListener(rotationCounter);

        for (final JournalRecord currRecord : targetRoJournal) {
            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
            assertEquals(currentTxId, currEntry.getTxId());
            final TxOpCode currOp = currEntry.getOp();
            if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                currentTxId++;
            }
        }
        assertEquals(lastTxId + 1, currentTxId);

    }

    /**
     * Tests failure to read a journal after it has been rotated once.
     * 
     * @throws IllegalStateException
     *             if the journal cannot be written to, not part of this test
     * @throws IOException
     *             if writing or parsing the contents of the journal fail, not part of this test
     * @throws InterruptedException
     *             if the thread is interrupted while waiting, not part of this test
     * @throws NoSuchElementException
     *             if trying to iterate after backup files have been deleted fails, expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testReadingAfterRotationFailureMissingFiles() throws IllegalStateException, IOException,
            InterruptedException, NoSuchElementException {
        LOGGER.info("Executing");

        final String journalFilename = writableJournal.getJournalFilename();
        final File journalFile = new File(journalFilename);
        assertTrue(journalFile.exists());

        final ReadOnlyTxJournal targetRoJournal = new ReadOnlyTxJournal(journalFile, journalRotMgr);

        assertTrue(journalFile.exists());
        final RotationSuccessCounter rotationCounter = new RotationSuccessCounter();

        journalRotMgr.addRotationEventListener(rotationCounter, journalFilename);

        // write just enough transactions to get over the rotation threshold
        long lengthBefore = journalFile.length();
        while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
            // write one complete transaction
            writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
            lengthBefore = journalFile.length();
        }

        while (1 > rotationCounter.getCount()) {
            Thread.sleep(ROTATION_WAIT_DELAY_MS);
            if (1 > rotationCounter.getCount()) {
                // write two complete transactions
                writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
            }
        }

        journalRotMgr.removeRotationEventListener(rotationCounter);

        // construct the iterator
        final Iterator<JournalRecord> jrnlIter = targetRoJournal.iterator();

        final NavigableMap<Integer, File> backupMap = JournalFileUtils.getInverseBackupMap(tmpJournalDir.toFile(),
                FileSystems.getDefault().getPath(writableJournal.getJournalFilename()).getFileName().toString());
        for (final File currBackupFile : backupMap.values()) {
            assertTrue(currBackupFile.delete());
        }

        // should throw an exception as the underlying journal has been compromised
        jrnlIter.next();
    }

    /**
     * Tests reading a journal while it is being written to and consequently rotated.
     * 
     * This tests reading by a reader thread that initially waits for a single writer to complete writing and rotating
     * up to a target, but continues reading during the next of the writer's iterations.
     * 
     * @throws InterruptedException
     *             if any of the threads are interrupted, not part of this test
     * @throws ExecutionException
     *             if any exception is thrown by either reader or writer threads, not part of this test
     * @throws TimeoutException
     *             if either reader or writer thread times out, not part of this test
     */
    @Test
    public final void testReadingWhileRotating() throws InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Executing");

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final String journalFilename = writableJournal.getJournalFilename();
        final File journalFile = new File(journalFilename);

        final ReadOnlyTxJournal targetRoJournal = writableJournal.newReadOnlyTxJournal();
        final long currentTxId = DtxTestHelper.nextTxId() + 1;

        assertTrue(journalFile.exists());
        final RotationSuccessCounter rotationCounter = new RotationSuccessCounter();

        journalRotMgr.addRotationEventListener(rotationCounter, journalFilename);

        final Exchanger<Long> lastTxIdExch = new java.util.concurrent.Exchanger<Long>();

        final Callable<Long> writer = new Callable<Long>() {

            @Override
            public Long call() throws Exception {

                long result = currentTxId;
                Long lastReadId = Long.valueOf(result);

                for (int i = 1; i <= NB_TEST_ROTATIONS; i++) {
                    // write just enough transactions to get over the rotation threshold
                    long lengthBefore = journalFile.length();
                    while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
                        // write two complete transactions
                        result = writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
                        lengthBefore = journalFile.length();
                    }

                    while (i > rotationCounter.getCount()) {
                        Thread.sleep(ROTATION_WAIT_DELAY_MS);
                        if (i > rotationCounter.getCount()) {
                            // write two complete transactions
                            result = writeCompleteTransactions(writableJournal, 2, null, PARTICIPANTS);
                        }
                    }

                    final Long targetId = Long.valueOf(result);
                    LOGGER.debug("Sending new target ID: " + targetId);
                    try {
                        final Long lastFromReader = lastTxIdExch.exchange(targetId, EXCHANGE_TIMEOUT_S,
                                TimeUnit.SECONDS);
                        assertTrue("Reader has read beyond last transmitted value; ID read before last rotation="
                                + lastReadId + ", last read ID=" + lastFromReader,
                                (lastFromReader.compareTo(lastReadId) >= 0));
                        lastReadId = lastFromReader;
                        LOGGER.debug("Writer received last read txID: " + lastReadId);
                    }
                    catch (final TimeoutException te) {
                        LOGGER.warn("Timed out waiting for reader thread");
                        break;
                    }
                }
                return Long.valueOf(result);
            }
        };

        final Callable<Long> reader = new Callable<Long>() {

            @Override
            public Long call() throws Exception {

                long result = currentTxId;
                Long targetTxId = Long.valueOf(result);

                for (int i = 1; i <= NB_TEST_ROTATIONS; i++) {
                    LOGGER.debug("Reader waiting for new target txID");
                    targetTxId = lastTxIdExch.exchange(targetTxId, EXCHANGE_TIMEOUT_S, TimeUnit.SECONDS);
                    LOGGER.debug("Reader received target txID: " + targetTxId);

                    for (final JournalRecord currRecord : targetRoJournal) {
                        final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
                        final long readTxId = currEntry.getTxId();
                        if (readTxId < result) {
                            continue;
                        }
                        assertTrue(result <= readTxId);
                        final TxOpCode currOp = currEntry.getOp();
                        if (COMMIT.equals(currOp) || ROLLBACK.equals(currOp)) {
                            result = currEntry.getTxId();
                        }
                    }
                    assertTrue("Read transaction value is greater or equal than target; target=" + targetTxId
                            + ", read=" + result, targetTxId.compareTo(Long.valueOf(result)) <= 0);
                }
                return Long.valueOf(result);
            }
        };

        final Future<Long> readerFut = executor.submit(reader);

        final Future<Long> writerFut = executor.submit(writer);

        final Long lastReadTxId = readerFut.get(EXCHANGE_TIMEOUT_S * NB_TEST_ROTATIONS, TimeUnit.SECONDS);
        LOGGER.debug("Reader returned; lastReadTxId=" + lastReadTxId);
        final Long lastWrittenTxId = writerFut.get(EXCHANGE_TIMEOUT_S * NB_TEST_ROTATIONS, TimeUnit.SECONDS);
        LOGGER.debug("Writer returned; lastWrittenTxId=" + lastWrittenTxId);

        journalRotMgr.removeRotationEventListener(rotationCounter);

        assertEquals(lastReadTxId, lastWrittenTxId);

        executor.shutdown();
        executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_S, TimeUnit.SECONDS);
    }

    /**
     * Tests continuous reading of journals by separate threads while they are being written to and rotated.
     * 
     * @throws IllegalStateException
     *             if a journal start fails, not part of this test
     * @throws IOException
     *             if disk I/O fails, not part of this test
     * @throws InterruptedException
     *             if executor shutdown fails, not part of this test
     * @throws ExecutionException
     *             if a reader thread fails, not part of this test
     * @throws TimeoutException
     *             if a reader thread times out, not part of this test
     */
    @Test
    public final void testMultipleJournalWritesAndReads() throws IllegalStateException, IOException,
            InterruptedException, ExecutionException, TimeoutException {
        LOGGER.info("Executing");

        final ExecutorService executor = Executors.newFixedThreadPool(ROTATOR_THREADCOUNT);

        final ArrayList<WritableTxJournal> journalList = new ArrayList<WritableTxJournal>(ROTATOR_THREADCOUNT);
        final HashMap<String, Future<Long>> readTaskList = new HashMap<String, Future<Long>>(ROTATOR_THREADCOUNT);

        final CountDownLatch writeDoneLatch = new CountDownLatch(1);

        final HashMap<String, AtomicInteger> rotationCounters = new HashMap<String, AtomicInteger>();

        for (int i = 0; i < ROTATOR_THREADCOUNT; i++) {
            final WritableTxJournal newJournal;
            if (i == 0) {
                newJournal = writableJournal;
            }
            else {
                newJournal = new WritableTxJournal(tmpJournalDir.toFile(), TestReadOnlyTxJournal.class.getSimpleName()
                        + "-" + i, rotationThresholdBytes, journalRotMgr);
                newJournal.start();
            }

            journalList.add(newJournal);
            rotationCounters.put(newJournal.getJournalFilename(), new AtomicInteger());
            readTaskList.put(newJournal.getJournalFilename(), executor.submit(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    long result = 0;
                    while (writeDoneLatch.getCount() > 0) {
                        for (final JournalRecord currRecord : newJournal.newReadOnlyTxJournal()) {
                            final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
                            result = currEntry.getTxId();
                        }
                    }
                    return Long.valueOf(result);
                }
            }));
        }

        journalRotMgr.addRotationEventListener(new RotationListener() {

            @Override
            public void rotationEventOccured(final RotationEvent rotevt) throws InterruptedException {
                if (RotationEvent.RotationStage.ROTATE_SUCCESS.equals(rotevt.getStage())) {
                    rotationCounters.get(rotevt.getFilename()).incrementAndGet();
                }
            }
        }, rotationCounters.keySet().toArray(new String[ROTATOR_THREADCOUNT]));

        for (int i = 0; i < NB_TEST_ROTATIONS; i++) {
            for (final WritableTxJournal currJournal : journalList) {
                final String journalFilename = currJournal.getJournalFilename();
                final File journalFile = new File(journalFilename);
                // write just enough transactions to get over the rotation threshold
                long lengthBefore = journalFile.length();
                while ((journalFile.length() < rotationThresholdBytes) && (lengthBefore <= journalFile.length())) {
                    // write two complete transactions
                    writeCompleteTransactions(currJournal, 2, null, PARTICIPANTS);
                    lengthBefore = journalFile.length();
                }

                final AtomicInteger rotationCounter = rotationCounters.get(journalFilename);

                while (i > rotationCounter.get()) {
                    Thread.sleep(ROTATION_WAIT_DELAY_MS);
                    if (i > rotationCounter.get()) {
                        // write two complete transactions
                        writeCompleteTransactions(currJournal, 2, null, PARTICIPANTS);
                    }
                }
            }
        }

        writeDoneLatch.countDown();

        for (final WritableTxJournal currJournal : journalList) {
            assertEquals(Long.valueOf(currJournal.getLastFinishedTxId()),
                    readTaskList.get(currJournal.getJournalFilename()).get(TASK_TIMEOUT_S, TimeUnit.SECONDS));
            currJournal.stop();
        }

        executor.shutdown();
        executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_S, TimeUnit.SECONDS);

    }

}
