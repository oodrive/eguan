package io.eguan.dtx.journal;

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

import static io.eguan.dtx.DtxConstants.DEFAULT_JOURNAL_FILE_PREFIX;
import static io.eguan.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static io.eguan.dtx.DtxConstants.JOURNAL_FILE_EXTENSION;
import static io.eguan.dtx.DtxTestHelper.DEFAULT_TX_MESSAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxTestHelper;
import io.eguan.dtx.TestTransactionManagerErrorCases;
import io.eguan.dtx.TestTransactionManagerErrorCases.TestOp;
import io.eguan.dtx.journal.JournalRecord;
import io.eguan.dtx.journal.JournalRotationManager;
import io.eguan.dtx.journal.WritableTxJournal;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.dtx.DistTxWrapper;
import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry;
import io.eguan.proto.dtx.DistTxWrapper.TxMessage;
import io.eguan.proto.dtx.DistTxWrapper.TxNode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

/**
 * Tests for the {@link WritableTxJournal} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class TestWritableTxJournal {

    private static final int TEST_THREAD_COUNT = 10;
    private static final int TEST_WRITES_PER_THREAD = 5;
    private static final int TEST_READS_PER_THREAD = 7;
    private static final int TEST_NB_OF_TEST_ENTRIES = 30;
    private static final int TEST_NB_OF_START_CYCLES = 6;
    private static final long TEST_ROTATION_THRESHOLD = 3221225472L; // keep this high to avoid rotation

    private static final Set<TxNode> PARTICIPANTS = DtxTestHelper.newRandomParticipantsSet();

    private static class JournalTestWriter implements Callable<Void> {

        private final WritableTxJournal target;
        private final TestOp op;
        private final int rep;

        JournalTestWriter(final WritableTxJournal target, final TestTransactionManagerErrorCases.TestOp operation,
                final int nbOfRepetitions) {
            this.target = target;
            this.op = operation;
            this.rep = nbOfRepetitions;
        }

        @Override
        public Void call() {
            final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).build();
            final Random rnd = new Random(System.currentTimeMillis());
            for (int i = 0; i < rep; i++) {
                try {
                    switch (op) {
                    case START:
                        target.writeStart(defTx, PARTICIPANTS);
                        break;
                    case COMMIT:
                        target.writeCommit(rnd.nextLong(), PARTICIPANTS);
                        break;
                    case ROLLBACK:
                        target.writeRollback(rnd.nextLong(), -1, PARTICIPANTS);
                        break;
                    default:
                        break;
                    }
                }
                catch (IllegalStateException | IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    private static class JournalTestReader implements Callable<Void> {

        private final WritableTxJournal target;
        private final int rep;

        JournalTestReader(final WritableTxJournal target, final int nbOfRepetitions) {
            this.target = target;
            this.rep = nbOfRepetitions;
        }

        @Override
        public Void call() throws Exception {
            for (int i = 0; i < rep; i++) {
                for (final JournalRecord currRecord : target) {
                    TxJournalEntry.parseFrom(currRecord.getEntry());
                }
            }
            return null;
        }

    }

    private Path tmpFileDir;

    private Path roFileDir;
    private JournalRotationManager journalRotMgr;
    private WritableTxJournal target;

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if creating some temporary directory fails
     */
    @Before
    public final void setUp() throws InitializationError {
        try {
            final String tmpPrefix = TestWritableTxJournal.class.getSimpleName();
            tmpFileDir = Files.createTempDirectory(tmpPrefix);
            roFileDir = Files.createTempDirectory(tmpPrefix,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("--x------")));
            assertFalse(Files.isReadable(roFileDir));
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        // this instance remains stopped on purpose so tests will fail if any rotation is submitted during writes
        journalRotMgr = new JournalRotationManager(0);

        this.target = new WritableTxJournal(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX, TEST_ROTATION_THRESHOLD,
                journalRotMgr);

    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if deleting some of the temporary files and directories fails
     */
    @After
    public final void tearDown() throws InitializationError {

        final ArrayList<Throwable> exceptionList = new ArrayList<Throwable>();
        if (target.isStarted()) {
            try {
                target.stop();
            }
            catch (final IOException e) {
                exceptionList.add(new InitializationError(e));
            }
        }
        try {
            io.eguan.utils.Files.deleteRecursive(tmpFileDir);
        }
        catch (final IOException e) {
            exceptionList.add(new InitializationError(e));
        }
        try {
            Files.setPosixFilePermissions(roFileDir, PosixFilePermissions.fromString("rwx------"));
            io.eguan.utils.Files.deleteRecursive(roFileDir);
        }
        catch (final IOException e) {
            exceptionList.add(new InitializationError(e));
        }
        if (!exceptionList.isEmpty()) {
            throw new InitializationError(exceptionList);
        }
    }

    /**
     * Tests creating and starting a journal with and without an explicit filename prefix.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     */
    @Test
    public final void testStartFileCreation() throws IOException {
        final File parentDir = tmpFileDir.toFile();
        final WritableTxJournal noPrefixJournal = new WritableTxJournal(parentDir, null, 0, journalRotMgr);
        noPrefixJournal.start();
        assertTrue(new File(parentDir, DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION).exists());

        noPrefixJournal.stop();
        assertFalse(noPrefixJournal.isStarted());

        final WritableTxJournal prefixedJournal = new WritableTxJournal(parentDir, DEFAULT_JOURNAL_FILE_PREFIX, 0,
                journalRotMgr);
        prefixedJournal.start();
        assertTrue(new File(parentDir, DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION).exists());

        prefixedJournal.stop();
        assertFalse(prefixedJournal.isStarted());
    }

    /**
     * Tests (repeatedly) starting and stopping a given journal.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     */
    @Test
    public final void testStartStopJournal() throws IOException {
        target.start();
        assertTrue(new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION).exists());
        // tests idempotence of start()
        target.start();
        assertTrue(target.isStarted());

        target.stop();
        assertFalse(target.isStarted());
        // tests idempotence of stop()
        target.stop();
        assertFalse(target.isStarted());

        // starts once more
        target.start();
        assertTrue(target.isStarted());

        // and finally stops
        target.stop();
        assertFalse(target.isStarted());
    }

    /**
     * Tests (repeatedly) starting and stopping a given journal, while checking written and recovered transaction IDs.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     */
    @Test
    public final void testStartStopJournalRecovery() throws IOException {
        long lastTxId = DEFAULT_LAST_TX_VALUE;
        for (int i = 0; i < TEST_NB_OF_START_CYCLES; i++) {
            target.start();
            // checks after (re)start
            assertEquals(lastTxId, target.getLastFinishedTxId());

            lastTxId = DtxTestHelper.writeCompleteTransactions(target, TEST_NB_OF_TEST_ENTRIES, null, PARTICIPANTS);
            // checks directly after write
            assertEquals(lastTxId, target.getLastFinishedTxId());

            final long nextTxId = DtxTestHelper.nextTxId();
            assertTrue(lastTxId < nextTxId);

            target.writeStart(TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(nextTxId).build(), PARTICIPANTS);
            // checks after partial transaction write
            assertEquals(lastTxId, target.getLastFinishedTxId());

            target.writeCommit(nextTxId, PARTICIPANTS);
            // checks after completing partial transaction write
            assertEquals(nextTxId, target.getLastFinishedTxId());

            lastTxId = nextTxId;
            target.stop();
        }
    }

    /**
     * Tests failure of the {@link WritableTxJournal#start()} method due to a missing target directory.
     * 
     * @throws IllegalArgumentException
     *             if construction fails, not part of this test
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     * @throws IllegalStateException
     *             expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testStartJournalFailNoDirectory() throws IllegalArgumentException, IOException,
            IllegalStateException {
        final Path vanishingDir = Files.createTempDirectory(TestWritableTxJournal.class.getSimpleName());
        Files.deleteIfExists(vanishingDir);
        assertFalse(Files.exists(vanishingDir));

        final WritableTxJournal target = new WritableTxJournal(vanishingDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX, 0,
                journalRotMgr);
        target.start();
    }

    /**
     * Tests failure of the {@link WritableTxJournal#start()} method due to a read-only target directory.
     * 
     * @throws IllegalArgumentException
     *             if construction fails, not part of this test
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     * @throws IllegalStateException
     *             expected for this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testStartJournalFailDirectoryNotWritable() throws IllegalArgumentException, IOException {

        final WritableTxJournal target = new WritableTxJournal(roFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX, 0,
                journalRotMgr);
        target.start();
    }

    /**
     * Tests failure of the {@link WritableTxJournal#writeStart(TxMessage)} method due to the journal not being started.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws IOException
     *             not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testWriteStartFailNotStarted() throws IllegalStateException, IOException {
        assertFalse(target.isStarted());
        final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).build();

        target.writeStart(defTx, PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link WritableTxJournal#writeCommit(long)} method due to the journal not being started.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws IOException
     *             not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testWriteCommitFailNotStarted() throws IllegalStateException, IOException {
        assertFalse(target.isStarted());

        target.writeCommit(DtxTestHelper.nextTxId(), PARTICIPANTS);
    }

    /**
     * Tests failure of the {@link WritableTxJournal#writeRollback(long, int)} method due to the journal not being
     * started.
     * 
     * @throws IllegalStateException
     *             expected for this test
     * @throws IOException
     *             not part of this test
     */
    @Test(expected = IllegalStateException.class)
    public final void testWriteRollbackFailNotStarted() throws IllegalStateException, IOException {
        assertFalse(target.isStarted());

        target.writeRollback(DtxTestHelper.nextTxId(), -1, PARTICIPANTS);
    }

    /**
     * Tests the {@link Iterator} provided by {@link WritableTxJournal#iterator()}, especially its failure when reaching
     * the end of the file.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * @throws IOException
     *             not part of this test
     * @throws NoSuchElementException
     *             expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorFailAtEnd() throws IllegalStateException, IOException, NoSuchElementException {

        target.start();
        final File journalFile = new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES; i++) {
            final long txId = DtxTestHelper.nextTxId();
            final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).build();

            target.writeStart(defTx, PARTICIPANTS);

            // writes commits and rollbacks for every other transaction
            if (i % 2 == 0) {
                target.writeRollback(txId, -1, PARTICIPANTS);
            }
            else {
                target.writeCommit(txId, PARTICIPANTS);
            }
        }

        final Iterator<JournalRecord> journalIter = target.iterator();
        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES * 2; i++) {
            assertTrue(journalIter.hasNext());
            assertNotNull(journalIter.next());
        }
        assertFalse(journalIter.hasNext());
        journalIter.next();
    }

    /**
     * Tests the {@link Iterator} provided by {@link WritableTxJournal#iterator()}, in particular its failure upon
     * calling {@link Iterator#remove()}.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * @throws IOException
     *             not part of this test
     * @throws UnsupportedOperationException
     *             expected for this test
     */
    @Test(expected = UnsupportedOperationException.class)
    public final void testIteratorFailOnRemove() throws IllegalStateException, IOException,
            UnsupportedOperationException {
        final File parentDir = tmpFileDir.toFile();
        target.start();
        final File journalFile = new File(parentDir, DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        final Iterator<JournalRecord> journalIter = target.iterator();
        journalIter.remove();
    }

    /**
     * Tests the {@link Iterator} provided by {@link WritableTxJournal#iterator()}, especially its correct behavior upon
     * stopping the underlying journal instance.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * @throws IOException
     *             not part of this test
     * @throws NoSuchElementException
     *             expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorEndIterationUponStop() throws IllegalStateException, IOException {

        target.start();
        final File journalFile = new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES; i++) {
            // only write commits to save time
            target.writeCommit(DtxTestHelper.nextTxId(), PARTICIPANTS);
        }

        final Iterator<JournalRecord> journalIter = target.iterator();
        // iterate up to half of the entries
        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES / 2; i++) {
            assertTrue(journalIter.hasNext());
            assertNotNull(journalIter.next());
        }
        target.stop();
        // checks for the last read element
        assertTrue(journalIter.hasNext());
        assertNotNull(journalIter.next());

        // no further elements should be available
        assertFalse(journalIter.hasNext());
        journalIter.next();
    }

    /**
     * Tests the {@link Iterator} provided by {@link WritableTxJournal#iterator()} on a stopped journal.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * @throws IOException
     *             not part of this test
     * @throws NoSuchElementException
     *             expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorNoIterationOnStopped() throws IllegalStateException, IOException {

        target.start();
        final File journalFile = new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES; i++) {
            // only write commits to save time
            target.writeCommit(DtxTestHelper.nextTxId(), PARTICIPANTS);
        }

        target.stop();

        final Iterator<JournalRecord> journalIter = target.iterator();
        // no elements should be available
        assertFalse(journalIter.hasNext());
        journalIter.next();
    }

    /**
     * Tests the {@link Iterator} provided by {@link WritableTxJournal#iterator()} on a deleted journal file.
     * 
     * @throws IllegalStateException
     *             not part of this test
     * @throws IOException
     *             not part of this test
     * @throws NoSuchElementException
     *             expected for this test
     */
    @Test(expected = NoSuchElementException.class)
    public final void testIteratorNoIterationOnDeleted() throws IllegalStateException, IOException {
        target.start();
        final File journalFile = new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES; i++) {
            // only write commits to save time
            target.writeCommit(DtxTestHelper.nextTxId(), PARTICIPANTS);
        }

        assertTrue(Files.deleteIfExists(journalFile.toPath()));

        final Iterator<JournalRecord> journalIter = target.iterator();
        // no elements should be available
        assertFalse(journalIter.hasNext());
        journalIter.next();
    }

    /**
     * Tests writing and reading back journal entries sequentially.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     */
    @Test
    public final void testSingleWritesAndReads() throws IOException {
        target.start();
        final File journalFile = new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION);
        assertTrue(journalFile.exists());

        for (int i = 0; i < TEST_NB_OF_TEST_ENTRIES; i++) {
            final long txId = DtxTestHelper.nextTxId();
            final TxMessage defTx = TxMessage.newBuilder(DEFAULT_TX_MESSAGE).setVersion(ProtocolVersion.VERSION_1).setTxId(txId).build();

            target.writeStart(defTx, PARTICIPANTS);

            // writes commits and rollbacks for every other transaction
            if (i % 2 == 0) {
                target.writeRollback(txId, -1, PARTICIPANTS);
            }
            else {
                target.writeCommit(txId, PARTICIPANTS);
            }

            assertTrue(journalFile.length() > i);
            for (final Iterator<JournalRecord> iter = target.iterator(); iter.hasNext();) {
                final JournalRecord currRecord = iter.next();
                final TxJournalEntry currEntry = DistTxWrapper.TxJournalEntry.parseFrom(currRecord.getEntry());
                assertEquals(PARTICIPANTS.size(), currEntry.getTxNodesList().size());
                switch (currEntry.getOp()) {
                case START:
                    final TxMessage currTransaction = currEntry.getTx();
                    assertTrue(currTransaction.getTxId() <= txId);
                    break;
                case COMMIT:
                    assertTrue(currEntry.getTxId() <= txId);
                    break;
                case ROLLBACK:
                    assertTrue(currEntry.getTxId() <= txId);
                    break;
                default:
                    // nothing
                }

                if (!iter.hasNext()) {
                    assertEquals(currEntry.getTxId(), defTx.getTxId());
                }
            }
        }
        target.stop();
    }

    /**
     * Tests concurrent write accesses to a single journal instance.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     * @throws InterruptedException
     *             if one of the thread is interrupted, not part of this test
     */
    @Test
    public final void testConcurrentWrites() throws IOException, InterruptedException {

        target.start();
        assertTrue(new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION).exists());

        final ExecutorService forkPool = Executors.newFixedThreadPool(TEST_THREAD_COUNT);
        final ArrayList<Callable<Void>> runList = new ArrayList<Callable<Void>>();
        int writesCounter = 0;
        for (int i = 0; i < TEST_THREAD_COUNT; i++) {
            runList.add(new JournalTestWriter(target, TestOp.START, TEST_WRITES_PER_THREAD));
            writesCounter++;
            runList.add(new JournalTestWriter(target, TestOp.COMMIT, TEST_WRITES_PER_THREAD));
            writesCounter++;
            runList.add(new JournalTestWriter(target, TestOp.ROLLBACK, TEST_WRITES_PER_THREAD));
            writesCounter++;
        }

        forkPool.invokeAll(runList);

        // reads back all records and counts them
        int counter = 0;
        for (final Iterator<JournalRecord> iter = target.iterator(); iter.hasNext(); iter.next()) {
            counter++;
        }
        assertEquals(writesCounter * TEST_WRITES_PER_THREAD, counter);

        target.stop();
    }

    /**
     * Tests concurrent read and write accesses to a single journal instance.
     * 
     * @throws IOException
     *             if initializing the journal fails, not part of this test
     * @throws InterruptedException
     *             if one of the thread is interrupted, not part of this test
     */
    @Test
    public final void testConcurrentReadsAndWrites() throws IOException, InterruptedException {

        target.start();
        assertTrue(new File(tmpFileDir.toFile(), DEFAULT_JOURNAL_FILE_PREFIX + JOURNAL_FILE_EXTENSION).exists());

        final ExecutorService execPool = Executors.newFixedThreadPool(TEST_THREAD_COUNT);
        final ArrayList<Callable<Void>> runList = new ArrayList<Callable<Void>>();
        int writesCounter = 0;
        for (int i = 0; i < TEST_THREAD_COUNT; i++) {
            runList.add(new JournalTestWriter(target, TestOp.START, TEST_WRITES_PER_THREAD));
            writesCounter++;
            runList.add(new JournalTestReader(target, TEST_READS_PER_THREAD));
        }

        execPool.invokeAll(runList);

        int counter = 0;
        for (final Iterator<JournalRecord> iter = target.iterator(); iter.hasNext(); iter.next()) {
            counter++;
        }
        assertEquals(writesCounter * TEST_WRITES_PER_THREAD, counter);

        target.stop();
    }

}
