package com.oodrive.nuage.dtx.journal;

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

import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_JOURNAL_FILE_PREFIX;
import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;
import static com.oodrive.nuage.dtx.DtxConstants.JOURNAL_FILE_EXTENSION;
import static com.oodrive.nuage.dtx.DtxUtils.updateAtomicLongToAtLeast;
import static com.oodrive.nuage.dtx.journal.JournalFileUtils.readLastCompleteTxId;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent.RotationStage;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationListener;
import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.dtx.DistTxWrapper;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxMessage;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxNode;

/**
 * Public interface to a readable and writable journal instance.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class WritableTxJournal implements Iterable<JournalRecord> {

    private static final long MIN_ROTATION_THRESHOLD = 4096; // 4 KiB

    private static final Logger LOGGER = LoggerFactory.getLogger(WritableTxJournal.class);

    private final File journalDirectory;

    private final ReentrantReadWriteLock accessLock = new ReentrantReadWriteLock(true);

    @GuardedBy("accessLock")
    private FileChannel writeChannel;

    @GuardedBy("accessLock")
    private List<RwJournalIterator> readIterators;

    @GuardedBy("accessLock")
    private FileLock fileLock;

    @GuardedBy("accessLock")
    private volatile boolean started = false;

    @GuardedBy("accessLock")
    private volatile boolean starting = false;

    private final AtomicLong lastFinishedTxId = new AtomicLong(DEFAULT_LAST_TX_VALUE);

    private final Path journalFile;

    private final long rotationThreshold;

    private final JournalRotationManager rotationMgr;

    /**
     * Constructs an instance using the given filename prefix.
     * 
     * @param journalDirectory
     *            a {@link File} pointing to an existing and writable directory
     * @param filenamePrefix
     *            the prefix to use for the journal file, defaults to {@value #DEFAULT_JOURNAL_FILE_PREFIX} if
     *            <code>null</code>
     * @param rotThreshold
     *            the size threshold in bytes for journal files above which to start requesting rotation, defaults to
     *            {@value #MIN_ROTATION_THRESHOLD} if given an inferior value
     * @param rotManager
     *            {@link JournalRotationManager} in charge of rotation
     * @throws IllegalArgumentException
     *             if the journal directory does not exist or is not writable
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public WritableTxJournal(@Nonnull final File journalDirectory, final String filenamePrefix,
            final long rotThreshold, final JournalRotationManager rotManager) throws IllegalArgumentException,
            NullPointerException {

        this.journalDirectory = Objects.requireNonNull(journalDirectory);

        this.rotationThreshold = Math.max(rotThreshold, MIN_ROTATION_THRESHOLD);
        this.rotationMgr = Objects.requireNonNull(rotManager);

        String filename = Strings.isNullOrEmpty(filenamePrefix) ? DEFAULT_JOURNAL_FILE_PREFIX : filenamePrefix;
        filename += JOURNAL_FILE_EXTENSION;
        this.journalFile = FileSystems.getDefault().getPath(journalDirectory.getAbsolutePath(), filename);
    }

    /**
     * Writes a start entry to the journal.
     * 
     * @param txMessage
     *            the complete transaction to write
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws IOException
     *             if writing to the journal fails
     * @throws IllegalStateException
     *             if the {@link WritableTxJournal} was not {@link #start() started}
     */
    public final void writeStart(@Nonnull final TxMessage txMessage, @Nonnull final Iterable<TxNode> participants)
            throws IOException, IllegalStateException {

        if (!started) {
            throw new IllegalStateException("Not yet started, stopped or aborted");
        }

        checkRotationCondition();

        final long txId = txMessage.getTxId();
        final TxJournalEntry txJEntry = DistTxWrapper.TxJournalEntry.newBuilder()
                .setTimestamp(System.currentTimeMillis()).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setOp(TxOpCode.START).addAllTxNodes(Objects.requireNonNull(participants)).setTx(txMessage).build();
        final JournalRecord record = new JournalRecord(txJEntry.toByteArray());

        accessLock.writeLock().lock();
        try {
            if (!writeChannel.isOpen()) {
                openAndLockJournalFile();
            }
            assert writeChannel.isOpen();
            writeChannel.write(ByteBuffer.wrap(record.getContent()));
        }
        finally {
            accessLock.writeLock().unlock();
        }
    }

    /**
     * Writes a commit entry to the journal.
     * 
     * @param txId
     *            the transaction ID to include in the entry
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws IOException
     *             if writing the entry fails
     * @throws IllegalStateException
     *             if the {@link WritableTxJournal} was not {@link #start() started}
     */
    public final void writeCommit(final long txId, @Nonnull final Iterable<TxNode> participants) throws IOException,
            IllegalStateException {

        if (!started) {
            throw new IllegalStateException("Not yet started, stopped or aborted");
        }

        checkRotationCondition();

        final TxJournalEntry txJEntry = DistTxWrapper.TxJournalEntry.newBuilder()
                .setTimestamp(System.currentTimeMillis()).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setOp(TxOpCode.COMMIT).addAllTxNodes(Objects.requireNonNull(participants)).build();
        final JournalRecord record = new JournalRecord(txJEntry.toByteArray());

        accessLock.writeLock().lock();
        try {
            if (!writeChannel.isOpen()) {
                openAndLockJournalFile();
            }
            assert writeChannel.isOpen();
            writeChannel.write(ByteBuffer.wrap(record.getContent()));
            lastFinishedTxId.set(txId);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Committed transaction; txId=" + txId + ", journal=" + journalFile);
            }
        }
        finally {
            accessLock.writeLock().unlock();
        }
    }

    /**
     * Writes a rollback entry to the journal.
     * 
     * @param txId
     *            the transaction ID to include in the entry
     * @param errCode
     *            an optional error code causing the rollback
     * @param participants
     *            the set of participant {@link TxNode}s
     * @throws IOException
     *             if writing the entry fails
     * @throws IllegalStateException
     *             if the {@link WritableTxJournal} was not {@link #start() started}
     */
    public final void writeRollback(final long txId, final int errCode, @Nonnull final Iterable<TxNode> participants)
            throws IOException, IllegalStateException {

        if (!started) {
            throw new IllegalStateException("Not yet started, stopped or aborted");
        }

        checkRotationCondition();

        final TxJournalEntry txJEntry = DistTxWrapper.TxJournalEntry.newBuilder()
                .setTimestamp(System.currentTimeMillis()).setVersion(ProtocolVersion.VERSION_1).setTxId(txId)
                .setOp(TxOpCode.ROLLBACK).addAllTxNodes(participants).setErrCode(errCode).build();
        final JournalRecord record = new JournalRecord(txJEntry.toByteArray());

        accessLock.writeLock().lock();
        try {
            if (!writeChannel.isOpen()) {
                openAndLockJournalFile();
            }
            assert writeChannel.isOpen();
            writeChannel.write(ByteBuffer.wrap(record.getContent()));
            lastFinishedTxId.set(txId);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Rolled back transaction; txId=" + txId + ", journal=" + journalFile);
            }
        }
        finally {
            accessLock.writeLock().unlock();
        }
    }

    /**
     * Constructs a new {@link ReadOnlyTxJournal} view on this journal.
     * 
     * @return a valid {@link ReadOnlyTxJournal} instance
     * @throws IllegalStateException
     *             if this instance is not {@link #start() started}
     */
    public final ReadOnlyTxJournal newReadOnlyTxJournal() throws IllegalStateException {
        if (!started && !starting) {
            throw new IllegalStateException("Not started and not starting");
        }
        return new ReadOnlyTxJournal(this.journalFile.toFile(), this.rotationMgr);
    }

    /**
     * Starts the journal into a state ready to receive input/output operations.
     * 
     * @throws IOException
     *             if opening the journal file fails
     * @throws IllegalStateException
     *             if the target directory does not exist or is not writable
     */
    public final void start() throws IOException, IllegalStateException {
        accessLock.writeLock().lock();
        try {
            if (started || starting) {
                return;
            }
            starting = true;

            if (!this.journalDirectory.exists() || !this.journalDirectory.canWrite()) {
                throw new IllegalStateException("Journal directory does not exist or is not writable");
            }

            openAndLockJournalFile();

            readIterators = Collections.synchronizedList(new ArrayList<RwJournalIterator>());

            updateLastTxCounters();

            starting = false;
            started = true;

        }
        finally {
            accessLock.writeLock().unlock();
        }
    }

    /**
     * Gets the started state.
     * 
     * @return <code>true</code> if the journal is ready to perform input/output operations, <code>false</code>
     *         otherwise
     */
    public final boolean isStarted() {
        return started;
    }

    /**
     * Stops the instance.
     * 
     * @throws IOException
     *             if flushing output or closing the journal file fails
     */
    public final void stop() throws IOException {
        accessLock.writeLock().lock();
        try {
            if (!started) {
                return;
            }

            closeAndReleaseJournalFile();

            // closes all read channels
            closeReadIterators();

            started = false;
        }
        finally {
            accessLock.writeLock().unlock();
        }
    }

    /**
     * Gets the ID of the last finished transaction.
     * 
     * 
     * @return an up-to-date long value or {@value TransactionManager#DEFAULT_LAST_TX_VALUE} if no completed transaction
     *         was found
     * @throws IllegalStateException
     *             if the journal is not {@link #started}
     */
    public final long getLastFinishedTxId() throws IllegalStateException {
        if (!started) {
            throw new IllegalStateException("Not started");
        }
        return lastFinishedTxId.get();
    }

    /**
     * Gets the absolute filename of the journal file.
     * 
     * @return a non-empty {@link String}
     */
    public final String getJournalFilename() {
        return journalFile.toString();
    }

    private final void closeAndReleaseJournalFile() throws IOException {
        assert accessLock.writeLock().isHeldByCurrentThread();
        try {
            // releases file lock
            if (fileLock.isValid()) {
                fileLock.release();
            }
        }
        catch (final IOException e) {
            LOGGER.warn("Error releasing lock on journal file; journalFile=" + journalFile, e);
        }
        finally {
            if (writeChannel.isOpen()) {
                writeChannel.close();
            }
        }
    }

    /**
     * Closes all read iterators, suppressing any exceptions thrown by {@link AutoCloseable#close()}, and clear the
     * {@link #readIterators} list. Needs external locking.
     */
    private final void closeReadIterators() {
        synchronized (readIterators) {
            for (final RwJournalIterator currReadIterator : readIterators) {
                try {
                    currReadIterator.close();
                }
                catch (final Exception e) {
                    // logs exceptions
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Failed to close read channel; file=" + journalFile.toString());
                    }
                }
            }
            readIterators.clear();
        }
    }

    /**
     * Does the rotation of journal backup files and of the journal file itself.
     * 
     * Note: This closes all active read iterators pending a consistent way to iterate through rotations.
     * 
     * {@link IOException}s are handled internally to avoid crashing the calling threads. Most conditions are recovered
     * gracefully but if no operational state can be re-established, an emergency {@link #stop()} of the journal is
     * requested. An exception are {@link InterruptedException}s, as this is something the calling method should know
     * about.
     * 
     * @param listeners
     *            optional {@link RotationListener}s to which to dispatch {@link RotationEvent}s
     * @throws InterruptedException
     *             if the thread is interrupted during processing
     * 
     */
    final void executeRotation(final RotationListener... listeners) throws InterruptedException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Rotating journal file; file=" + journalFile + ", listeners: " + Arrays.asList(listeners));
        }
        final String journalFilename = journalFile.toString();
        accessLock.readLock().lock();
        try {
            if (!needsRotation()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Journal does not need rotation; file=" + journalFile);
                }
                return;
            }
            final RotationEvent preRotEvt = new RotationEvent(journalFilename, RotationEvent.RotationStage.PRE_ROTATE);
            for (final RotationListener currListener : listeners) {
                try {
                    currListener.rotationEventOccured(preRotEvt);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Exception on notification listener", t);
                }
            }
            shiftExistingBackupFiles(journalDirectory, journalFile);
        }
        finally {
            accessLock.readLock().unlock();
        }

        boolean rotationSuccess = false;
        // lock and move the journal file itself
        accessLock.writeLock().lock();
        try {
            try {
                closeAndReleaseJournalFile();
            }
            catch (final IOException e) {
                // if the write channel is still open, abort gracefully
                if (writeChannel.isOpen()) {
                    return;
                }
            }

            closeReadIterators();

            final File firstBackupFile = new File(journalDirectory, journalFile.getFileName() + ".1");
            try {
                if (firstBackupFile.exists()) {
                    LOGGER.warn("Could not rotate journal file, backup exists; backup=" + firstBackupFile);
                    return;
                }
                else {
                    Files.move(this.journalFile.toFile(), firstBackupFile);
                }
            }
            catch (final IOException e) {
                // moving failed, return hoping the channel can be re-opened
                LOGGER.warn("Backing up journal failed", e);
            }
            finally {
                // tries to put journal back into operation
                openAndLockJournalFile();
                rotationSuccess = true;
            }
        }
        catch (final IOException e) {
            // re-opening the write channel failed
            LOGGER.error("Could not re-open journal file channel", e);
        }
        finally {
            accessLock.writeLock().unlock();
            final RotationEvent endRotEvt = new RotationEvent(journalFilename,
                    rotationSuccess ? RotationStage.ROTATE_SUCCESS : RotationStage.ROTATE_FAILURE);
            for (final RotationListener currListener : listeners) {
                try {
                    currListener.rotationEventOccured(endRotEvt);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Exception on notification listener", t);
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Rotation finished; file=" + journalFile + ", result=" + endRotEvt.getStage());
            }
        }

    }

    /**
     * Shifts all backup files of the form <journalFilename>.<backupNumber>. This needs an external read lock.
     * 
     * @param journalDirectory
     * @param journalFile
     * @throws IOException
     */
    private static void shiftExistingBackupFiles(final File journalDirectory, final Path journalFile) {

        final String journalFilename = journalFile.getFileName().toString();

        // retrieves an inverse order map of backup files
        final Map<Integer, File> jFileMap = JournalFileUtils.getInverseBackupMap(journalDirectory, journalFilename);

        // rotates each one of the existing backup files
        for (final Integer currIndex : jFileMap.keySet()) {
            final int currIndexValue = currIndex.intValue();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Backup to rotate found; rank=" + currIndex + ", file=" + jFileMap.get(currIndex));
            }
            // shifts the backup file to the next index
            final File srcFile = jFileMap.get(currIndex);
            final File targetFile = new File(journalDirectory, journalFilename + "." + (currIndexValue + 1));

            if (srcFile.exists() && !targetFile.exists()) {
                try {
                    Files.move(srcFile, targetFile);
                }
                catch (final IOException e) {
                    // suppress errors and log them
                    LOGGER.warn("Failed to move backup file; src=" + srcFile + ", destination=" + targetFile, e);
                }
            }
            else {
                LOGGER.warn("Not rotating backup file, either source doesn't exist or destination does; source="
                        + srcFile + ", destination=" + targetFile);
            }
        }
    }

    /**
     * Checks if the journal needs to be rotated with {@link #needsRotation()} and if <code>true</code> submits a
     * rotation request to the local {@link #rotationMgr}.
     * 
     * This must not be called while holding a write lock on {@link #accessLock}.
     */
    private final void checkRotationCondition() {
        if (!needsRotation()) {
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Submitting rotation; file=" + journalFile);
        }
        this.rotationMgr.submitRotation(this);
    }

    /**
     * Checks if the rotation size condition is fulfilled.
     * 
     * This method acquires a shared lock on {@link #accessLock} and therefore must not be called while holding a write
     * lock.
     * 
     * @return <code>true</code> if the file's size could be determined and is over the threshold, <code>false</code>
     *         otherwise
     */
    private final boolean needsRotation() {
        try {
            accessLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e1) {
            LOGGER.warn("Interrupted while checking rotation condition");
            return false;
        }
        try {
            if (!writeChannel.isOpen()) {
                return false;
            }
            return (started && (writeChannel.size() > this.rotationThreshold));
        }
        catch (final IOException e) {
            LOGGER.warn("Could not determine journal file size", e);
            return false;
        }
        finally {
            accessLock.readLock().unlock();
        }
    }

    private final void openAndLockJournalFile() throws IOException {
        writeChannel = FileChannel.open(journalFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC);
        this.fileLock = writeChannel.lock();
    }

    private final void updateLastTxCounters() {
        long lastFinished = Math.max(this.lastFinishedTxId.get(), readLastCompleteTxId(this));

        if (lastFinished == DEFAULT_LAST_TX_VALUE) {
            lastFinished = readLastCompleteTxId(newReadOnlyTxJournal());
        }
        updateAtomicLongToAtLeast(lastFinishedTxId, lastFinished);
    }

    /**
     * Weakly consistent iterator for a read/write journal.
     * 
     * 
     */
    private final class RwJournalIterator implements Iterator<JournalRecord>, AutoCloseable {

        private JournalRecord currentRecord;
        private final Lock readLock;
        private final FileChannel journal;

        /**
         * Constructs a read-only iterator for a transaction journal file.
         * 
         * @param readLock
         *            the shared lock on guarding the file to acquire before reading from it
         * @param journal
         *            the target journal file
         * @throws IOException
         *             if initializing the read access to the file fails
         */
        @ParametersAreNonnullByDefault
        RwJournalIterator(final Lock readLock, final Path journal) throws IOException {
            this.readLock = Objects.requireNonNull(readLock);
            this.journal = FileChannel.open(journal, StandardOpenOption.READ);
            readNextRecord();
        }

        @Override
        public final boolean hasNext() {
            return (currentRecord != null);
        }

        @Override
        public final JournalRecord next() {
            if (currentRecord == null) {
                throw new NoSuchElementException();
            }
            final JournalRecord result = currentRecord;
            readNextRecord();
            return result;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }

        private final void readNextRecord() {
            readLock.lock();
            try {
                if (!journal.isOpen()) {
                    terminateIteration();
                    return;
                }
                currentRecord = JournalRecord.readRecordFromByteChannel(journal);
            }
            catch (final IOException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Could not read from journal", e);
                }
                // skipping or reading failed once, so we consider any further read attempts futile
                terminateIteration();
            }
            finally {
                readLock.unlock();
            }
        }

        private final void terminateIteration() {
            currentRecord = null;
        }

        @Override
        public final void close() throws Exception {
            readLock.lock();
            try {
                journal.close();
            }
            finally {
                readLock.unlock();
            }

        }
    }

    /**
     * Noop iterator in case the journal is stopped or the read channel cannot be set up.
     * 
     * 
     */
    private final class UnresponsiveIterator implements Iterator<JournalRecord> {

        @Override
        public final boolean hasNext() {
            return false;
        }

        @Override
        public final JournalRecord next() {
            throw new NoSuchElementException();
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public final Iterator<JournalRecord> iterator() {
        // only takes the read lock while modifying the synchronized list of read channels
        accessLock.readLock().lock();

        try {
            if (!started) {
                return new UnresponsiveIterator();
            }
            final RwJournalIterator result = new RwJournalIterator(accessLock.readLock(), journalFile);
            readIterators.add(result);
            return result;
        }
        catch (final IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Failed to open read channel on journal; file=" + journalFile.toString(), e);
            }
            return new UnresponsiveIterator();
        }
        finally {
            accessLock.readLock().unlock();
        }
    }

    @Override
    public final String toString() {
        return com.google.common.base.Objects.toStringHelper(WritableTxJournal.class).add("journalFile", journalFile)
                .add("started", started).add("rotationThreshold", rotationThreshold).toString();
    }

}
