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

import static com.oodrive.nuage.dtx.journal.JournalFileUtils.getInverseBackupMap;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationEvent;
import com.oodrive.nuage.dtx.journal.JournalRotationManager.RotationListener;

/**
 * Read-only representation of a transaction journal.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class ReadOnlyTxJournal implements Iterable<JournalRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadOnlyTxJournal.class);

    private final File journalFile;

    private final JournalRotationManager rotationManager;

    /**
     * Constructs a read-only view of the given journal file.
     * 
     * @param journalFile
     *            the journal file to read, which must exist and be readable
     * @param rotationManager
     *            the optional {@link JournalRotationManager} responsible for rotating the given journal file
     * @throws IllegalArgumentException
     *             if the journal file does not exist or is not readable
     */
    ReadOnlyTxJournal(@Nonnull final File journalFile, final JournalRotationManager rotationManager)
            throws IllegalArgumentException {
        if (!journalFile.exists() || !journalFile.canRead()) {
            throw new IllegalArgumentException("Journal file does not exist or is not readable; file=" + journalFile);
        }
        this.journalFile = journalFile;
        this.rotationManager = rotationManager;
    }

    @Override
    public final Iterator<JournalRecord> iterator() {
        final RoJournalIterator result = new RoJournalIterator(this.journalFile.toPath());
        this.rotationManager.addRotationEventListener(result, this.journalFile.getAbsolutePath());
        return result;
    }

    /**
     * {@link Iterator} over a {@link ReadOnlyTxJournal} that's capable of iterating over the entire journal, its backup
     * history included.
     * 
     * 
     */
    private final class RoJournalIterator implements Iterator<JournalRecord>, RotationListener {

        private JournalRecord currentRecord;
        private final Path journalFile;
        private FileChannel journalChannel;

        private int lastBackupOffset;
        private long lastFilePosition;

        private final Semaphore switchLock = new Semaphore(1, true);

        private volatile boolean closed = false;

        private RoJournalIterator(final Path journalFile) {
            this.journalFile = journalFile;
            final NavigableMap<Integer, File> backupList = getInverseBackupMap(this.journalFile.getParent().toFile(),
                    this.journalFile.getFileName().toString());
            if (backupList.isEmpty()) {
                lastBackupOffset = 0;
            }
            else {
                final Integer backupIndex = backupList.firstKey();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Initializing backup index; backupRank=" + backupIndex);
                }
                lastBackupOffset = backupIndex.intValue();
            }
        }

        @Override
        public final void rotationEventOccured(final RotationEvent rotevt) throws InterruptedException {
            if (closed) {
                return;
            }
            switch (rotevt.getStage()) {
            case PRE_ROTATE:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Pre-rotate received; file=" + rotevt.getFilename());
                }
                switchLock.acquire();
                break;
            case ROTATE_SUCCESS:
                try {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Rotation success received; file=" + rotevt.getFilename() + ", backupRank="
                                + lastBackupOffset);
                    }
                    lastBackupOffset++;
                    journalChannel.close();
                    openReadFileChannel();
                    journalChannel.position(lastFilePosition);
                }
                catch (final IOException ie) {
                    throw new IllegalStateException(ie);
                }
                finally {
                    switchLock.release();
                }
                break;
            case ROTATE_FAILURE:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Rotation failure received; file=" + rotevt.getFilename() + ", backupRank="
                            + lastBackupOffset);
                }
                switchLock.release();
                break;
            default:
                LOGGER.warn("Unhandled rotation event; file=" + rotevt.getFilename() + ",stage=" + rotevt.getStage());
            }
        }

        @Override
        public final boolean hasNext() {
            final boolean result = (currentRecord != null) || readNextRecord();

            if (!result) {
                closed = true;
                rotationManager.removeRotationEventListener(this);
                try {
                    if (journalChannel != null) {
                        journalChannel.close();
                    }
                }
                catch (final IOException e) {
                    LOGGER.warn("Failed to close file channel", e);
                }
            }

            return result;
        }

        @Override
        public final JournalRecord next() {
            if ((currentRecord == null) && !readNextRecord()) {
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

        private final boolean readNextRecord() {
            try {
                switchLock.acquire();
            }
            catch (final InterruptedException e1) {
                throw new IllegalStateException("Interrupted");
            }
            try {
                if (journalChannel == null || !journalChannel.isOpen()) {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Channel closed on read, trying to re-open; backupIndex=" + lastBackupOffset);
                    }
                    openReadFileChannel();
                    if (!journalChannel.isOpen()) {
                        return false;
                    }
                }
                // checks if the end of a file was reached
                if (journalChannel.size() <= journalChannel.position()) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("current file read to the end, switching; oldBackupRank=" + lastBackupOffset);
                    }
                    if (lastBackupOffset == 0) {
                        currentRecord = null;
                        return false;
                    }
                    journalChannel.close();
                    lastBackupOffset--;
                    openReadFileChannel();
                }

                currentRecord = JournalRecord.readRecordFromByteChannel(journalChannel);
                lastFilePosition = journalChannel.position();
                return (currentRecord != null);
            }
            catch (final IOException e) {
                LOGGER.warn("Could not read from journal file", e);
                return false;
            }
            catch (final IllegalArgumentException ie) {
                LOGGER.warn("Could not read journal", ie);
                return false;
            }
            catch (final IllegalStateException ise) {
                LOGGER.warn("Journal unreadable", ise);
                return false;
            }
            finally {
                switchLock.release();
            }
        }

        /**
         * (Re-)opens the read {@link FileChannel}.
         * 
         * @throws IOException
         *             if opening the file channel fails
         */
        private final void openReadFileChannel() throws IOException {
            if (closed) {
                throw new IllegalStateException("Closed");
            }
            if (lastBackupOffset == 0) {
                journalChannel = FileChannel.open(journalFile, StandardOpenOption.READ);
            }
            else {
                final Map<Integer, File> backupFileMap = getInverseBackupMap(journalFile.getParent().toFile(),
                        journalFile.getFileName().toString());
                final File targetFile = backupFileMap.get(Integer.valueOf(lastBackupOffset));
                if (targetFile == null) {
                    LOGGER.error("Target file not found; backupRank=" + lastBackupOffset + " fileList="
                            + backupFileMap.values());
                    throw new IllegalStateException("Target file does not exist; backupRank=" + lastBackupOffset);
                }
                journalChannel = FileChannel.open(targetFile.toPath(), StandardOpenOption.READ);
            }
        }
    }

}
