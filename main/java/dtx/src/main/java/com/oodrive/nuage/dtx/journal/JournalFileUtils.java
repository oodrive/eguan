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

import static com.oodrive.nuage.dtx.DtxConstants.DEFAULT_LAST_TX_VALUE;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry;

/**
 * Utility class for journal file operations.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class JournalFileUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JournalFileUtils.class);

    /**
     * {@link FilenameFilter} for listing the journal file and all backups in a directory.
     * 
     * 
     */
    static final class JournalFilenameFilter implements FilenameFilter {

        private final String journalFilename;

        /**
         * Constructs an instance working with the provided journal filename.
         * 
         * @param journalFilename
         *            a non-<code>null</code>, non-empty journal filename
         */
        JournalFilenameFilter(@Nonnull final String journalFilename) {
            if (Strings.isNullOrEmpty(journalFilename)) {
                throw new IllegalArgumentException("Journal filename is null or empty");
            }
            this.journalFilename = journalFilename;
        }

        @Override
        public final boolean accept(final File dir, final String name) {
            return name.startsWith(journalFilename);
        }

    }

    private JournalFileUtils() {
        throw new AssertionError("Not instantiable.");
    }

    /**
     * Extracts the rank of a backup file, i.e. the number after the last dot in the filename.
     * 
     * @param baseName
     *            the journal base name to expect
     * @param filename
     *            the filename of the backup file
     * @return a positive backup number if one was found, 0 if the filename is the journal's filename itself and -1
     *         otherwise
     */
    static final int extractBackupRank(final String baseName, final String filename) {

        // gives the base file a rank of 0
        if (baseName.equals(filename)) {
            return 0;
        }

        // gives a rank of -1 to files without a dot or the last dot not immediately following the base file name
        final int dotIndex = filename.lastIndexOf('.');
        if ((dotIndex == -1) || (dotIndex > baseName.length())) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Not a backup; base=" + baseName + ", file=" + filename);
            }
            return -1;
        }
        // tries to extract a number from the filename part beyond the dot
        // Note: may return 0 or negative numbers!
        try {
            return Integer.parseInt(filename.substring(dotIndex + 1));
        }
        catch (final IllegalArgumentException e) {
            return -1;
        }
    }

    /**
     * Gets a map of backup files of a journal file from a given directory.
     * 
     * @param journalDirectory
     *            the directory to search for backups
     * @param journalFilename
     *            the base journal filename to filter by
     * @return a (possibly empty) {@link Map<Integer,File>} of backup files sorted by inverse order of backup rank
     *         (highest first)
     */
    static final NavigableMap<Integer, File> getInverseBackupMap(final File journalDirectory,
            final String journalFilename) {

        final File[] jFiles = journalDirectory.listFiles(new JournalFilenameFilter(journalFilename));

        // inits a TreeMap that'll sort in reverse order
        final TreeMap<Integer, File> result = new TreeMap<Integer, File>(Collections.reverseOrder());

        if (jFiles == null) {
            return result;
        }

        for (final File currFile : jFiles) {
            final int backupRank = extractBackupRank(journalFilename, currFile.getName());

            // don't add anything below 0 (ignores the journal file itself and everything that's not a valid backup)
            if (backupRank > 0) {
                result.put(Integer.valueOf(backupRank), currFile);
            }
        }

        return result;

    }

    /**
     * Reads the ID of the last completed transaction from the given journal.
     * 
     * @param journal
     *            a non-<code>null</code> {@link Iterable} of {@link JournalRecord}
     * @return a positive transaction ID read from the journal,
     *         {@value com.oodrive.nuage.dtx.DtxConstants#DEFAULT_LAST_TX_VALUE} if none was found
     */
    static final long readLastCompleteTxId(@Nonnull final Iterable<JournalRecord> journal) {
        long result = DEFAULT_LAST_TX_VALUE;
        // iterates on journal and decodes every entry
        for (final JournalRecord currRecord : journal) {
            try {
                final TxJournalEntry currEntry = TxJournalEntry.parseFrom(currRecord.getEntry());
                final long txId = currEntry.getTxId();
                switch (currEntry.getOp()) {
                case START:
                    break;
                case COMMIT:
                case ROLLBACK:
                    result = txId;
                    break;
                default:
                    // nothing
                }
            }
            catch (final InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
        }
        return result;
    }

}
