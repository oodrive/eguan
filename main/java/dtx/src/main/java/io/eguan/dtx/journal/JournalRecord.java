package io.eguan.dtx.journal;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * An {@link Immutable} journal record including a length and checksum field.
 * 
 * Journal entries are handled transparently as byte arrays without any parsing or validation to avoid useless overhead
 * while reading records sequentially.
 * 
 * <table border='1'>
 * <tr>
 * <td>length (int)</td>
 * <td>content (bytes)</td>
 * <td>CRC32 checksum (long)</td>
 * </tr>
 * <tr>
 * <td>the length in bytes of the entry</td>
 * <td>the binary journal entry of the record, at least 1 byte long</td>
 * <td>a {@link CRC32} checksum computed on the length and entry fields</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class JournalRecord {

    /**
     * Maximum length of an entry in bytes.
     */
    public static final long MAX_RECORD_LENGTH_BYTES = 20971520L; // = 10 MB

    // long and integer sizes in bytes
    private static final int LONG_SIZE_BYTES = Long.SIZE / Byte.SIZE;
    private static final int INT_SIZE_BYTES = Integer.SIZE / Byte.SIZE;

    /**
     * The journal entry.
     */
    private final byte[] entry;

    /**
     * The {@link CRC32} checksum computed on length and entry fields.
     */
    private final long checksum;

    /**
     * The complete binary content of this record.
     */
    private final byte[] content;

    /**
     * Constructs an instance with the given data as entry.
     * 
     * @param entry
     *            the non-empty entry data to include in the
     * @throws IllegalArgumentException
     *             if the argument is empty
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    JournalRecord(final byte[] entry) throws IllegalArgumentException, NullPointerException {
        if (entry.length == 0) {
            throw new IllegalArgumentException("Entry must not be empty");
        }
        this.entry = Arrays.copyOf(entry, entry.length);

        // initializes the content array
        content = new byte[this.entry.length + INT_SIZE_BYTES + LONG_SIZE_BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(content);

        // writes the length field and the entry
        buffer.putInt(entry.length);
        buffer.put(entry);

        // computes and writes the checksum
        final CRC32 chksum = new CRC32();
        chksum.update(content, 0, this.entry.length + INT_SIZE_BYTES);
        checksum = chksum.getValue();
        buffer.putLong(checksum);
    }

    /**
     * Private constructor for internal use without any argument validation.
     * 
     * @param entry
     * @param checksum
     * @param content
     */
    private JournalRecord(final byte[] entry, final long checksum, final byte[] content) {
        this.entry = entry;
        this.checksum = checksum;
        this.content = content;
    }

    /**
     * Gets the journal entry.
     * 
     * @return a copy of the entry as given at construction time
     */
    public final byte[] getEntry() {
        return Arrays.copyOf(entry, entry.length);
    }

    /**
     * Gets the record's checksum.
     * 
     * @return the checksum as provided by {@link CRC32#getValue()}
     */
    final long getChecksum() {
        return checksum;
    }

    /**
     * Gets the binary content of this record.
     * 
     * @return a copy of the complete content of this record
     */
    final byte[] getContent() {
        return Arrays.copyOf(content, content.length);
    }

    /**
     * Builds a new {@link JournalRecord} from binary content.
     * 
     * This method expects to find the length and checksum field and an entry at least 1 byte long.
     * 
     * @param content
     *            the binary content from which to read the record
     * @return a functional {@link JournalRecord}
     * @throws IllegalArgumentException
     *             if the record is too short
     */
    static JournalRecord buildJournalRecord(final byte[] content) throws IllegalArgumentException {

        if (content.length < INT_SIZE_BYTES + LONG_SIZE_BYTES + 1) {
            throw new IllegalArgumentException("Record content much too short");
        }

        final byte[] newContent = Arrays.copyOf(content, content.length);

        // reads the entry length
        final ByteBuffer buffer = ByteBuffer.wrap(newContent);
        final int entryLength = buffer.getInt();

        final int expLength = entryLength + LONG_SIZE_BYTES;
        if (buffer.remaining() < expLength) {
            throw new IllegalArgumentException("Input provides to little data; expected=" + expLength + ",found="
                    + buffer.remaining());
        }

        // reads the entry itself
        final byte[] newEntry = new byte[entryLength];
        buffer.get(newEntry);

        // computes the checksum
        final CRC32 chksum = new CRC32();
        final int checkBytes = INT_SIZE_BYTES + entryLength;
        chksum.update(newContent, 0, checkBytes);

        // reads and verifies the checksum
        final long verifSum = buffer.getLong(checkBytes);
        if (verifSum != chksum.getValue()) {
            throw new IllegalArgumentException("Checksum verification failed");
        }

        return new JournalRecord(newEntry, verifSum, newContent);
    }

    /**
     * Reads a journal record from the given {@link ReadableByteChannel}.
     * 
     * Unlike {@link #buildJournalRecord(byte[])}, this method returns <code>null</code> if it finds there is not enough
     * data to read an entire record to accommodate the use by iterators.
     * 
     * @param input
     *            a non-<code>null</code> {@link ReadableByteChannel}
     * @return a valid {@link JournalRecord} or <code>null</code> if there was not enough data
     * @throws IOException
     *             if reading from the input fails
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the input contains invalid data
     */
    static JournalRecord readRecordFromByteChannel(@Nonnull final ReadableByteChannel input) throws IOException,
            NullPointerException, IllegalArgumentException {
        final byte[] lengthField = new byte[INT_SIZE_BYTES];
        final ByteBuffer lengthBuffer = ByteBuffer.wrap(lengthField);

        if (input.read(lengthBuffer) < INT_SIZE_BYTES) {
            return null;
        }

        final int entryLength = lengthBuffer.getInt(0);

        if (entryLength <= 0) {
            throw new IllegalArgumentException("Invalid entry length; entryLength=" + entryLength);
        }

        if (entryLength >= MAX_RECORD_LENGTH_BYTES) {
            throw new IllegalArgumentException("Entry length larger than maximum allowable size; entryLength="
                    + entryLength + ", maximum size=" + MAX_RECORD_LENGTH_BYTES);
        }

        final int expRestLength = entryLength + LONG_SIZE_BYTES;

        final byte[] newContent = Arrays.copyOf(lengthField, INT_SIZE_BYTES + expRestLength);

        final ByteBuffer newContentBuffer = ByteBuffer.wrap(newContent);

        newContentBuffer.position(lengthField.length);

        if (input.read(newContentBuffer) < expRestLength) {
            return null;
        }

        return buildJournalRecord(newContent);
    }

}
