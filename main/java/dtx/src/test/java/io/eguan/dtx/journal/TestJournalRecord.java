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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxTestHelper;
import io.eguan.dtx.journal.JournalRecord;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.dtx.DistTxWrapper;
import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry.TxOpCode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import org.junit.Test;

/**
 * Tests for the {@link JournalRecord} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestJournalRecord {

    // long and integer sizes in bytes
    private static final int LONG_SIZE_BYTES = Long.SIZE / Byte.SIZE;
    private static final int INT_SIZE_BYTES = Integer.SIZE / Byte.SIZE;

    /**
     * This timestamp is chosen explicitly as it makes errors reproducible (e.g. its protobuf encoding contains -1, so
     * reading it through an InputStream fails).
     */
    private static final long DEFAULT_TX_TIMESTAMP = 1365669478398L;

    /**
     * Dummy implementation of the {@link ReadableByteChannel} class for testing purposes.
     * 
     * 
     */
    private static class DummyReadableByteChannel implements ReadableByteChannel {
        private boolean open = true;
        private final ByteBuffer innerBuffer;

        public DummyReadableByteChannel(final byte[] content) {
            this.innerBuffer = ByteBuffer.wrap(content);
        }

        @Override
        public final boolean isOpen() {
            return this.open;
        }

        @Override
        public final void close() throws IOException {
            this.open = false;
        }

        @Override
        public final int read(final ByteBuffer dst) throws IOException {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            final byte[] data = new byte[Math.min(dst.remaining(), innerBuffer.remaining())];
            innerBuffer.get(data);
            dst.put(data);
            return data.length;
        }
    }

    /**
     * Tests the construction of a {@link JournalRecord} from a given entry and compares to the result produced by
     * {@link JournalRecord#buildJournalRecord(byte[])}.
     */
    @Test
    public final void testNewJournalRecord() {
        final long txId = DtxTestHelper.nextTxId();
        final byte[] entry1 = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(txId).setOp(TxOpCode.COMMIT).build().toByteArray();
        final byte[] entry2 = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(txId).setOp(TxOpCode.ROLLBACK).build().toByteArray();

        final JournalRecord target1 = new JournalRecord(entry1);
        assertNotNull(target1);
        assertTrue(Arrays.equals(entry1, target1.getEntry()));
        assertFalse(entry1 == target1.getEntry());

        final JournalRecord target2 = new JournalRecord(entry2);
        assertNotNull(target2);
        assertTrue(Arrays.equals(entry2, target2.getEntry()));
        assertFalse(entry2 == target2.getEntry());

        assertFalse(target1.getChecksum() == target2.getChecksum());

        final JournalRecord builtRecord1 = JournalRecord.buildJournalRecord(target1.getContent());
        assertTrue(Arrays.equals(target1.getContent(), builtRecord1.getContent()));
        assertEquals(target1.getChecksum(), builtRecord1.getChecksum());

        final JournalRecord builtRecord2 = JournalRecord.buildJournalRecord(target2.getContent());
        assertTrue(Arrays.equals(target2.getContent(), builtRecord2.getContent()));
        assertEquals(target2.getChecksum(), builtRecord2.getChecksum());
    }

    /**
     * Tests failure to construct a {@link JournalRecord} from an empty entry.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateRecordFailMissingEntry() {
        new JournalRecord(new byte[0]);
    }

    /**
     * Tests failure to construct a {@link JournalRecord} from a <code>null</code> entry.
     */
    @Test(expected = NullPointerException.class)
    public final void testCreateRecordFailNullEntry() {
        new JournalRecord(null);
    }

    /**
     * Tests the {@link JournalRecord#buildJournalRecord(byte[])} method's failure due to insufficient entry content.
     * 
     * @throws IOException
     *             not part of this test
     * @throws IllegalArgumentException
     *             if the argument is invalid, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testBuildJournalRecordFailContentTooShort() throws IOException, IllegalArgumentException {
        final byte[] entry = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(DtxTestHelper.nextTxId()).setOp(TxOpCode.ROLLBACK).build().toByteArray();
        final byte[] recordContent = new JournalRecord(entry).getContent();

        final byte[] tooShortContent = Arrays.copyOf(recordContent, recordContent.length - 2);

        JournalRecord.buildJournalRecord(tooShortContent);
    }

    /**
     * Tests the {@link JournalRecord#buildJournalRecord(byte[])} method's failure due to content too short to provide
     * the length field.
     * 
     * @throws IOException
     *             not part of this test
     * @throws IllegalArgumentException
     *             if the argument is invalid, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testBuildJournalRecordFailContentMuchTooShort() throws IOException, IllegalArgumentException {
        final byte[] entry = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(DtxTestHelper.nextTxId()).setOp(TxOpCode.ROLLBACK).build().toByteArray();
        final byte[] recordContent = new JournalRecord(entry).getContent();

        final byte[] muchTooShortContent = Arrays.copyOf(recordContent, INT_SIZE_BYTES + 1);

        JournalRecord.buildJournalRecord(muchTooShortContent);
    }

    /**
     * Tests the {@link JournalRecord#buildJournalRecord(byte[])} method's failure due to a bad checksum.
     * 
     * @throws IOException
     *             not part of this test
     * @throws IllegalArgumentException
     *             if the checksum is invalid, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testBuildJournalRecordFailBadChecksum() throws IOException, IllegalArgumentException {
        final byte[] entry = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(DtxTestHelper.nextTxId()).setOp(TxOpCode.ROLLBACK).build().toByteArray();
        final byte[] recordContent = new JournalRecord(entry).getContent();

        final byte[] badChksmContent = Arrays.copyOf(recordContent, recordContent.length);
        ByteBuffer.wrap(badChksmContent).putLong(badChksmContent.length - LONG_SIZE_BYTES, -1);

        JournalRecord.buildJournalRecord(badChksmContent);
    }

    /**
     * Tests the {@link JournalRecord#readRecordFromByteChannel(ReadableByteChannel)} method.
     * 
     * @throws IOException
     *             if reading from the input fails, not part of this test
     */
    @Test
    public final void testReadRecordFromByteChannel() throws IOException {
        final byte[] entry = DistTxWrapper.TxJournalEntry.newBuilder().setTimestamp(DEFAULT_TX_TIMESTAMP)
                .setVersion(ProtocolVersion.VERSION_1).setTxId(DtxTestHelper.nextTxId()).setOp(TxOpCode.COMMIT).build().toByteArray();
        final JournalRecord record = new JournalRecord(entry);

        final ReadableByteChannel testChannel = new DummyReadableByteChannel(record.getContent());

        final JournalRecord result = JournalRecord.readRecordFromByteChannel(testChannel);
        assertTrue(Arrays.equals(record.getContent(), result.getContent()));
        assertEquals(record.getChecksum(), result.getChecksum());
        assertTrue(Arrays.equals(record.getEntry(), result.getEntry()));
    }

    /**
     * Tests the {@link JournalRecord#readRecordFromByteChannel(ReadableByteChannel)} method's failure due to a lack of
     * data in provided by the channel (no exception thrown).
     * 
     * @throws IOException
     *             if reading from the input fails, not part of this test
     */
    @Test
    public final void testReadRecordFromByteChannelFailNotEnoughData() throws IOException {

        final byte[] badContent = new byte[2 * INT_SIZE_BYTES];
        Arrays.fill(badContent, (byte) 1);

        // channel only contains two integers
        final JournalRecord shortResult = JournalRecord.readRecordFromByteChannel(new DummyReadableByteChannel(
                badContent));
        assertTrue(shortResult == null);

        final byte[] worseContent = new byte[INT_SIZE_BYTES / 2];
        Arrays.fill(worseContent, (byte) 2);

        // channel reads even less than an integer
        final JournalRecord shorterResult = JournalRecord.readRecordFromByteChannel(new DummyReadableByteChannel(
                worseContent));
        assertTrue(shorterResult == null);

    }

    /**
     * Tests the {@link JournalRecord#readRecordFromByteChannel(ReadableByteChannel)} method's failure due to a
     * <code>null</code> argument.
     * 
     * @throws IOException
     *             if reading from the input fails, not part of this test
     * @throws NullPointerException
     *             expected for this test
     */
    @Test(expected = NullPointerException.class)
    public final void testReadRecordFromByteChannelFailNullChannel() throws IOException, NullPointerException {
        JournalRecord.readRecordFromByteChannel(null);
    }

    /**
     * Tests the {@link JournalRecord#readRecordFromByteChannel(ReadableByteChannel)} method's failure due to a zero in
     * the length field.
     * 
     * @throws IOException
     *             if reading from the input fails, not part of this test
     * @throws IllegalArgumentException
     *             if the length is invalid, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testReadRecordFromByteChannelFailLengthZero() throws IOException, IllegalArgumentException {

        final byte[] emptyEntryContent = new byte[INT_SIZE_BYTES + LONG_SIZE_BYTES];

        final IntBuffer entryBuffer = ByteBuffer.wrap(emptyEntryContent).asIntBuffer();
        entryBuffer.put(0);

        // channel contains an entry of zero length
        JournalRecord.readRecordFromByteChannel(new DummyReadableByteChannel(emptyEntryContent));
    }

    /**
     * Tests the {@link JournalRecord#readRecordFromByteChannel(ReadableByteChannel)} method's failure due to a negative
     * value in the length field.
     * 
     * @throws IOException
     *             if reading from the input fails, not part of this test
     * @throws IllegalArgumentException
     *             if the length is invalid, expected for this test
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testReadRecordFromByteChannelFailLengthNegative() throws IOException, IllegalArgumentException {

        final byte[] emptyEntryContent = new byte[INT_SIZE_BYTES + LONG_SIZE_BYTES];

        final IntBuffer entryBuffer = ByteBuffer.wrap(emptyEntryContent).asIntBuffer();
        entryBuffer.put(-1);

        // channel contains an entry of zero length
        JournalRecord.readRecordFromByteChannel(new DummyReadableByteChannel(emptyEntryContent));
    }

}
