package io.eguan.nrs;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import java.lang.ref.PhantomReference;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Class representing the H1 header at the start of the NRS file.
 * <p>
 * <b>Note</b>: this class does not lock anything. The caller must ensure that read and write are consistent.
 * <p>
 * The header contains:
 * <ul>
 * <li>the file version
 * <li>the L1 table
 * </ul>
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
final class H1Header {

    /** Index of the version in the longTableView */
    private static final int VERSION_INDEX = 0;

    /** Offset to add to get the address of a L2 table */
    private static final int L1TABLE_OFFSET = 1;

    /**
     * Wraps the given {@link MappedByteBuffer} into a new instance.
     * <ul>
     * <li>In line with its purpose, this does not make any defensive copy.</li>
     * <li>Byte order is determined by the provided buffer when calling this method, and must not be changed afterwards.
     * </li>
     * <li>The capacity of the table is computed from the limit of the provided buffer, so the state of the resulting
     * instance rests on it.</li>
     * <li>Neither cluster nor word alignment of the buffer are verified.</li>
     * </ul>
     * 
     * @param mappedBuffer
     *            a non-{@code null} {@link MappedByteBuffer} instance of sufficient capacity
     * @return a functional instance of {@link H1Header}
     */
    final static H1Header wrap(@Nonnull final MappedByteBuffer mappedBuffer) {
        return new H1Header(Objects.requireNonNull(mappedBuffer));
    }

    /**
     * Internal buffer pointing to the memory-mapped file region.
     */
    private MappedByteBuffer memoryMappedTable;
    /** Keep phantom reference to ensure GC */
    private final PhantomReference<MappedByteBuffer> ref;

    /**
     * View on the {@link #memoryMappedTable} as a {@link LinkBuffer}.
     */
    private LongBuffer longTableView;

    /** Version of the file */
    private long version;

    /**
     * Constructs a new instance backed by the provided {@link MappedByteBuffer}.
     * 
     * @param mappedTable
     *            the table mapped in memory to use as backend
     */
    private H1Header(final MappedByteBuffer mappedTable) {
        super();

        this.memoryMappedTable = mappedTable;
        this.ref = new PhantomReference<>(mappedTable, null);
        this.longTableView = this.memoryMappedTable.asLongBuffer();

        // Read the version
        this.version = longTableView.get(VERSION_INDEX);
    }

    /**
     * Get the current value of the {@link NrsFile} version.
     * 
     * @return the version of the {@link NrsFile}.
     */
    final long getVersion() {
        return version;
    }

    /**
     * Increment the version and save it in the file.
     */
    final void incrVersion() {
        longTableView.put(VERSION_INDEX, ++version);
    }

    /**
     * Reload the version from the file.
     */
    final void loadVersion() {
        this.version = longTableView.get(VERSION_INDEX);
    }

    /**
     * Reads a {@link Long#SIZE long-sized} L2 address from the given offset.
     * 
     * @param l1Offset
     *            the offset in the L1 table
     * @return the L2 address read at the requested offset, or zero if
     */
    final long readL2Address(final int l1Offset) {
        return longTableView.get(L1TABLE_OFFSET + l1Offset);
    }

    /**
     * Writes the given L2 address to an entry of the table.
     * 
     * @param l1Offset
     *            the entry offset to write
     * @param l2Address
     *            the address to write
     */
    final void writeL2Address(final int l1Offset, final long l2Address) {
        this.longTableView.put(L1TABLE_OFFSET + l1Offset, l2Address);
    }

    /**
     * Prepare a {@link ByteBuffer} that allows iteration over the L2tables addresses from an image of the h1header.
     * 
     */
    final void prepareIterator(final ByteBuffer h1headerContents) {
        h1headerContents.order(longTableView.order()).rewind();
        h1headerContents.position(L1TABLE_OFFSET * NrsFileHeader.BYTES_PER_LONG);
    }

    final void close() {
        // TODO: need to force sync to ensure the writing of the buffer? This call costs a lot...
        // memoryMappedTable.force();

        // Unreference mapped buffer
        memoryMappedTable = null;
        longTableView = null;

        ref.get(); // Make compiler happy
    }

}
