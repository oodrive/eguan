package io.eguan.nrs;

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

import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey;
import io.eguan.utils.mapper.FileMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A NrsFile stores a key associated to a block of data corresponding to the contents of a <i>large</i> file. It is
 * optimized for sparse files.
 *
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author ebredzinski
 *
 */
public final class NrsFile extends NrsAbstractFile<byte[], NrsFile> {

    /** Value returned by read() when a hash have been trimmed */
    public static final byte[] HASH_TRIMMED = new byte[0];

    /**
     * Constructs an instance from the given builder.
     *
     * @param fileMapper
     *            file mapper handling that file
     * @param header
     *            read header of the file
     * @param postOffice
     *            optional notification of some remote peers.
     */
    NrsFile(final FileMapper fileMapper, final NrsFileHeader<NrsFile> header, final NrsMsgPostOffice postOffice) {
        super(header.getHashSize(), fileMapper, header, postOffice, HASH_TRIMMED);
    }

    /**
     * Writes the given block in the {@link NrsFileBlock} associated to this {@link NrsFile}.
     *
     * @param blockIndex
     *            index of the block
     * @param block
     *            block to write
     * @throws IOException
     *             if the write fails
     */
    public final void writeBlock(final long blockIndex, final ByteBuffer block) throws IOException {
        final NrsFileBlock nrsFileBlock = getFileBlock();
        nrsFileBlock.write(blockIndex, block);
    }

    /**
     * Fills block with the contents of the block.
     * 
     * @param blockIndex
     * @param block
     *            block to fill with the block found in the associated {@link NrsFileBlock}.
     * @throws IOException 
     * @throws IndexOutOfBoundsException 
     */
    public final void readBlock(long blockIndex, ByteBuffer block) throws IndexOutOfBoundsException, IOException {
        final NrsFileBlock nrsFileBlock = getFileBlock();
        nrsFileBlock.read(blockIndex, block);
    }

    @Override
    final byte[] newElement() {
        return new byte[getElementSize()];
    }

    @Override
    final void checkValueLength(final byte[] value) throws NullPointerException, IllegalArgumentException {
        final int elementSize = getElementSize();
        if (value.length != elementSize) {
            throw new IllegalArgumentException("Invalid hash size=" + value.length + " instead of " + elementSize);
        }
    }

    @Override
    final void appendDebugString(final StringBuilder dst, final byte[] value) {
        dst.append(value[0]).append(value[1]).append(value[2]).append(value[3]);
    }

    @Override
    final byte[] decodeValue(final NrsKey value) {
        return value.getKey().toByteArray();
    }

    @Override
    final void readFully(final MappedByteBuffer src, final byte[] result) {
        assert result.length == getElementSize();

        src.get(result);
    }

    @Override
    final void readFully(final FileChannel src, final byte[] result) throws IOException {
        final int elementSize = getElementSize();
        assert result.length == elementSize;

        final ByteBuffer dst = ByteBuffer.wrap(result);
        int readLen = 0;
        while (readLen < elementSize) {
            final int read = src.read(dst);
            if (read == -1) {
                throw new IOException("Unexpected end of file '" + getFile() + "' readOffset=" + src.position());
            }
            readLen += read;
        }
    }

    @Override
    final void writeFully(final FileChannel dst, final byte[] value) throws IOException {
        final int elementSize = getElementSize();
        assert value.length == elementSize;

        final ByteBuffer src = ByteBuffer.wrap(value);
        int writeLen = 0;
        while (writeLen < elementSize) {
            writeLen += dst.write(src);
        }
    }
}
