package com.oodrive.nuage.nrs;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.oodrive.nuage.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey;
import com.oodrive.nuage.utils.mapper.FileMapper;

/**
 * A NrsFileBlock stores blocks of data corresponding to the contents of a <i>large</i> file. It is optimized for sparse
 * files.
 * <p>
 * Note: this class supposes that the position, mark and limit of the {@link ByteBuffer} can be freely changed as
 * needed.
 *
 * @author oodrive
 * @author llambert
 *
 */
final class NrsFileBlock extends NrsAbstractFile<ByteBuffer, NrsFileBlock> {

    /** Value returned by read() when a block have been trimmed */
    public static final ByteBuffer BLOCK_TRIMMED = ByteBuffer.allocate(0);

    NrsFileBlock(final FileMapper fileMapper, final NrsFileHeader<NrsFileBlock> header,
            final NrsMsgPostOffice postOffice) {
        super(header.getBlockSize(), fileMapper, header, postOffice, BLOCK_TRIMMED);
    }

    @Override
    final ByteBuffer newElement() {
        return NrsByteBufferCache.allocate(getElementSize());
    }

    @Override
    final void checkValueLength(final ByteBuffer value) throws NullPointerException, IllegalArgumentException {
        final int elementSize = getElementSize();
        final int len = value.limit() - value.position();
        if (len != elementSize) {
            throw new IllegalArgumentException("Invalid block len=" + len + " instead of " + elementSize);
        }
    }

    @Override
    final void appendDebugString(final StringBuilder stringBuilder, final ByteBuffer value) {
        // Read at the current position and restore the position
        final int position = value.position();
        try {
            stringBuilder.append(value.get()).append(value.get()).append(value.get()).append(value.get());
        }
        finally {
            value.position(position);
        }
    }

    @Override
    final ByteBuffer decodeValue(final NrsKey value) {
        return value.getKey().asReadOnlyByteBuffer();
    }

    @Override
    final void readFully(final MappedByteBuffer src, final ByteBuffer result) {
        final int elementSize = getElementSize();
        // Read at the current position and restore the position
        final int position = result.position();
        try {
            result.limit(position + elementSize);

            src.limit(src.position() + elementSize);
            try {
                result.put(src);
            }
            finally {
                // Restore previous limit
                src.limit(src.capacity());
            }
        }
        finally {
            result.position(position);
        }
    }

    @Override
    final void readFully(final FileChannel src, final ByteBuffer result) throws IOException {
        final int elementSize = getElementSize();
        // Read at the current position and restore the position
        final int position = result.position();
        try {
            result.limit(position + elementSize);

            int readLen = 0;
            while (readLen < elementSize) {
                final int read = src.read(result);
                if (read == -1) {
                    throw new IOException("Unexpected end of file '" + getFile() + "' readOffset=" + src.position());
                }
                readLen += read;
            }
        }
        finally {
            result.position(position);
        }
    }

    @Override
    final void writeFully(final FileChannel dst, final ByteBuffer value) throws IOException {
        final int elementSize = getElementSize();
        // Write from the current position and restore the position
        final int position = value.position();
        try {
            value.limit(position + elementSize);
            int writeLen = 0;
            while (writeLen < elementSize) {
                writeLen += dst.write(value);
            }
        }
        finally {
            value.position(position);
        }
    }

}
