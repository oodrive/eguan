package io.eguan.nrs;

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

import io.eguan.nrs.NrsAbstractFile;
import io.eguan.nrs.NrsByteBufferCache;
import io.eguan.nrs.NrsFileBlock;
import io.eguan.nrs.NrsFileFlag;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.nrs.NrsMsgPostOffice;
import io.eguan.utils.ByteBuffers;
import io.eguan.utils.mapper.FileMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link NrsFileBlock} as {@link NrsAbstractFile}.
 *
 * @author oodrive
 * @author llambert
 *
 */
public final class TestNrsFileBlocks extends TestNrsAbstractFiles<ByteBuffer, NrsFileBlock> {

    private static final Random rand = new SecureRandom();

    public TestNrsFileBlocks() {
        super();
    }

    @Override
    final NrsAbstractFile<ByteBuffer, NrsFileBlock> newNrsAbstractFile(final FileMapper fileMapper,
            final NrsFileHeader<NrsFileBlock> header, final NrsMsgPostOffice postOffice) {
        return new NrsFileBlock(fileMapper, header, postOffice);
    }

    @Override
    final int getWriteSize(final int hashSize, final int blockSize) {
        return blockSize;
    }

    @Override
    final ByteBuffer getTrimElement() {
        return NrsFileBlock.BLOCK_TRIMMED;
    }

    @Override
    final ByteBuffer newRandomElement(final int size) {
        final ByteBuffer e = newEmptyElement(size);
        nextRandomElement(e, rand);
        return e;
    }

    @Override
    final void nextRandomElement(final ByteBuffer e, final Random random) {
        e.clear();
        while (e.position() != e.capacity()) {
            e.put((byte) random.nextInt());
        }
        // Reset position
        e.clear();
    }

    @Override
    final ByteBuffer newEmptyElement(final int size) {
        return NrsByteBufferCache.allocate(size);
    }

    @Override
    final void releaseElement(final ByteBuffer e) {
        NrsByteBufferCache.release(e);
    }

    @Override
    final void assertEqualsElements(final ByteBuffer e1, final ByteBuffer e2) {
        assert e1.position() == 0;
        assert e2.position() == 0;
        assert e1.capacity() == e2.capacity();
        // Sets the position to check the contents of the buffers
        e1.position(e1.capacity());
        e2.position(e2.capacity());
        ByteBuffers.assertEqualsByteBuffers(e1, e2);

        // Reset the positions for the rest of the test
        e1.position(0);
        e2.position(0);
    }

    /**
     * Reads and writes blocks in a {@link ByteBuffer} larger than blockSize.
     *
     * @throws Exception
     */
    @Test
    public void testRWLargeBlock() throws Exception {
        final int blockSize = 4 * 1024;
        final int blockCount = 512;
        final long size = blockSize * blockCount;
        final int hashSize = 20;
        final int writeSize = getWriteSize(hashSize, blockSize);
        assert writeSize == blockSize;
        final int clusterSize = writeSize * 11;
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.BLOCKS);

        // Create NrsAbstractFile
        final NrsFileHeader.Builder<NrsFileBlock> headerBuilder = newHeaderBuilder(size, blockSize, hashSize,
                clusterSize, flags);
        final NrsFileHeader<NrsFileBlock> header = headerBuilder.build();
        final NrsAbstractFile<ByteBuffer, NrsFileBlock> nrsFile = newNrsAbstractFile(fileMapper, header, null);
        nrsFile.create();
        nrsFile.open(false);
        try {
            final ByteBuffer toWrite = newRandomElement(writeSize * 16);

            // Write of the beginning of the buffer
            writeBlock(nrsFile, toWrite, 0, 0, writeSize);

            { // Takes a new random read area to be sure that the data have been read
                final ByteBuffer toRead = newRandomElement(writeSize * 16);
                try {
                    readCheckBlock(nrsFile, toWrite, toRead, 0, 0, writeSize, false, false);
                }
                finally {
                    releaseElement(toRead);
                }
            }

            // Write of the beginning of the buffer
            writeBlock(nrsFile, toWrite, 500, 0, writeSize);
            writeBlock(nrsFile, toWrite, 501, writeSize, writeSize * 2);
            writeBlock(nrsFile, toWrite, 502, writeSize * 2, writeSize * 3);

            { // Takes a new random read area to be sure that the data have been read
                final ByteBuffer toRead = newRandomElement(writeSize * 16);
                try {
                    // Last writes
                    readCheckBlock(nrsFile, toWrite, toRead, 501, writeSize, writeSize * 2, true, true);
                    // First write
                    readCheckBlock(nrsFile, toWrite, toRead, 0, 0, writeSize, false, false);
                }
                finally {
                    releaseElement(toRead);
                }
            }

            // Write after the first block written
            final int offsetInSrc1 = 55;
            writeBlock(nrsFile, toWrite, 1, offsetInSrc1, offsetInSrc1 + writeSize);
            writeBlock(nrsFile, toWrite, 2, offsetInSrc1 + writeSize, offsetInSrc1 + writeSize * 2);
            writeBlock(nrsFile, toWrite, 3, offsetInSrc1 + writeSize * 2, offsetInSrc1 + writeSize * 3);

            { // Takes a new random read area to be sure that the data have been read
                final ByteBuffer toRead = newRandomElement(writeSize * 16);
                try {
                    // Check writes
                    readCheckBlock(nrsFile, toWrite, toRead, 2, offsetInSrc1 + writeSize, offsetInSrc1 + writeSize * 2,
                            true, true);
                    readCheckBlock(nrsFile, toWrite, toRead, 501, writeSize, writeSize * 2, true, true);
                    readCheckBlock(nrsFile, toWrite, toRead, 0, 0, writeSize, false, false);
                }
                finally {
                    releaseElement(toRead);
                }
            }

            // Re-write some blocks
            final int offsetInSrc2 = 155;
            writeBlock(nrsFile, toWrite, 501, offsetInSrc2, offsetInSrc2 + writeSize);
            writeBlock(nrsFile, toWrite, 502, offsetInSrc2 + writeSize, offsetInSrc2 + writeSize * 2);
            writeBlock(nrsFile, toWrite, 503, offsetInSrc2 + writeSize * 2, offsetInSrc2 + writeSize * 3);

            { // Takes a new random read area to be sure that the data have been read
                final ByteBuffer toRead = newRandomElement(writeSize * 16);
                try {
                    // Check writes
                    readCheckBlock(nrsFile, toWrite, toRead, 502, offsetInSrc2 + writeSize, offsetInSrc2 + writeSize * 2,
                            true, true);
                    readCheckBlock(nrsFile, toWrite, toRead, 2, offsetInSrc1 + writeSize, offsetInSrc1 + writeSize * 2,
                            true, true);
                    readCheckBlock(nrsFile, toWrite, toRead, 500, 0, writeSize , false, false);
                    readCheckBlock(nrsFile, toWrite, toRead, 0, 0, writeSize, false, false);
                }
                finally {
                    releaseElement(toRead);
                }
            }


        }
        finally {
            nrsFile.close();
        }
    }

    private final void writeBlock(final NrsAbstractFile<ByteBuffer, NrsFileBlock> nrsFile, final ByteBuffer toWrite,
            final int blockIndex, final int position, final int limit) throws IOException {
        toWrite.position(position);
        toWrite.limit(limit);
        nrsFile.write(blockIndex, toWrite);

        // Check position and limit (unchanged)
        Assert.assertEquals(position, toWrite.position());
        Assert.assertEquals(limit, toWrite.limit());
    }

    private final void readCheckBlock(final NrsAbstractFile<ByteBuffer, NrsFileBlock> nrsFile,
            final ByteBuffer toWrite, final ByteBuffer toRead, final int blockIndex, final int position,
            final int limit, final boolean checkPrev, final boolean checkNext) throws IndexOutOfBoundsException,
            IOException {
        toRead.clear();
        toRead.position(position);
        toRead.limit(limit);

        nrsFile.read(blockIndex, toRead);

        // Check position and limit (unchanged)
        Assert.assertEquals(position, toRead.position());
        Assert.assertEquals(limit, toRead.limit());
        // Check contents
        toWrite.clear();
        toWrite.position(position);
        toWrite.limit(limit);
        Assert.assertEquals(toWrite, toRead);

        // Check prev block?
        final int blockSize = nrsFile.getDescriptor().getBlockSize();
        if (checkPrev) {
            readCheckBlock(nrsFile, toWrite, toRead, blockIndex - 1, position - blockSize, limit - blockSize, false,
                    false);
        }
        if (checkNext) {
            readCheckBlock(nrsFile, toWrite, toRead, blockIndex + 1, position + blockSize, limit + blockSize, false,
                    false);
        }
    }

}
