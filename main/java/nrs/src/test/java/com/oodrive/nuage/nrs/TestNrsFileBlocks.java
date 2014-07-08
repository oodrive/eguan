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

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

import com.oodrive.nuage.utils.ByteBuffers;
import com.oodrive.nuage.utils.mapper.FileMapper;

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
        assert e1.position() == e1.capacity();
        ByteBuffers.assertEqualsByteBuffers(e1, e2);
    }

}
