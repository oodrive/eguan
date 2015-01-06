package com.oodrive.nuage.nrs;

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

import java.security.SecureRandom;
import java.util.Random;

import com.oodrive.nuage.utils.ByteArrays;
import com.oodrive.nuage.utils.mapper.FileMapper;

/**
 * Test {@link NrsFile} as {@link NrsAbstractFile}.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public final class TestNrsFiles extends TestNrsAbstractFiles<byte[], NrsFile> {
    private static final Random rand = new SecureRandom();

    public TestNrsFiles() {
        super();
    }

    @Override
    final NrsAbstractFile<byte[], NrsFile> newNrsAbstractFile(final FileMapper fileMapper,
            final NrsFileHeader<NrsFile> header, final NrsMsgPostOffice postOffice) {
        return new NrsFile(fileMapper, header, postOffice);
    }

    @Override
    final int getWriteSize(final int hashSize, final int blockSize) {
        return hashSize;
    }

    @Override
    final byte[] getTrimElement() {
        return NrsFile.HASH_TRIMMED;
    }

    @Override
    final byte[] newRandomElement(final int size) {
        final byte[] block = new byte[size];
        rand.nextBytes(block);
        return block;
    }

    @Override
    final void nextRandomElement(final byte[] e, final Random random) {
        random.nextBytes(e);
    }

    @Override
    final byte[] newEmptyElement(final int size) {
        return new byte[size];
    }

    @Override
    final void releaseElement(final byte[] e) {
        // Nothing to do
    }

    @Override
    final void assertEqualsElements(final byte[] e1, final byte[] e2) {
        ByteArrays.assertEqualsByteArrays(e1, e2);
    }

}
