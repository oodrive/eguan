package io.eguan.hash;

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

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.google.protobuf.ByteString;

/**
 * This class contains utility methods around the digest of the datas of a {@link ByteBuffer} or a {@link ByteString}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class ByteBufferDigest {

    /**
     * No instance.
     */
    private ByteBufferDigest() {
        throw new AssertionError();
    }

    /**
     * Gets the digest of the contents of the given buffer. Hashes the bytes between the position and the limit of the
     * {@link ByteBuffer}. The {@link ByteBuffer} is left unchanged. The hash returned may be verified by calling
     * {@link #match(ByteBuffer, byte[])}.
     * 
     * @param algorithm
     *            hash algorithm
     * @param buffer
     *            buffer to read
     * @return the digest for the data of the buffer
     */
    public static final byte[] digest(final HashAlgorithm algorithm, final ByteBuffer buffer) {
        final ByteBufferDigestProvider provider = algorithm.getByteBufferDigestProvider(buffer);
        return provider.getDigest();
    }

    /**
     * Tells if the contents of the buffer and the hash match. The hash algorithm is read from the hash. Than the hash
     * is computed for the given buffer and tested against the given hash.
     * 
     * @param buffer
     *            buffer to check
     * @param hash
     *            input hash to compare
     * @return <code>true</code> if the buffer and the hash match
     * @throws NoSuchAlgorithmException
     *             if the hash algorithm is not found
     * @throws IllegalArgumentException
     *             if <code>hash</code> is corrupted
     */
    public static final boolean match(final ByteBuffer buffer, final byte[] hash) throws NoSuchAlgorithmException,
            IllegalArgumentException {
        // Get algorithm and check hash contents
        final HashAlgorithm hashAlgorithm = HashAlgorithm.getHashHashAlgorithm(hash);
        if (!hashAlgorithm.checkHash(hash)) {
            throw new IllegalArgumentException();
        }
        // Compute digest and compare
        final byte[] hashNew = digest(hashAlgorithm, buffer);
        return Arrays.equals(hash, hashNew);
    }

    /**
     * Gets the digest of the contents of the given buffer. The hash returned may be verified by calling
     * {@link #match(ByteString, byte[])}.
     * 
     * @param algorithm
     *            hash algorithm
     * @param byteString
     *            buffer to read
     * @return the digest for the data of the buffer
     */
    public static final byte[] digest(final HashAlgorithm algorithm, final ByteString byteString) {
        final ByteBufferDigestProvider provider = algorithm.getByteStringDigestProvider(byteString);
        return provider.getDigest();
    }

    /**
     * Tells if the contents of the buffer and the hash match. The hash algorithm is read from the hash. Than the hash
     * is computed for the given buffer and tested against the given hash.
     * 
     * @param byteString
     *            buffer to check
     * @param hash
     *            input hash to compare
     * @return <code>true</code> if the buffer and the hash match
     * @throws NoSuchAlgorithmException
     *             if the hash algorithm is not found
     * @throws IllegalArgumentException
     *             if <code>hash</code> is corrupted
     */
    public static final boolean match(final ByteString byteString, final byte[] hash) throws NoSuchAlgorithmException,
            IllegalArgumentException {
        // Get algorithm and check hash contents
        final HashAlgorithm hashAlgorithm = HashAlgorithm.getHashHashAlgorithm(hash);
        if (!hashAlgorithm.checkHash(hash)) {
            throw new IllegalArgumentException();
        }
        // Compute digest and compare
        final byte[] hashNew = digest(hashAlgorithm, byteString);
        return Arrays.equals(hash, hashNew);
    }

}
