package io.eguan.hash;

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

import io.eguan.utils.ByteArrays;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.protobuf.ByteString;

/**
 * Hash algorithm. The class can be associated to {@link ByteBufferDigest} to create and check persistent hash digest of
 * the contents of a {@link ByteBuffer}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public enum HashAlgorithm {

    /**
     * MD5 as defined by <a href='http://tools.ietf.org/html/rfc1321'>RFC 1321</a>.
     */
    MD5("MD5", (byte) 0, 16) {
        /*
         * Implementation based on MD5Digest if the native is found.
         * 
         * @see io.eguan.hash.HashAlgorithm#getByteBufferDigestProvider(java.nio.ByteBuffer)
         */
        @Override
        final ByteBufferDigestProvider getByteBufferDigestProvider(final ByteBuffer byteBuffer) {
            // Prefer native implementation if available
            if (MD5Digest.isNative()) {
                final ByteBuffer digest = MD5Digest.doFinalNative(byteBuffer);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                return super.getByteBufferDigestProvider(byteBuffer);
            }
        }

        @Override
        ByteBufferDigestProvider getByteStringDigestProvider(final ByteString byteString) {
            // Prefer native implementation if available
            if (MD5Digest.isNative()) {
                final ByteBuffer digest = MD5Digest.doFinalNative(byteString);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                return super.getByteStringDigestProvider(byteString);
            }
        }
    },

    /**
     * TIGER as defined by <a href='http://www.cs.technion.ac.il/~biham/Reports/Tiger/'>Tiger — A Fast New Hash
     * Function</a>.
     */
    TIGER("Tiger", (byte) 1, 24) {
        /*
         * Implementation based on TigerDigest.
         * 
         * @see io.eguan.hash.HashAlgorithm#getByteBufferDigestProvider(java.nio.ByteBuffer)
         */
        @Override
        final ByteBufferDigestProvider getByteBufferDigestProvider(final ByteBuffer byteBuffer) {
            // Prefer native implementation if available
            if (TigerDigest.isNative()) {
                final ByteBuffer digest = TigerDigest.doFinalNative(byteBuffer);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                final TigerDigest digest = new TigerDigest(byteBuffer);
                return getByteBufferDigestProvider(this, digest);
            }
        }

        @Override
        ByteBufferDigestProvider getByteStringDigestProvider(final ByteString byteString) {
            // Prefer native implementation if available
            if (TigerDigest.isNative()) {
                final ByteBuffer digest = TigerDigest.doFinalNative(byteString);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                return super.getByteStringDigestProvider(byteString);
            }
        }
    },

    /**
     * SHA-1 as defined by <a href='http://tools.ietf.org/html/rfc3174'>RFC 3174</a>.
     */
    SHA1("SHA-1", (byte) 2, 20) {
        /*
         * Implementation based on SHA1Digest if the native is found.
         * 
         * @see io.eguan.hash.HashAlgorithm#getByteBufferDigestProvider(java.nio.ByteBuffer)
         */
        @Override
        final ByteBufferDigestProvider getByteBufferDigestProvider(final ByteBuffer byteBuffer) {
            // Prefer native implementation if available
            if (SHA1Digest.isNative()) {
                final ByteBuffer digest = SHA1Digest.doFinalNative(byteBuffer);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                return super.getByteBufferDigestProvider(byteBuffer);
            }
        }

        @Override
        ByteBufferDigestProvider getByteStringDigestProvider(final ByteString byteString) {
            // Prefer native implementation if available
            if (SHA1Digest.isNative()) {
                final ByteBuffer digest = SHA1Digest.doFinalNative(byteString);
                return getByteBufferDigestProvider(this, digest);
            }
            else {
                return super.getByteStringDigestProvider(byteString);
            }
        }
    },
    /**
     * SHA-256 as defined by <a href='http://csrc.nist.gov/publications/fips/fips180-3/fips180-3_final.pdf'>FIPS 180-3:
     * Secure Hash Standard (SHS)</a>.
     */
    SHA256("SHA-256", (byte) 3, 32),
    /**
     * SHA-512 as defined by <a href='http://csrc.nist.gov/publications/fips/fips180-3/fips180-3_final.pdf'>FIPS 180-3:
     * Secure Hash Standard (SHS)</a>.
     */
    SHA512("SHA-512", (byte) 4, 64);

    /** Version of the hash digest encoding */
    private static final byte VERSION1 = (byte) 0x40; // 0100 0000

    /** Bits reserved for the version encoding in the header */
    private static final byte VERSION_MASK = (byte) 0xC0; // 1100 0000

    /**
     * Create a new {@link HashAlgorithm}.
     * 
     * @param standardName
     *            standard name, to get a {@link MessageDigest} for example.
     * @param index
     *            index, id of the algorithm (written in the header)
     * @param digestLen
     *            length of the digest in bytes
     */
    private HashAlgorithm(final String standardName, final byte index, final int digestLen) {
        this.standardName = standardName;
        this.index = index;
        this.digestLen = digestLen;
    }

    /** Standard name, needed to get an implementation */
    private final String standardName;
    /** Constant index, stored in constant hash. */
    private final byte index;
    /** digest length in bytes */
    private final int digestLen;

    /**
     * Gets the Java standard name of the algorithm.
     * 
     * @return the standard name.
     */
    public final String getStandardName() {
        return standardName;
    }

    /**
     * Gets the length of a standard digest.
     * 
     * @return the digest length
     */
    public final int getStandardDigestLength() {
        return digestLen;
    }

    /**
     * Gets the length of a persistent digest.
     * 
     * @return the length of a persistent digest (with header and trailer)
     */
    public final int getPersistedDigestLength() {
        return 1 + digestLen + 1;
    }

    /**
     * Gets the {@link HashAlgorithm} used to code the given hash
     * 
     * @param hash
     *            hash to analyze
     * @return the {@link HashAlgorithm} used to code <code>hash</code>
     * @throws NoSuchAlgorithmException
     *             if the hash can not be analyzed
     */
    static final HashAlgorithm getHashHashAlgorithm(final byte[] hash) throws NoSuchAlgorithmException {
        final byte header = hash[0];
        final byte version = (byte) (header & VERSION_MASK);
        final byte index = (byte) (header & ~VERSION_MASK);
        if (version == VERSION1) {
            for (final HashAlgorithm hashAlgorithm : values()) {
                if (hashAlgorithm.index == index) {
                    return hashAlgorithm;
                }
            }
            throw new NoSuchAlgorithmException("index=" + index);
        }
        else {
            throw new NoSuchAlgorithmException("version=" + version);
        }
    }

    /**
     * Tells if the hash is valid for this algorithm.
     * 
     * @param hash
     *            hash to test
     * @return <code>true</code> if the hash is correct
     */
    final boolean checkHash(final byte[] hash) {
        final byte header = hash[0];
        final byte version = (byte) (header & VERSION_MASK);
        if (version == VERSION1) {
            if (index != (byte) (header & ~VERSION_MASK)) {
                // Wrong algorithm
                return false;
            }
            // Check hash length: header+len+trailer
            if (hash.length != (1 + digestLen + 1)) {
                // Wrong length
                return false;
            }
            // Check trailer
            final int limit = 1 + digestLen;
            byte trailer = hash[0];
            for (int i = 1; i < limit; i++) {
                trailer ^= hash[i];
            }
            if (hash[limit] != trailer) {
                return false;
            }
            // Ok
            return true;
        }
        else {
            // Unknown version
            return false;
        }
    }

    /**
     * Hash the byte between the position and the limit of the {@link ByteBuffer}. The {@link ByteBuffer} is left
     * unchanged. Default implementation based on a {@link MessageDigest}.
     * 
     * @param byteBuffer
     *            buffer to hash
     * @return digest provider
     */
    ByteBufferDigestProvider getByteBufferDigestProvider(final ByteBuffer byteBuffer) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(getStandardName());
        }
        catch (final NoSuchAlgorithmException e) {
            // Algorithms should be supported by the JVM
            throw new IllegalStateException(e);
        }

        // Hash the whole ByteBuffer
        final ByteBuffer duplicate = byteBuffer.duplicate();
        final byte[] src = new byte[duplicate.limit() - duplicate.position()];
        duplicate.get(src);
        assert duplicate.position() == duplicate.limit();
        digest.update(src);

        // Allocate result: header <digest> trailer
        final byte[] hash = new byte[1 + digestLen + 1];
        final int offset = writeHeader(hash);
        try {
            digest.digest(hash, offset, digestLen);
        }
        catch (final DigestException e) {
            // Should not occur
            throw new IllegalStateException(e);
        }
        // Write trailer
        writeTrailer(hash, offset + digestLen);

        return new ByteBufferDigestProviderImpl(this, hash);
    }

    ByteBufferDigestProvider getByteStringDigestProvider(final ByteString byteString) {
        return getByteBufferDigestProvider(byteString.asReadOnlyByteBuffer());
    }

    static final ByteBufferDigestProvider getByteBufferDigestProvider(final HashAlgorithm hashAlgorithm,
            final Digest digest) {
        // Allocate result: header <digest> trailer
        final int len = digest.getDigestSize();
        final byte[] hash = new byte[1 + len + 1];

        // Write header
        final int offset = hashAlgorithm.writeHeader(hash);
        // Write digest
        digest.doFinal(hash, offset);
        // Write trailer
        hashAlgorithm.writeTrailer(hash, offset + len);

        return new ByteBufferDigestProviderImpl(hashAlgorithm, hash);
    }

    /**
     * Compute the digest provider for the given digest buffer. Release the given buffer.
     * 
     * @param hashAlgorithm
     * @param digest
     *            Computed digest. The {@link ByteBuffer} is released.
     * @return the digest provider
     */
    static final ByteBufferDigestProvider getByteBufferDigestProvider(final HashAlgorithm hashAlgorithm,
            final ByteBuffer digest) {
        try {
            // Allocate result: header <digest> trailer
            final int len = digest.capacity();
            final byte[] hash = new byte[1 + len + 1];

            // Write header
            final int offset = hashAlgorithm.writeHeader(hash);
            // Write digest
            ByteArrays.fillArray(digest, hash, offset);
            // Write trailer
            hashAlgorithm.writeTrailer(hash, offset + len);

            return new ByteBufferDigestProviderImpl(hashAlgorithm, hash);
        }
        finally {
            HashByteBufferCache.release(digest);
        }
    }

    /**
     * Write the header of a digest for this algorithm.
     * 
     * @param hash
     *            resulting hash
     * @return index to write the position of the digest in hash.
     */
    final int writeHeader(final byte[] hash) {
        hash[0] = (byte) (VERSION1 | index);
        return 1;
    }

    /**
     * Write the trailer in the digest.
     * 
     * @param hash
     *            resulting hash
     * @param offset
     *            position to write the trailer to
     */
    final void writeTrailer(final byte[] hash, final int offset) {
        byte trailer = hash[0];
        for (int i = 1; i < offset; i++) {
            trailer ^= hash[i];
        }
        hash[offset] = trailer;
    }

    /**
     * Implementation of the {@link ByteBufferDigestProvider}. The hash
     * 
     * 
     */
    static class ByteBufferDigestProviderImpl implements ByteBufferDigestProvider {

        private final HashAlgorithm hashAlgorithm;
        private final byte[] digest;

        ByteBufferDigestProviderImpl(final HashAlgorithm hashAlgorithm, final byte[] digest) {
            super();
            this.hashAlgorithm = hashAlgorithm;
            this.digest = digest;
        }

        @Override
        public final HashAlgorithm getAlgorithm() {
            return hashAlgorithm;
        }

        @Override
        public final byte[] getDigest() {
            return digest;
        }
    }
}
