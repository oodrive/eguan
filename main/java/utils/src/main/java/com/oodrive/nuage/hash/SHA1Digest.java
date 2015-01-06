package com.oodrive.nuage.hash;

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.oodrive.nuage.utils.ByteArrays;

/**
 * implementation of SHA-1 as outlined in "Handbook of Applied Cryptography", pages 346 - 349.
 * 
 * It is interesting to ponder why the, apart from the extra IV, the other difference here from MD5 is the "endienness"
 * of the word processing!<br>
 * Hacked to work with ByteBuffer. Modified to have to create a new object every time you need to compute a hash value.
 * 
 * <pre>
 * SHA1Digest d = new SHA1Digest(myBuffer);
 * byte[] hash = new byte[20];
 * d.doFinal(hash, 0);
 * </pre>
 * 
 * @author oodrive
 * @author llambert
 * @author bouncy castle
 * 
 */
public final class SHA1Digest extends GeneralDigest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SHA1Digest.class);

    /* Try to find the native implementation */
    private static final String SHA1_NAME_IMPL = "com.oodrive.nuage.hash.SHA1Native";
    private static final String SHA1_HASH_NAME = "hash";
    private static final Class<?> nativeImplClass;
    private static final Method nativeImplHashByteBuffer; // Methods: both or none are null
    private static final Method nativeImplHashByteString;
    private static final boolean nativeImpl;

    static {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(SHA1_NAME_IMPL);
        }
        catch (final Throwable t) {
            LOGGER.debug("SHA1 native implementation not found", t);
        }
        if (clazz == null) {
            nativeImplClass = null;
            nativeImplHashByteBuffer = null;
            nativeImplHashByteString = null;
        }
        else {
            nativeImplClass = clazz;
            Method hashByteBuffer = null;
            try {
                hashByteBuffer = nativeImplClass.getDeclaredMethod(SHA1_HASH_NAME, ByteBuffer.class);
            }
            catch (NoSuchMethodException | SecurityException e) {
                // Ignored here
            }
            Method hashByteString = null;
            try {
                hashByteString = nativeImplClass.getDeclaredMethod(SHA1_HASH_NAME, ByteString.class);
            }
            catch (NoSuchMethodException | SecurityException e) {
                // Ignored here
            }

            if (hashByteBuffer == null || hashByteString == null) {
                nativeImplHashByteBuffer = null;
                nativeImplHashByteString = null;
                LOGGER.warn("SHA1 native hash method not found");
            }
            else {
                nativeImplHashByteBuffer = hashByteBuffer;
                nativeImplHashByteString = hashByteString;
                LOGGER.info("SHA1 native implementation found");
            }
        }
        nativeImpl = nativeImplHashByteBuffer != null;
    }

    /**
     * Tells if the SHA1 native implementation is available.
     * 
     * @return <code>true</code> if the native implementation is available.
     */
    static final boolean isNative() {
        return nativeImpl;
    }

    private static final int DIGEST_LENGTH = 20;

    private int H1 = 0x67452301;
    private int H2 = 0xefcdab89;
    private int H3 = 0x98badcfe;
    private int H4 = 0x10325476;
    private int H5 = 0xc3d2e1f0;

    private final int[] X = new int[80];
    private int xOff;

    /**
     * Standard constructor
     */
    public SHA1Digest(final ByteBuffer source) {
        super(source, true);
    }

    public final String getAlgorithmName() {
        return "SHA-1";
    }

    @Override
    public final int getDigestSize() {
        return DIGEST_LENGTH;
    }

    @Override
    protected final void processWord(final int in) {
        X[xOff] = in;

        if (++xOff == 16) {
            processBlock();
        }
    }

    @Override
    protected final void processLength(final long bitLength) {
        if (xOff > 14) {
            processBlock();
        }

        X[14] = (int) (bitLength >>> 32);
        X[15] = (int) (bitLength & 0xffffffff);
    }

    private final void unpackWord(final int word, final byte[] out, final int outOff) {
        out[outOff] = (byte) (word >>> 24);
        out[outOff + 1] = (byte) (word >>> 16);
        out[outOff + 2] = (byte) (word >>> 8);
        out[outOff + 3] = (byte) (word);
    }

    @Override
    public final int doFinal(final byte[] out, final int outOff) {
        // Try native call
        if (nativeImpl) {
            final ByteBuffer hash = doFinalNative(source);
            try {
                ByteArrays.fillArray(hash, out, outOff);
            }
            finally {
                HashByteBufferCache.release(hash);
            }

            // Done
            return DIGEST_LENGTH;
        }

        finish();

        unpackWord(H1, out, outOff);
        unpackWord(H2, out, outOff + 4);
        unpackWord(H3, out, outOff + 8);
        unpackWord(H4, out, outOff + 12);
        unpackWord(H5, out, outOff + 16);

        return DIGEST_LENGTH;
    }

    //
    // Additive constants
    //
    private static final int Y1 = 0x5a827999;
    private static final int Y2 = 0x6ed9eba1;
    private static final int Y3 = 0x8f1bbcdc;
    private static final int Y4 = 0xca62c1d6;

    private int f(final int u, final int v, final int w) {
        return ((u & v) | ((~u) & w));
    }

    private int h(final int u, final int v, final int w) {
        return (u ^ v ^ w);
    }

    private int g(final int u, final int v, final int w) {
        return ((u & v) | (u & w) | (v & w));
    }

    @Override
    protected final void processBlock() {
        //
        // expand 16 word block into 80 word block.
        //
        for (int i = 16; i < 80; i++) {
            final int t = X[i - 3] ^ X[i - 8] ^ X[i - 14] ^ X[i - 16];
            X[i] = t << 1 | t >>> 31;
        }

        //
        // set up working variables.
        //
        int A = H1;
        int B = H2;
        int C = H3;
        int D = H4;
        int E = H5;

        //
        // round 1
        //
        int idx = 0;

        for (int j = 0; j < 4; j++) {
            // E = rotateLeft(A, 5) + f(B, C, D) + E + X[idx++] + Y1
            // B = rotateLeft(B, 30)
            E += (A << 5 | A >>> 27) + f(B, C, D) + X[idx++] + Y1;
            B = B << 30 | B >>> 2;

            D += (E << 5 | E >>> 27) + f(A, B, C) + X[idx++] + Y1;
            A = A << 30 | A >>> 2;

            C += (D << 5 | D >>> 27) + f(E, A, B) + X[idx++] + Y1;
            E = E << 30 | E >>> 2;

            B += (C << 5 | C >>> 27) + f(D, E, A) + X[idx++] + Y1;
            D = D << 30 | D >>> 2;

            A += (B << 5 | B >>> 27) + f(C, D, E) + X[idx++] + Y1;
            C = C << 30 | C >>> 2;
        }

        //
        // round 2
        //
        for (int j = 0; j < 4; j++) {
            // E = rotateLeft(A, 5) + h(B, C, D) + E + X[idx++] + Y2
            // B = rotateLeft(B, 30)
            E += (A << 5 | A >>> 27) + h(B, C, D) + X[idx++] + Y2;
            B = B << 30 | B >>> 2;

            D += (E << 5 | E >>> 27) + h(A, B, C) + X[idx++] + Y2;
            A = A << 30 | A >>> 2;

            C += (D << 5 | D >>> 27) + h(E, A, B) + X[idx++] + Y2;
            E = E << 30 | E >>> 2;

            B += (C << 5 | C >>> 27) + h(D, E, A) + X[idx++] + Y2;
            D = D << 30 | D >>> 2;

            A += (B << 5 | B >>> 27) + h(C, D, E) + X[idx++] + Y2;
            C = C << 30 | C >>> 2;
        }

        //
        // round 3
        //
        for (int j = 0; j < 4; j++) {
            // E = rotateLeft(A, 5) + g(B, C, D) + E + X[idx++] + Y3
            // B = rotateLeft(B, 30)
            E += (A << 5 | A >>> 27) + g(B, C, D) + X[idx++] + Y3;
            B = B << 30 | B >>> 2;

            D += (E << 5 | E >>> 27) + g(A, B, C) + X[idx++] + Y3;
            A = A << 30 | A >>> 2;

            C += (D << 5 | D >>> 27) + g(E, A, B) + X[idx++] + Y3;
            E = E << 30 | E >>> 2;

            B += (C << 5 | C >>> 27) + g(D, E, A) + X[idx++] + Y3;
            D = D << 30 | D >>> 2;

            A += (B << 5 | B >>> 27) + g(C, D, E) + X[idx++] + Y3;
            C = C << 30 | C >>> 2;
        }

        //
        // round 4
        //
        for (int j = 0; j <= 3; j++) {
            // E = rotateLeft(A, 5) + h(B, C, D) + E + X[idx++] + Y4
            // B = rotateLeft(B, 30)
            E += (A << 5 | A >>> 27) + h(B, C, D) + X[idx++] + Y4;
            B = B << 30 | B >>> 2;

            D += (E << 5 | E >>> 27) + h(A, B, C) + X[idx++] + Y4;
            A = A << 30 | A >>> 2;

            C += (D << 5 | D >>> 27) + h(E, A, B) + X[idx++] + Y4;
            E = E << 30 | E >>> 2;

            B += (C << 5 | C >>> 27) + h(D, E, A) + X[idx++] + Y4;
            D = D << 30 | D >>> 2;

            A += (B << 5 | B >>> 27) + h(C, D, E) + X[idx++] + Y4;
            C = C << 30 | C >>> 2;
        }

        H1 += A;
        H2 += B;
        H3 += C;
        H4 += D;
        H5 += E;

        //
        // reset start of the buffer.
        //
        xOff = 0;
        for (int i = 0; i < 16; i++) {
            X[i] = 0;
        }
    }

    /**
     * Call native implementation.
     * 
     * @param source
     *            buffer to hash
     * @return digest
     * @throws NativeHashException
     */
    static final ByteBuffer doFinalNative(final ByteBuffer source) throws NativeHashException {
        return doFinalNative(source, nativeImplHashByteBuffer);
    }

    /**
     * Call native implementation.
     * 
     * @param source
     *            buffer to hash
     * @return digest
     * @throws NativeHashException
     */
    static final ByteBuffer doFinalNative(final ByteString source) throws NativeHashException {
        return doFinalNative(source, nativeImplHashByteString);
    }

    private static final ByteBuffer doFinalNative(final Object source, final Method implHash)
            throws NativeHashException {
        try {
            final ByteBuffer hash = (ByteBuffer) implHash.invoke(null, source);
            assert hash.position() == DIGEST_LENGTH;
            assert hash.capacity() == DIGEST_LENGTH;
            hash.clear();
            return hash;
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new NativeHashException("Failed to compute hash with native implementation", e);
        }
    }
}
