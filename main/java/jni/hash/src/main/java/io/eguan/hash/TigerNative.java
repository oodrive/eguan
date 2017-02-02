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

import io.eguan.utils.ByteArrays;

import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;

/**
 * Tiger native implementation, based on the official implementation of the <a
 * href="http://www.cs.technion.ac.il/~biham/Reports/Tiger/">Tiger Algorithm</a>.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class TigerNative {

    // Load native code
    static {
        NarSystem.loadLibrary();
    }

    /** Hash length in bytes */
    private static int HASH_LEN = 24;

    /**
     * No instance.
     */
    private TigerNative() {
        throw new AssertionError();
    }

    /**
     * Computes the TigerNative hash for the source buffer.
     * 
     * @param src
     * @return the hash computed in a direct {@link ByteBuffer}
     */
    public final static ByteBuffer hash(final ByteBuffer src) {
        final ByteBuffer result = HashByteBufferCache.allocate(HASH_LEN);
        hash(src, result);
        return result;
    }

    /**
     * Fills the destination buffer with the TigerNative hash of the source buffer.
     * 
     * @param src
     * @param dst
     *            fills dst from its position with the TigerNative hash of the source buffer. Update the position of the
     *            dst buffer.
     */
    public final static void hash(final ByteBuffer src, final ByteBuffer dst) {
        assert dst.capacity() - dst.position() >= HASH_LEN;
        assert dst.isDirect();

        final int offsetSrc = src.position();
        final int lengthSrc = src.limit() - offsetSrc;
        final int offsetDst = dst.position();
        final int retValue;
        if (src.isDirect()) {
            retValue = tigerNativeDirect(src, offsetSrc, lengthSrc, dst, offsetDst);
        }
        else {
            retValue = tigerNative(src.array(), offsetSrc, lengthSrc, dst, offsetDst);
        }
        if (retValue != 0) {
            throw new AssertionError("retValue=" + retValue);
        }
        dst.position(offsetDst + HASH_LEN);
    }

    /**
     * Computes the TigerNative hash for the source buffer.
     * 
     * @param src
     * @return the hash computed in a byte array
     */
    public final static byte[] hashToByteArray(final ByteBuffer src) {
        final ByteBuffer result = hash(src);
        try {
            assert result.position() == HASH_LEN;
            assert result.capacity() == HASH_LEN;
            final byte[] resultB = new byte[HASH_LEN];

            ByteArrays.fillArray(result, resultB, 0);
            return resultB;
        }
        finally {
            HashByteBufferCache.release(result);
        }
    }

    /**
     * Native computation of the hash.
     * 
     * @param src
     *            direct buffer
     * @param offsetSrc
     * @param lengthSrc
     * @param dst
     * @param offsetDst
     * @return 0 on success
     */
    private static native int tigerNativeDirect(ByteBuffer src, int offsetSrc, int lengthSrc, ByteBuffer dst,
            int offsetDst);

    /**
     * Native computation of the hash.
     * 
     * @param src
     * @param offsetSrc
     * @param lengthSrc
     * @param dst
     * @param offsetDst
     */
    private static native int tigerNative(byte[] src, int offsetSrc, int lengthSrc, ByteBuffer dst, int offsetDst);

    /**
     * Computes the TigerNative hash for the source buffer.
     * 
     * @param src
     * @return the hash computed in a direct {@link ByteBuffer}
     */
    public final static ByteBuffer hash(final ByteString src) {
        final ByteBuffer result = HashByteBufferCache.allocate(HASH_LEN);
        hash(src, result);
        return result;
    }

    /**
     * Fills the destination buffer with the TigerNative hash of the source buffer.
     * 
     * @param src
     * @param dst
     *            fills dst from its position with the TigerNative hash of the source buffer. Update the position of the
     *            dst buffer.
     */
    public final static void hash(final ByteString src, final ByteBuffer dst) {
        assert dst.capacity() - dst.position() >= HASH_LEN;
        assert dst.isDirect();

        final int offsetDst = dst.position();
        final int retValue = tigerNativeByteStr(src, dst, offsetDst);
        if (retValue != 0) {
            throw new AssertionError("retValue=" + retValue);
        }
        dst.position(offsetDst + HASH_LEN);
    }

    /**
     * Computes the TigerNative hash for the source buffer.
     * 
     * @param src
     * @return the hash computed in a byte array
     */
    public final static byte[] hashToByteArray(final ByteString src) {
        final ByteBuffer result = hash(src);
        try {
            assert result.position() == HASH_LEN;
            assert result.capacity() == HASH_LEN;
            final byte[] resultB = new byte[HASH_LEN];
            result.clear();
            for (int i = 0; i < HASH_LEN; i++) {
                resultB[i] = result.get();
            }
            return resultB;
        }
        finally {
            HashByteBufferCache.release(result);
        }
    }

    private static native int tigerNativeByteStr(ByteString src, ByteBuffer dst, int offsetDst);
}
