package io.eguan.utils;

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

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

/**
 * Utility class around byte arrays. Must not call junit <code>Assert.assertArrayEquals()</code>: very slow, allocates
 * tons of {@link Byte}s.
 * 
 * @author oodrive
 * @author llambert
 */
public final class ByteArrays {

    /**
     * No instance.
     */
    private ByteArrays() {
        throw new AssertionError();
    }

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Converts a byte array to a hex-string
     * 
     * @param bytes
     * @return the hexadecimal version of the byte array
     */
    public static final String toHex(@Nonnull final byte[] bytes) {
        final int count = bytes.length;
        final char[] hexChars = new char[count * 2];
        int j = 0;
        for (int i = 0; i < count; i++, j += 2) {
            final int v = bytes[i] & 0xFF;
            hexChars[j] = hexArray[v >>> 4];
            hexChars[j + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Fills <code>out</code> with the contents of <code>in</code>.
     * 
     * @param in
     *            Input buffer. The position is incremented.
     * @param out
     *            Destination buffer.
     * @param outOffset
     *            Starting offset in out.
     */
    public static final void fillArray(final ByteBuffer in, final byte[] out, final int outOffset) {
        // Starts from the beginning of in
        final int len = in.clear().capacity();
        int outIdx = outOffset;
        for (int i = 0; i < len; i++) {
            out[outIdx++] = in.get();
        }
    }

    /**
     * Compares two byte arrays.
     * 
     * @param b1
     * @param b2
     * @throws AssertionError
     *             on comparison failure
     */
    public static final void assertEqualsByteArrays(final byte[] b1, final byte[] b2) throws AssertionError {
        assertEqualsByteArrays("", b1, b2);
    }

    /**
     * Compares two byte arrays.
     * 
     * @param assertHeader
     *            text header of assertion error
     * @param b1
     * @param b2
     * @throws AssertionError
     *             on comparison failure
     */
    public static final void assertEqualsByteArrays(final String assertHeader, final byte[] b1, final byte[] b2)
            throws AssertionError {
        if (b1 == b2) {
            return;
        }
        if (b1 == null || b2 == null) {
            throw new AssertionError(assertHeader + "b1=" + b1 + ", b2=" + b2);
        }
        final int b1Len = b1.length;
        final int b2Len = b2.length;
        if (b1Len != b2Len) {
            throw new AssertionError(assertHeader + "Invalid length l1=" + b1Len + ", l2=" + b2Len);
        }
        for (int i = 0; i < b1Len; i++) {
            if (b1[i] != b2[i]) {
                throw new AssertionError(assertHeader + "#" + i + ": " + (int) (b1[i]) + "<>" + (int) (b2[i]));
            }
        }
    }
}
