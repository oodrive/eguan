package io.eguan.utils;

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

import java.nio.ByteBuffer;

/**
 * Utility class to help tests with {@link ByteBuffer} (direct or not direct).
 * 
 * @author oodrive
 * @author llambert
 */
public final class ByteBuffers {

    /** true to allocate direct byte buffers */
    public static final boolean ALLOCATE_DIRECT = !Boolean.getBoolean("io.eguan.nonDirectByteBuffers");

    /**
     * No instance.
     */
    private ByteBuffers() {
        throw new AssertionError();
    }

    /** {@link ByteBuffer} factory for unit tests */
    public static interface ByteBufferFactory {
        /**
         * Create a new {@link ByteBuffer} of the given capacity.
         * 
         * @param capacity
         * @return a new empty {@link ByteBuffer}
         */
        ByteBuffer newByteBuffer(int capacity);

        /**
         * Create a new {@link ByteBuffer} of the given contents.
         * 
         * @param contents
         * @return a new {@link ByteBuffer}
         */
        ByteBuffer newByteBuffer(byte[] contents);
    }

    /** Create {@link ByteBuffer} wrapping around a new byte array */
    public static final ByteBufferFactory FACTORY_BYTE_ARRAY = new ByteBufferFactory() {
        @Override
        public final ByteBuffer newByteBuffer(final int capacity) {
            return ByteBuffer.wrap(new byte[capacity]);
        }

        @Override
        public final ByteBuffer newByteBuffer(final byte[] contents) {
            final int len = contents.length;
            final byte[] dest = new byte[len];
            System.arraycopy(contents, 0, dest, 0, len);
            return ByteBuffer.wrap(dest);
        }
    };

    /** Create direct {@link ByteBuffer}s */
    public static final ByteBufferFactory FACTORY_BYTE_DIRECT = new ByteBufferFactory() {
        @Override
        public final ByteBuffer newByteBuffer(final int capacity) {
            return ByteBuffer.allocateDirect(capacity);
        }

        @Override
        public final ByteBuffer newByteBuffer(final byte[] contents) {
            final ByteBuffer result = ByteBuffer.allocateDirect(contents.length);
            result.put(contents);
            result.position(0);
            return result;
        }
    };

    /**
     * Compares to {@link ByteBuffer}s. Compares from the offset 0 to the current position.
     * 
     * @param b1
     * @param b2
     * @throws AssertionError
     *             on comparison failure
     */
    public static final void assertEqualsByteBuffers(final ByteBuffer b1, final ByteBuffer b2) throws AssertionError {
        assertEqualsByteBuffers(b1, b2, 0);
    }

    /**
     * Compares to {@link ByteBuffer}s. Compares from the offset 0 (for <code>b1</code>) and <code>offsetB2</code> (for
     * <code>b2</code>) to the current position.
     * 
     * @param b1
     * @param b2
     * @param offsetB2
     *            offset in <code>b2</code> of the start of the comparison
     * @throws AssertionError
     *             on comparison failure
     */
    public static final void assertEqualsByteBuffers(final ByteBuffer b1, final ByteBuffer b2, final int offsetB2)
            throws AssertionError {
        final int b1Pos = b1.position();
        final int b2Pos = b2.position() - offsetB2;
        if (b1Pos != b2Pos) {
            throw new AssertionError("Invalid position p1=" + b1Pos + ", p2=" + b2Pos);
        }
        b1.position(0);
        b2.position(offsetB2);
        for (int i = 0; i < b1Pos; i++) {
            final byte byte1 = b1.get();
            final byte byte2 = b2.get();
            if (byte1 != byte2) {
                throw new AssertionError("#" + i + ": " + (int) byte1 + "<>" + (int) byte2);
            }
        }
    }

}
