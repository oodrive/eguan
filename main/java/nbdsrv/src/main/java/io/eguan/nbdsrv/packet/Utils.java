package io.eguan.nbdsrv.packet;

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

/**
 * Utility class used to manipulate unsigned/signed type.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class Utils {

    static final int MAX_HEADER_SIZE = ExportFlagsPacket.HEADER_SIZE;

    private static final long INT_FLAG_MASK_LONG = 0x00000000FFFFFFFFL;
    private static final long LONG_FLAG_MASK_LONG = 0xFFFFFFFFFFFFFFFFL;
    private static final int SHORT_FLAG_MASK_INT = 0x0000FFFF;

    /**
     * Gets an unsigned short at the current position in the buffer. The unsigned short is stored in a (signed) int.
     * 
     * @param src
     *            the {@link ByteBuffer} to parse
     * 
     * @return the unsigned short stored in a signed int
     */

    public static final int getUnsignedShort(final ByteBuffer src) {
        return (src.getShort() & SHORT_FLAG_MASK_INT);
    }

    /**
     * Gets an unsigned int at the current position in the buffer. The unsigned short is stored in a (signed) long.
     * 
     * @param src
     *            the {@link ByteBuffer} to parse
     * 
     * @return the unsigned int stored in a signed long
     */
    public static final long getUnsignedInt(final ByteBuffer src) {
        return (src.getInt() & INT_FLAG_MASK_LONG);
    }

    /**
     * Gets an unsigned integer from a long. If the long is higher than an integer, the most significant bits are
     * ignored.
     * 
     * @param l
     *            the long to convert in unsigned integer
     * 
     * @return the integer
     * 
     */
    public static final int getUnsignedIntPositive(final long l) {
        final int i = (int) (l & INT_FLAG_MASK_LONG);
        if (i < 0) {
            throw new IllegalArgumentException("Argument is negative");
        }
        return i;
    }

    /**
     * Gets an unsigned long at the current position in the buffer. The sign of the result is useless.
     * 
     * @param src
     *            the {@link ByteBuffer} to parse
     * 
     * @return the unsigned long contained in the long
     * 
     */
    public static final long getUnsignedLong(final ByteBuffer src) {
        return (src.getLong() & LONG_FLAG_MASK_LONG);
    }

    /**
     * Gets an unsigned long at the current position in the buffer. The sign of the result is checked to be stored in a
     * long (signed) with a positive value.
     * 
     * @param src
     *            the {@link ByteBuffer} to parse
     * 
     * @return the unsigned long contained in the long
     * 
     * @throws IllegalArgumentException
     *             if the long is negative
     */
    public static final long getUnsignedLongPositive(final ByteBuffer src) {
        final long l = src.getLong();
        if (l >= 0) {
            return (l & LONG_FLAG_MASK_LONG);
        }
        else {
            throw new IllegalArgumentException("Argument is negative");
        }
    }

    /**
     * Put an unsigned short in the {@link ByteBuffer} at the current position.
     * 
     * @param dst
     *            the {@link ByteBuffer} to store the short
     * @param value
     *            the integer which contains the unsigned short to store
     */
    public final static void putUnsignedShort(final ByteBuffer dst, final int value) {
        dst.putShort((short) (value & SHORT_FLAG_MASK_INT));
    }

    /**
     * Put an unsigned int in the {@link ByteBuffer} at the current position.
     * 
     * @param dst
     *            the {@link ByteBuffer} to store the int
     * @param value
     *            the long which contains the unsigned int to store
     */
    public static final void putUnsignedInt(final ByteBuffer dst, final long value) {
        dst.putInt((int) (value & INT_FLAG_MASK_LONG));
    }

    /**
     * Put an unsigned long in the {@link ByteBuffer} at the current position.
     * 
     * @param dst
     *            the {@link ByteBuffer} to store the long
     * @param value
     *            the long which contains the unsigned long to store
     */

    public static final void putUnsignedLong(final ByteBuffer dst, final long value) {
        dst.putLong(value & LONG_FLAG_MASK_LONG);
    }

}
