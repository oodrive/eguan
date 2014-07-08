package com.oodrive.nuage.utils;

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

import java.util.UUID;

/**
 * {@link CharSequence} over the hexa string representation of a UUID. The first char is the hexadecimal representation
 * of the highest half-byte of the most significant long. The last char is the hexadecimal representation of the lowest
 * half-byte of the least significant long.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class UuidCharSequence implements CharSequence {

    /**
     * Implementation of the subsequence.
     * 
     * 
     */
    private class SubSequence implements CharSequence {
        private final int start;
        private final int end;

        SubSequence(final int start, final int end) {
            super();
            this.start = start;
            this.end = end;
        }

        @Override
        public final CharSequence subSequence(final int start, final int end) {
            if (start < 0 || end < 0 || start > end || end > length()) {
                throw new IndexOutOfBoundsException("start=" + start + ", end=" + end);
            }
            return new SubSequence(this.start + start, this.start + end);
        }

        @Override
        public final int length() {
            return end - start;
        }

        @Override
        public final char charAt(final int index) {
            if (index < 0 || index >= length()) {
                throw new IndexOutOfBoundsException("index=" + index);
            }
            return UuidCharSequence.this.charAt(start + index);
        }

        @Override
        public final String toString() {
            return new StringBuilder(length()).append(this).toString();
        }

    }

    private static final char[] HEX_UPPER_CASE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
            'D', 'E', 'F' };
    private static final char[] HEX_LOWER_CASE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
            'd', 'e', 'f' };

    private final UUID uuid;
    private final boolean upperCase;

    /**
     * Create a new lower-case char sequence over a UUID.
     * 
     * @param uuid
     *            uuid to parse
     */
    public UuidCharSequence(final UUID uuid) {
        this(uuid, false);
    }

    /**
     * Create a new char sequence over the given UUID.
     * 
     * @param uuid
     *            uuid to parse
     * @param upperCase
     *            <code>true</code> to select upper characters.
     */
    public UuidCharSequence(final UUID uuid, final boolean upperCase) {
        super();
        this.uuid = uuid;
        this.upperCase = upperCase;
    }

    /**
     * Create a new lower-case char sequence over a UUID.
     * 
     * @param uuid
     *            uuid to parse
     */
    public <F> UuidCharSequence(final UuidT<F> uuid) {
        this(uuid.getUuid());
    }

    /**
     * Create a new char sequence over the given UUID.
     * 
     * @param uuid
     *            uuid to parse
     * @param upperCase
     *            <code>true</code> to select upper characters.
     */
    public <F> UuidCharSequence(final UuidT<F> uuid, final boolean upperCase) {
        this(uuid.getUuid(), upperCase);
    }

    @Override
    public final int length() {
        return 32;
    }

    @Override
    public final char charAt(final int index) {
        if (index < 0 || index >= length()) {
            throw new IndexOutOfBoundsException("index=" + index);
        }
        final long value;
        final int shift;
        if (index > 15) {
            value = uuid.getLeastSignificantBits();
            shift = (31 - 16 - index) * 4;
        }
        else {
            value = uuid.getMostSignificantBits();
            shift = (15 - index) * 4;
        }
        final int digit = (int) (value >> shift) & 0xF;
        return upperCase ? HEX_UPPER_CASE[digit] : HEX_LOWER_CASE[digit];
    }

    @Override
    public final CharSequence subSequence(final int start, final int end) {
        if (start < 0 || end < 0 || start > end || end > length()) {
            throw new IndexOutOfBoundsException("start=" + start + ", end=" + end);
        }
        return new SubSequence(start, end);
    }

    @Override
    public final String toString() {
        return new StringBuilder(32).append(this).toString();
    }

}
