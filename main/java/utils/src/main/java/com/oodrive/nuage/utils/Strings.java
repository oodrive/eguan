package com.oodrive.nuage.utils;

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

import javax.annotation.Nonnull;

/**
 * Utility class for {@link String}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public final class Strings {

    /**
     * No instance.
     */
    private Strings() {
        throw new AssertionError("No instance");
    }

    private static final char[] HEX_UPPER_CASE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C',
            'D', 'E', 'F' };
    private static final char[] HEX_LOWER_CASE = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
            'd', 'e', 'f' };

    /**
     * Gets the hexadecimal string representation of the given bytes.
     * 
     * @param bytes
     * @return the hexa string of the bytes buffer, with lower case characters
     */
    public static final String toHexString(@Nonnull final byte[] bytes) {
        return toHexString(bytes, false);
    }

    /**
     * Gets the hexadecimal string representation of the given bytes.
     * 
     * @param bytes
     * @param upperCase
     *            hexadecimal with upper case if <code>true</code>
     * @return the hexa string of the bytes buffer
     */
    public static final String toHexString(@Nonnull final byte[] bytes, final boolean upperCase) {
        final int byteCount = bytes.length;
        final char[] result = new char[byteCount * 2];
        int v;
        final char[] hexChars = upperCase ? HEX_UPPER_CASE : HEX_LOWER_CASE;
        for (int i = byteCount - 1; i >= 0; i--) {
            v = bytes[i] & 0xFF;
            result[i * 2] = hexChars[v / 16];
            result[i * 2 + 1] = hexChars[v % 16];
        }
        return new String(result);
    }
}
