package io.eguan.nrs;

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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Flags sets on a {@link NrsFile}. The flags are written in the file header {@link NrsFileHeader}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public enum NrsFileFlag {
    /**
     * Identifies the root snapshot.
     */
    ROOT(1 << 0),
    /**
     * Set when the NrsFile is a partial. A partial file is incomplete: when a key is not present for a given offset,
     * the key may be set in a parent {@link NrsFile}.
     */
    PARTIAL(1 << 1),
    /**
     * Set when the <b>NrsFile</b> is associated to one or more {@link NrsFileBlock}.
     */
    BLOCKS(1 << 2);

    /** Bit set when the flag is set */
    private final int code;

    private NrsFileFlag(final int code) {
        this.code = code;
    }

    /**
     * Encode the given flags in the buffer.
     * 
     * @param buffer
     * @param flags
     */
    static final void encodeFlags(final ByteBuffer buffer, final Set<NrsFileFlag> flags) {
        int value = 0;
        for (final NrsFileFlag flag : flags) {
            value |= flag.code;
        }
        buffer.putInt(value);
    }

    /**
     * Gets the flags coded in the buffer at the current position.
     * 
     * @param buffer
     * @return the flag found
     */
    static final Set<NrsFileFlag> decodeFlags(final ByteBuffer buffer) {
        final int value = buffer.getInt();
        final EnumSet<NrsFileFlag> result = EnumSet.noneOf(NrsFileFlag.class);
        for (final NrsFileFlag flag : values()) {
            if ((value & flag.code) != 0) {
                result.add(flag);
            }
        }
        return Collections.unmodifiableSet(result);
    }
}
