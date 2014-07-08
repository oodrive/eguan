package com.oodrive.nuage.nbdsrv.packet;

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

import java.nio.ByteBuffer;

public final class GlobalFlagsPacket {

    /** Size */
    private static final int HEADER_SIZE = 32 / 8;

    /** Flags */
    public static final long NBD_FLAG_FIXED_NEWSTYLE = (1 << 0);

    /**
     * Allocate a header buffer for a {@link GlobalFlagsPacket}
     * 
     * @return the allocated {@link ByteBuffer}
     */
    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release a buffer.
     * 
     * @param dst
     *            the {@link ByteBuffer} to release
     */
    public static final void release(final ByteBuffer dst) {
        NbdByteBufferCache.release(dst);
    }

    /**
     * Serialize a flag in a {@link ByteBuffer}.
     * 
     * @param flag
     *            the flag to serialize
     * 
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer serialize(final long flag) {
        final ByteBuffer buffer = allocateHeader();

        Utils.putUnsignedInt(buffer, flag);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a {@link ByteBuffer} in a {@link GlobalFlagsPacket}
     * 
     * @param buffer
     *            the {@link ByteBuffer} to serialize
     * 
     * @param isModern
     *            <code>true</code> if the connection is modern
     * 
     * @return the long read in the buffer
     * 
     * @throws NbdException
     *             if the buffer is malformed
     */
    public static final long deserialize(final ByteBuffer buffer) throws NbdException {

        final long flags = Utils.getUnsignedInt(buffer);
        /*
         * nbd-client do not set the flag every time if (isModern != checkNewStyle(flags)) { throw new
         * NbdException("New/Old style flag is false"); }
         */
        return flags;
    }

    /**
     * Check if new flag is set.
     * 
     * @param flag
     *            the flag to check
     * 
     * @return <code>true</code> if the new flag is set
     */
    public static final boolean checkNewStyle(final long flag) {
        return (flag & NBD_FLAG_FIXED_NEWSTYLE) != 0;
    }
}
