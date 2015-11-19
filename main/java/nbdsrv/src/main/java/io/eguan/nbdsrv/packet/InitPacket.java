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

public final class InitPacket {

    /** Magic */
    public static final String MAGIC_STR = "NBDMAGIC";
    public static final long MAGIC = 0x49484156454F5054L;

    /** Flags */
    public static final short NBD_FLAG_FIXED_NEWSTYLE = 1 << 0;

    /** Header size */
    private static final int HEADER_SIZE = 64 / 8 + 64 / 8 + 16 / 8;

    /** NBD magic string */
    private final String magicStr;
    /** NBD magic number for init */
    private final long magic;
    /** global flags for the server */
    private final int globalFlags;

    public InitPacket(final String magicStr, final long magic, final int flags) {
        super();
        this.magicStr = magicStr;
        this.magic = magic;
        this.globalFlags = flags;
    }

    /**
     * Get the magic string.
     * 
     * @return the magic string
     */
    public final String getMagicStr() {
        return magicStr;
    }

    /**
     * Get the magic number.
     * 
     * @return the magic number
     */
    public final long getMagic() {
        return magic;
    }

    /**
     * Gets the global flags.
     * 
     * 
     * @return the global flags
     */
    public final int getGlobalFlags() {
        return globalFlags;
    }

    /**
     * Allocate a {@link ByteBuffer} for a {@link InitPacket}.
     * 
     * @return the allocated {@link ByteBuffer}
     */

    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release a {@link ByteBuffer}.
     * 
     * @param buffer
     *            the buffer to release
     */
    public static final void release(final ByteBuffer buffer) {
        NbdByteBufferCache.release(buffer);
    }

    /**
     * Serialize a {@link InitPacket} in a {@link ByteBuffer}.
     * 
     * @param packet
     *            the {@link InitPacket} to serialize
     * 
     * @return the {@link ByteBuffer} serialized
     */
    public static final ByteBuffer serialize(final InitPacket packet) {

        final ByteBuffer buffer = allocateHeader();

        buffer.put(packet.magicStr.getBytes());
        Utils.putUnsignedLong(buffer, packet.magic);
        Utils.putUnsignedShort(buffer, packet.globalFlags);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a {@link ByteBuffer} in a {@link InitPacket}
     * 
     * @param buffer
     *            the {@link ByteBuffer} to decode
     * 
     * @return the {@link InitPacket}
     * @throws NbdException
     */
    public static final InitPacket deserialize(final ByteBuffer buffer) throws NbdException {
        final byte[] magicBytes = new byte[MAGIC_STR.length()];
        buffer.get(magicBytes);
        final String magicStr = new String(magicBytes);

        if (!magicStr.equals(MAGIC_STR)) {
            throw new NbdException("Bad magic String");
        }

        final long magic = Utils.getUnsignedLong(buffer);
        if (magic != MAGIC) {
            throw new NbdException("Bad magic Number");
        }

        final int flags = Utils.getUnsignedShort(buffer);

        return new InitPacket(magicStr, magic, flags);
    }
}
