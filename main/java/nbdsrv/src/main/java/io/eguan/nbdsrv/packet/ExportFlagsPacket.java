package io.eguan.nbdsrv.packet;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExportFlagsPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportFlagsPacket.class);
    /** */
    public static final int NBD_FLAG_HAS_FLAGS = (1 << 0);
    /** If export is read-only */
    public static final int NBD_FLAG_READ_ONLY = (1 << 1);
    /** If command flush is supported by the server */
    public static final int NBD_FLAG_SEND_FLUSH = (1 << 2);
    /** if the server supports NBD_CMD_FLAG_FUA */
    public static final int NBD_FLAG_SEND_FUA = (1 << 3);
    /** */
    public static final int NBD_FLAG_ROTATIONAL = (1 << 4);
    /** if the server supports NBD_CMD_TRIM */
    public static final int NBD_FLAG_SEND_TRIM = (1 << 5);

    /** Static Sizes */
    private static final int RESERVED_BYTES_SIZE = 124;
    public static final int HEADER_SIZE = 64 / 8 + 16 / 8 + 124;

    /** Size of the export */
    final private long exportSize;
    /** Properties of the export */
    final private int exportFlags;

    public ExportFlagsPacket(final long size, final int flags) {
        super();
        this.exportSize = size;
        this.exportFlags = flags;
    }

    /**
     * Gets the export size.
     * 
     * @return the size of the export
     */
    public final long getExportSize() {
        return exportSize;
    }

    /**
     * Gets the export flags.
     * 
     * @return the flags of the export
     */
    public final int getExportFlags() {
        return exportFlags;
    }

    /**
     * Allocate a header for a {@link ExportFlagsPacket}.
     * 
     * @return the allocated {@link ByteBuffer}
     */
    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release a buffer.
     * 
     * @param buffer
     *            the {@link ByteBuffer}
     */
    public static final void release(final ByteBuffer buffer) {
        NbdByteBufferCache.release(buffer);
    }

    /**
     * Serialize a packet in a {@link ByteBuffer}
     * 
     * @param packet
     *            the {@link ExportFlagsPacket} to encode
     * 
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer serialize(final ExportFlagsPacket packet) {

        final ByteBuffer buffer = allocateHeader();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Send export size=0x" + Long.toHexString(packet.exportSize));
        }

        Utils.putUnsignedLong(buffer, packet.exportSize);
        Utils.putUnsignedShort(buffer, packet.exportFlags);
        final byte[] reserved = new byte[RESERVED_BYTES_SIZE];
        buffer.put(reserved);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a {@link ByteBuffer} in a {@link ExportFlagsPacket}
     * 
     * @param src
     *            the buffer to decode
     * 
     * @return the {@link ExportFlagsPacket}
     */
    public static final ExportFlagsPacket deserialize(final ByteBuffer buffer) {
        final long size = Utils.getUnsignedLongPositive(buffer);
        final int flags = Utils.getUnsignedShort(buffer);
        return new ExportFlagsPacket(size, flags);
    }

}
