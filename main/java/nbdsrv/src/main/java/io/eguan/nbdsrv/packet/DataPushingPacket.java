package io.eguan.nbdsrv.packet;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a packet received during the data pushing phase.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author jmcaba
 * @author llambert
 */
public final class DataPushingPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPushingPacket.class);

    /** Magic */
    public static final long MAGIC = 0x25609513L;

    /** Static Size */
    private static final int HEADER_SIZE = 32 / 8 + 32 / 8 + 64 / 8 + 64 / 8 + 32 / 8;

    /** Magic Number */
    private final long magic;
    /** Request type */
    private final DataPushingCmd type;
    /** Handle to identify the request */
    private final long handle;
    /** Offset for the read/write request */
    private final long from;
    /** Number of bytes to read/write */
    private final long len;

    /**
     * Gets the magic number contained in the request.
     * 
     * @return the magic number
     */
    public final long getMagic() {
        return magic;
    }

    /**
     * Gets the command type contained int the request.
     * 
     * @return the {@link DataPushingCmd}
     */
    public final DataPushingCmd getType() {
        return type;
    }

    /**
     * Gets the handle used in the request to identify itself.
     * 
     * @return the handle
     */
    public final long getHandle() {
        return handle;
    }

    /**
     * Gets the offset to read/write data.
     * 
     * @return the position of the first byte
     */
    public final long getFrom() {
        return from;
    }

    /**
     * Gets the number of bytes to read/write.
     * 
     * @return the number of bytes to read/write
     */
    public final long getLen() {
        return len;
    }

    public DataPushingPacket(final long magic, final DataPushingCmd type, final long handle, final long from,
            final long len) {
        this.magic = magic;
        this.type = type;
        this.handle = handle;
        this.from = from;
        this.len = len;
    }

    /**
     * Allocate the header for Data Pushing command.
     * 
     * @return the {@link ByteBuffer} allocated
     */
    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release the header for Data Pushing command.
     * 
     * @param the
     *            {@link ByteBuffer} allocated
     */
    public static final void release(final ByteBuffer buffer) {
        NbdByteBufferCache.release(buffer);
    }

    /**
     * Serialize a Data pushing command.
     * 
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer serialize(final DataPushingPacket packet) {
        final ByteBuffer buffer = allocateHeader();

        Utils.putUnsignedInt(buffer, packet.magic);
        Utils.putUnsignedInt(buffer, packet.type.value());
        Utils.putUnsignedLong(buffer, packet.handle);
        Utils.putUnsignedLong(buffer, packet.from);
        Utils.putUnsignedInt(buffer, packet.len);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a Data pushing command.
     * 
     * @return the {@link DataPushingPacket}
     */
    public static final DataPushingPacket deserialize(final ByteBuffer buffer) {
        final long magicNumber = Utils.getUnsignedInt(buffer);

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("magicNumber=0x" + Long.toHexString(magicNumber));
        }

        final DataPushingCmd type = DataPushingCmd.valueOf(Utils.getUnsignedInt(buffer));
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("type=0x" + Long.toHexString(type.value()));
        }

        // No need to check the sign
        final long handle = Utils.getUnsignedLong(buffer);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("handle=0x" + Long.toHexString(handle));
        }

        final long from;
        final long len;

        // The len and from for the command different from READ and WRITE, might be a value which can not be contained
        // in a signed long, so ignore it
        if (type == DataPushingCmd.NBD_CMD_READ || type == DataPushingCmd.NBD_CMD_WRITE
                || type == DataPushingCmd.NBD_CMD_TRIM) {
            from = Utils.getUnsignedLong(buffer);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("from=0x" + Long.toHexString(from));
            }

            len = Utils.getUnsignedInt(buffer);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("len=0x" + Long.toHexString(len));
            }
        }
        else {
            len = 0;
            from = 0;
        }
        return new DataPushingPacket(magicNumber, type, handle, from, len);
    }
}
