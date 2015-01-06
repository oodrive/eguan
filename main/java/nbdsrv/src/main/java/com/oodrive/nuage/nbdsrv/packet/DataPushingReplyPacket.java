package com.oodrive.nuage.nbdsrv.packet;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DataPushingReplyPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPushingReplyPacket.class);

    /** Magic */
    public static final long MAGIC = 0x67446698L;

    /** Static size */
    private static final int HEADER_SIZE = 32 / 8 + 32 / 8 + 64 / 8;

    /** Magic Number */
    private final long magic;
    /** Error */
    private final DataPushingError error;
    /** Handle to identify the request */
    private final long handle;

    public DataPushingReplyPacket(final long magic, final DataPushingError error, final long handle) {
        super();
        this.magic = magic;
        this.error = error;
        this.handle = handle;
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
     * Get the error code.
     * 
     * @return the error code
     */
    public final DataPushingError getError() {
        return error;
    }

    /**
     * Get the handle.
     * 
     * @return the handle
     */
    public final long getHandle() {
        return handle;
    }

    /**
     * Allocate header for {@link DataPushingReplyPacket}.
     * 
     * @return the allocated {@link ByteBuffer}
     */
    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release an {@link ByteBuffer}.
     * 
     * @param buffer
     *            the buffer to release
     */
    public static final void release(final ByteBuffer buffer) {
        NbdByteBufferCache.release(buffer);
    }

    /**
     * Serialize the {@link DataPushingReplyPacket}.
     * 
     * @param packet
     *            the {@link DataPushingReplyPacket} to serialize
     * 
     * @return the serialized {@link ByteBuffer}
     */
    public static final ByteBuffer serialize(final DataPushingReplyPacket packet) {

        final ByteBuffer buffer = allocateHeader();

        Utils.putUnsignedInt(buffer, packet.magic);
        Utils.putUnsignedInt(buffer, packet.error.value());
        buffer.putLong(packet.handle);

        buffer.flip();
        return buffer;
    }

    /**
     * Deserialize a {@link ByteBuffer} in a {@link DataPushingReplyPacket}
     * 
     * @param buffer
     *            the buffer to decode
     * @return the {@link DataPushingReplyPacket}
     * 
     * @throws NbdException
     * 
     */
    public static final DataPushingReplyPacket deserialize(final ByteBuffer buffer) throws NbdException {
        final long magic = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("magic=0x" + Long.toHexString(magic));
        }
        if (magic != MAGIC) {
            throw new NbdException("Illegal magic number for data pushing reply");
        }
        final long error = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("error=0x" + Long.toHexString(error));
        }
        if (DataPushingError.valueOf(error) != DataPushingError.NBD_NO_ERROR) {
            throw new NbdException("Received error=" + error);
        }

        final long handle = Utils.getUnsignedLong(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("handle=0x" + Long.toHexString(handle));
        }
        return new DataPushingReplyPacket(magic, DataPushingError.valueOf(error), handle);
    }
}
