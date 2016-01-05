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

public final class OptionPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(OptionPacket.class);

    /** Magic */
    public static final long MAGIC = 0x49484156454F5054L;

    /** Static Size */
    private static final int HEADER_SIZE = 64 / 8 + 32 / 8 + 32 / 8;

    /** Magic number */
    private final long magicNumber;
    /** Option code */
    private final OptionCmd optionCode;
    /** Size of the data */
    private final long size;

    public OptionPacket(final long magicNumber, final OptionCmd optionCode, final long size) {
        this.magicNumber = magicNumber;
        this.optionCode = optionCode;
        this.size = size;
    }

    /**
     * Gets the magic number.
     * 
     * @return the magic number
     */
    public final long getMagicNumber() {
        return magicNumber;
    }

    /**
     * Gets the option code.
     * 
     * @return the option code
     */
    public final OptionCmd getOptionCode() {
        return optionCode;
    }

    /**
     * Gets the size of the next data.
     * 
     * @return the data size
     */
    public final long getSize() {
        return size;
    }

    /**
     * Allocate buffer for a {@link OptionPacket}.
     * 
     * @return the allocated {@link ByteBuffer}
     */
    public static final ByteBuffer allocateHeader() {
        return (ByteBuffer) NbdByteBufferCache.allocate(Utils.MAX_HEADER_SIZE).limit(HEADER_SIZE);
    }

    /**
     * Release an array of {@link ByteBuffer}.
     * 
     * @param buffers
     */
    public static final void release(final ByteBuffer[] buffers) {
        for (final ByteBuffer buffer : buffers) {
            NbdByteBufferCache.release(buffer);
        }
    }

    /**
     * Release a {@link ByteBuffer}.
     * 
     * @param buffer
     */
    public static final void release(final ByteBuffer buffer) {
        NbdByteBufferCache.release(buffer);
    }

    /**
     * Serialize a {@link OptionPacket} in a {@link ByteBuffer}.
     * 
     * @param packet
     *            the packet to serialize
     * @param data
     *            the data to add
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer[] serialize(final OptionPacket packet, final String data) {
        final ByteBuffer header = allocateHeader();

        Utils.putUnsignedLong(header, packet.magicNumber);
        Utils.putUnsignedInt(header, packet.optionCode.value());
        Utils.putUnsignedInt(header, packet.size);
        header.flip();

        if (packet.size != 0) {
            final ByteBuffer body = allocateData(Utils.getUnsignedIntPositive(packet.size));
            if (data != null) {
                body.put(data.getBytes());
            }
            body.flip();
            final ByteBuffer[] buffers = { header, body };
            return buffers;
        }
        else {
            final ByteBuffer[] buffers = { header };
            return buffers;
        }
    }

    /**
     * Decode and construct an {@link OptionPacket} instance from a {@link ByteBuffer}.
     * 
     * @param buffer
     *            the {@link ByteBuffer} to decode
     * 
     * @return the {@link OptionPacket}
     * 
     * @throws NbdException
     *             if something is malformed in the received packet
     */
    public static final OptionPacket deserialize(final ByteBuffer buffer) throws NbdException {
        // No need to check the sign
        final long magicNumber = Utils.getUnsignedLong(buffer);
        if (magicNumber != MAGIC) {
            throw new NbdException("Illegal magic number for option: " + Long.toHexString(magicNumber));
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("magicNumber=0x" + Long.toHexString(magicNumber));
        }
        final OptionCmd optionCode = OptionCmd.valueOf(Utils.getUnsignedInt(buffer));
        final long size = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("size=0x" + Long.toHexString(size));
        }
        return new OptionPacket(magicNumber, optionCode, size);
    }

    /**
     * Allocate a {@link ByteBuffer} to receive data.
     * 
     * @param size
     *            the size of the buffer
     * 
     * @return the allocated {@link ByteBuffer}
     */
    public static final ByteBuffer allocateData(final int size) {
        return NbdByteBufferCache.allocate(size);
    }

    /**
     * Decode a data {@link ByteBuffer} in a String
     * 
     * @param dst
     *            the {@link ByteBuffer} to decode
     * 
     * @return a String
     */
    public static final String getData(final ByteBuffer dst) {
        return new String(dst.array());
    }

}
