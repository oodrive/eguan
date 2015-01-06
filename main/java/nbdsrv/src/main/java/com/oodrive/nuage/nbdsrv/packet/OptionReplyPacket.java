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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionReplyPacket {

    private static final Logger LOGGER = LoggerFactory.getLogger(OptionReplyPacket.class);

    private static final ByteBuffer[] EMPTY_BUFFER_ARRAY = new ByteBuffer[0];
    private static final String[] EMPTY_DATA_ARRAY = new String[0];

    /** Magic */
    public static final long MAGIC = 0x3e889045565a9L;

    /** Static size */
    private static final int HEADER_SIZE = 64 / 8 + 32 / 8 + 32 / 8 + 32 / 8;

    /** Magic Number */
    private final long magic;
    /** Option code */
    private final OptionCmd optionCmd;
    /** Option reply code */
    private final OptionReplyCmd replyCmd;
    /** The data size */
    private long dataSize;

    public OptionReplyPacket(final long magic, final OptionCmd option, final OptionReplyCmd reply) {
        super();
        this.magic = magic;
        this.optionCmd = option;
        this.replyCmd = reply;
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
     * Gets the command in the reply.
     * 
     * @return the {@link OptionCmd}
     */
    public final OptionCmd getOptionCmd() {
        return optionCmd;
    }

    /**
     * Gets the reply.
     * 
     * @return the {@link OptionReplyCmd}
     */
    public final OptionReplyCmd getReplyCmd() {
        return replyCmd;
    }

    /**
     * Gets the data size.
     * 
     * @return the size of the data
     */
    public final long getDataSize() {
        return dataSize;
    }

    /**
     * Sets the data size.
     * 
     * @param dataSize
     *            the size of the data
     */
    public final void setDataSize(final long dataSize) {
        this.dataSize = dataSize;
    }

    /**
     * Allocate {@link ByteBuffer} for {@link OptionReplyPacket}.
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
     * Release an array of {@link ByteBuffer}
     * 
     * @param buffers
     *            the array of buffer to release
     */
    public static final void release(final ByteBuffer[] buffers) {
        for (final ByteBuffer buffer : buffers) {
            NbdByteBufferCache.release(buffer);
        }
    }

    /**
     * Serialize a {@link OptionReplyPacket} in a {@link ByteBuffer}
     * 
     * @param packet
     *            the {@link OptionReplyPacket} to serialize
     * @param data
     *            the data to transfer - May not be null
     * 
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer serialize(final OptionReplyPacket packet, final String data) {

        final ByteBuffer buffer;
        if (data.length() != 0) {
            buffer = NbdByteBufferCache.allocate(HEADER_SIZE + data.length() + 4);
        }
        else {
            buffer = allocateHeader();
        }
        Utils.putUnsignedLong(buffer, packet.magic);
        Utils.putUnsignedInt(buffer, packet.optionCmd.value());
        Utils.putUnsignedInt(buffer, packet.replyCmd.value());

        if (data.length() != 0) {
            Utils.putUnsignedInt(buffer, data.length() + 4);
            Utils.putUnsignedInt(buffer, data.length());
            buffer.put(data.getBytes());
        }
        else {
            Utils.putUnsignedInt(buffer, 0);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Serialize a {@link OptionReplyPacket} in an array {@link ByteBuffer}
     * 
     * @param packet
     *            the {@link OptionReplyPacket} to serialize
     * @param data
     *            the array of data to transfer- May be null
     * 
     * @return the {@link ByteBuffer}
     */
    public static final ByteBuffer[] serializeMultiple(final OptionReplyPacket packet, final String[] data) {

        final List<ByteBuffer> buffersList = new ArrayList<>();

        if (data != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("data.length= " + data.length);
            }

            for (int i = 0; i < data.length; i++) {
                final ByteBuffer buffer = serialize(packet, data[i]);
                buffersList.add(buffer);
            }
        }
        else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("No data");
            }

            final ByteBuffer buffer = serialize(packet, "");
            buffersList.add(buffer);
        }
        return buffersList.toArray(EMPTY_BUFFER_ARRAY);
    }

    /**
     * Deserialize a {@link ByteBuffer} in {@link OptionReplyPacket}.
     * 
     * @param buffer
     *            the {@link ByteBuffer} to deserialize
     * 
     * @return the {@link OptionReplyPacket}
     */
    public static final OptionReplyPacket deserialize(final ByteBuffer buffer) {

        final long magic = Utils.getUnsignedLong(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("magic=0x" + Long.toHexString(magic));
        }

        final long value = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("option=0x" + Long.toHexString(value));
        }
        final OptionCmd option = OptionCmd.valueOf(value);

        final long value2 = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("reply=0x" + Long.toHexString(value2));
        }
        final OptionReplyCmd reply = OptionReplyCmd.valueOf(value2);

        final long size = Utils.getUnsignedInt(buffer);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("size=0x" + Long.toHexString(size));
        }
        final OptionReplyPacket packet = new OptionReplyPacket(magic, option, reply);
        packet.dataSize = size;
        return packet;
    }

    /**
     * Gets data contains in the option reply packet
     * 
     * @param buffer
     *            the buffer to decode
     * @param cmd
     *            the cmd reply
     * @return an array of String contained in the ByteBuffer
     */
    public static final String[] getData(final ByteBuffer buffer, final OptionReplyPacket packet) {
        final List<String> dataList = new ArrayList<>();

        if (packet.replyCmd.equals(OptionReplyCmd.NBD_REP_SERVER)) {
            long dataSize = packet.dataSize;

            while (dataSize != 0) {
                final long size = Utils.getUnsignedInt(buffer);
                final byte[] b = new byte[Utils.getUnsignedIntPositive(size)];
                buffer.get(b);
                final String s = new String(b);
                dataList.add(s);
                // Data size + an integer for the size
                dataSize -= size + 4;
            }
        }
        return dataList.toArray(EMPTY_DATA_ARRAY);
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
}
