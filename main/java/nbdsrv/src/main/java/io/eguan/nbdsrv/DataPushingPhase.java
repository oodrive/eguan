package io.eguan.nbdsrv;

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

import io.eguan.nbdsrv.packet.DataPushingCmd;
import io.eguan.nbdsrv.packet.DataPushingError;
import io.eguan.nbdsrv.packet.DataPushingPacket;
import io.eguan.nbdsrv.packet.DataPushingReplyPacket;
import io.eguan.nbdsrv.packet.NbdByteBufferCache;
import io.eguan.nbdsrv.packet.NbdException;
import io.eguan.nbdsrv.packet.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the data pushing phase.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
final class DataPushingPhase extends PhaseAbstract {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataPushingPhase.class);

    DataPushingPhase(final ClientConnection connection) {
        super(connection);
    }

    @Override
    public final boolean execute() throws IOException, NbdException {
        final DataPushingPacket dataPacket = readDataPushingPacket();

        final long length;
        try {
            length = checkArguments(dataPacket);
        }
        catch (final IllegalArgumentException e) {
            LOGGER.error("Invalid argument", e);
            return true;
        }
        switch (dataPacket.getType()) {
        case NBD_CMD_READ:
            handleRead(dataPacket.getFrom(), (int) length, dataPacket.getHandle());
            break;
        case NBD_CMD_WRITE:
            handleWrite(dataPacket.getFrom(), (int) length, dataPacket.getHandle());
            break;
        case NBD_CMD_DISC:
            LOGGER.debug("Receive NBD_CMD_DISC");
            return false;
        case NBD_CMD_TRIM:
            handleTrim(dataPacket.getFrom(), length, dataPacket.getHandle());
            break;
        case NBD_CMD_FLUSH:
            LOGGER.debug("Receive NBD_CMD_FLUSH");
            break;
        default:
            LOGGER.error("Ignore not supported command ");
            break;
        }
        return true;
    }

    /**
     * Handle the reception of a NBD_CMD_TRIM.
     * 
     * @param from
     *            the offset of the first byte to be trimmed
     * @param len
     *            the number of bytes to trim
     * @param handle
     *            the handle used to identify the request
     * @throws IOException
     * @throws NbdException
     */
    private final void handleTrim(final long from, final long len, final long handle) throws IOException, NbdException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("NBD_CMD_TRIM from " + from + " len " + len);
        }
        final ClientConnection connection = getConnection();
        final NbdDevice device = connection.getNbdDevice();
        if (device == null) {
            throw new NbdException("Client not connected");
        }

        // Trim data in the device
        if (connection.isTrimEnabled()) {
            device.trim(len, from);
        }

        // Create reply, no data necessary
        final DataPushingReplyPacket replyPacket = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                DataPushingError.NBD_NO_ERROR, handle);
        final ByteBuffer replyBuffer = DataPushingReplyPacket.serialize(replyPacket);
        try {
            connection.write(replyBuffer);
        }
        finally {
            DataPushingPacket.release(replyBuffer);
        }
    }

    /**
     * Handle the reception of a NBD_CMD_READ.
     * 
     * @param from
     *            the offset of the first byte to be copied
     * @param len
     *            the number of bytes to copy
     * @param handle
     *            the handle used to identify the request
     * 
     */
    private final void handleRead(final long from, final int len, final long handle) throws NbdException, IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("NBD_CMD_READ from " + from + " len " + len);
        }
        final ClientConnection connection = getConnection();
        final NbdDevice device = connection.getNbdDevice();
        if (device == null) {
            throw new NbdException("Client not connected");
        }
        final ByteBuffer body = NbdByteBufferCache.allocate(len);
        try {
            // Read data in the device
            device.read(body, len, from);
            body.flip();
            // Create header
            final DataPushingReplyPacket replyPacket = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                    DataPushingError.NBD_NO_ERROR, handle);
            final ByteBuffer header = DataPushingReplyPacket.serialize(replyPacket);
            try {
                // Send the two buffers with scatter gather
                final ByteBuffer[] buffers = { header, body };
                connection.write(buffers);
            }
            finally {
                DataPushingPacket.release(header);
            }
        }
        catch (final IOException e) {
            LOGGER.error("I/O Exception thrown", e);
            sendError(DataPushingError.NBD_IO_ERROR, connection, handle);
        }
        finally {
            DataPushingPacket.release(body);
        }
    }

    /**
     * Check the arguments offset and length.
     * 
     * @param DataPushingPacket
     *            the data pushing packet received
     * 
     * @return the length as an integer if it is possible.
     * 
     * @throws IllegalArgumentException
     *             if one of the parameter is illegal
     * @throws IOException
     *             IF I/O exception occurs when writing reply in the socket
     */
    private final long checkArguments(final DataPushingPacket dataPushingPacket) throws IOException {

        final ClientConnection connection = getConnection();
        final NbdDevice device = connection.getNbdDevice();
        final long handle = dataPushingPacket.getHandle();
        final long length;

        // Check command
        if (dataPushingPacket.getType() == null) {
            // Ignore unknown command
            throw new IllegalArgumentException("Unknown command");
        }
        // Check Magic number
        if (dataPushingPacket.getMagic() != DataPushingPacket.MAGIC) {
            sendError(DataPushingError.NBD_EINVAL_ERROR, connection, handle);
            throw new IllegalArgumentException("Bad magic number=0x" + Long.toHexString(dataPushingPacket.getMagic()));
        }

        // Check length parameter (defined as an unsigned int in NBD protocol)
        if (dataPushingPacket.getType() == DataPushingCmd.NBD_CMD_TRIM) {
            // long is allowed for TRIM command
            length = dataPushingPacket.getLen();
        }
        else {
            // Convert len in positive int (only if possible). Long not allowed for READ/WRITE commands
            try {
                length = Utils.getUnsignedIntPositive(dataPushingPacket.getLen());
            }
            catch (final IllegalArgumentException e) {
                sendError(DataPushingError.NBD_EINVAL_ERROR, connection, handle);
                if (dataPushingPacket.getType() == DataPushingCmd.NBD_CMD_WRITE) {
                    skipData(dataPushingPacket.getLen());
                }
                throw new IllegalArgumentException("Length=" + dataPushingPacket.getLen()
                        + " is not a positive integer");
            }

        }
        // Check if from is positive
        if (dataPushingPacket.getFrom() < 0) {
            sendError(DataPushingError.NBD_EINVAL_ERROR, connection, handle);
            if (dataPushingPacket.getType() == DataPushingCmd.NBD_CMD_WRITE) {
                skipData(length);
            }
            throw new IllegalArgumentException("Offset=" + dataPushingPacket.getFrom() + " is not a positive long");
        }
        // Check the range
        if (dataPushingPacket.getFrom() + length > device.getSize()) {
            sendError(DataPushingError.NBD_EINVAL_ERROR, connection, handle);
            if (dataPushingPacket.getType() == DataPushingCmd.NBD_CMD_WRITE) {
                skipData(length);
            }
            throw new IllegalArgumentException("Illegal range: from=" + dataPushingPacket.getFrom() + " length="
                    + length + " deviceSize=" + device.getSize());
        }
        return length;
    }

    /**
     * Consume data without processing them.
     * 
     * @param length
     *            the length to consume
     * @throws IOException
     */
    private final void skipData(final long length) throws IOException {
        final ClientConnection connection = getConnection();
        long skipLength = 0;

        while (skipLength < length) {
            final ByteBuffer bytes = NbdByteBufferCache.allocate(length - skipLength > 4096 ? 4096
                    : (int) (length - skipLength));
            try {
                // Read data into the socket
                skipLength += connection.read(bytes);
            }
            finally {
                NbdByteBufferCache.release(bytes);
            }
        }
    }

    /**
     * Send an error.
     * 
     * @param error
     *            error code to send
     * @param connection
     *            the connection
     * @param handle
     *            the handle
     * @throws IOException
     *             if some i/o error occurs during socket writing
     */
    private final void sendError(final DataPushingError error, final ClientConnection connection, final long handle)
            throws IOException {
        // Create header
        final DataPushingReplyPacket replyPacket = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC, error,
                handle);
        final ByteBuffer header = DataPushingReplyPacket.serialize(replyPacket);
        try {
            // Send the two buffers with scatter gather
            final ByteBuffer[] buffers = { header };
            connection.write(buffers);
        }
        finally {
            DataPushingPacket.release(header);
        }
    }

    /**
     * Handle the reception of a NBD_CMD_WRITE.
     * 
     * @param from
     *            the offset of the first byte to store
     * @param len
     *            the number of bytes to store
     * @param handle
     *            the handle used to identify the request
     * @throws IOException
     * 
     */
    private final void handleWrite(final long from, final int len, final long handle) throws NbdException, IOException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("NBD_CMD_WRITE from " + from + " len " + len);
        }
        final ClientConnection connection = getConnection();
        final NbdDevice device = connection.getNbdDevice();

        if (device == null) {
            throw new NbdException("Client not connected");
        }
        final ByteBuffer bytes = NbdByteBufferCache.allocate(len);
        try {
            // Read data into the socket
            connection.read(bytes);

            // Write data on the device if not read-only
            if (device.isReadOnly()) {
                LOGGER.error("Write on read-only device not permitted");
                sendError(DataPushingError.NBD_EPERM_ERROR, connection, handle);
            }
            else {
                // Write them in the device
                device.write(bytes, len, from);

                // Create reply, no data necessary
                final DataPushingReplyPacket replyPacket = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                        DataPushingError.NBD_NO_ERROR, handle);
                final ByteBuffer replyBuffer = DataPushingReplyPacket.serialize(replyPacket);
                try {
                    connection.write(replyBuffer);
                }
                finally {
                    DataPushingPacket.release(replyBuffer);
                }
            }
        }
        catch (final IOException e) {
            LOGGER.error("I/O Exception thrown", e);
            sendError(DataPushingError.NBD_IO_ERROR, connection, handle);
        }
        finally {
            DataPushingPacket.release(bytes);
        }
    }

    /**
     * Read a packet during DATA PUSHING phase.
     * 
     * @return a {@link DataPushingPacket}
     * 
     */
    private final DataPushingPacket readDataPushingPacket() throws IOException, NbdException {
        final ClientConnection client = getConnection();
        final ByteBuffer buffer = DataPushingPacket.allocateHeader();
        try {
            client.read(buffer);
            return DataPushingPacket.deserialize(buffer);
        }
        finally {
            DataPushingPacket.release(buffer);
        }
    }
}
