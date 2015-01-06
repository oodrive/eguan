package com.oodrive.nuage.nbdsrv.client;

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nbdsrv.SocketHandle;
import com.oodrive.nuage.nbdsrv.packet.DataPushingCmd;
import com.oodrive.nuage.nbdsrv.packet.DataPushingPacket;
import com.oodrive.nuage.nbdsrv.packet.DataPushingReplyPacket;
import com.oodrive.nuage.nbdsrv.packet.NbdException;
import com.oodrive.nuage.nbdsrv.packet.OptionCmd;

final class NbdClient {

    private enum Phase {
        IDLE_PHASE, HANDSHAKE_PHASE, DATA_PUSHING_PHASE;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    /** Server address */
    private final InetSocketAddress address;
    /** Handle for the socket */
    private SocketHandle socketHandle;

    /** Export flags received from the server */
    private long exportFlags;
    /** Export Size receive from the server */
    private long exportSize;
    /** Export Name chosen */
    private String exportName;

    /** Global flags of the server */
    private long globalRemoteFlags;

    /** Current Phase */
    private Phase phase;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    NbdClient(final InetSocketAddress address) {
        this.address = address;
        this.phase = Phase.IDLE_PHASE;
    }

    /**
     * Gets the export Flags.
     * 
     * @return the export flags
     */
    final long getExportFlags() {
        return exportFlags;
    }

    /**
     * Sets the export flags.
     * 
     * @param exportFlags
     *            the flags to set
     */
    final void setExportFlags(final long exportFlags) {
        this.exportFlags = exportFlags;
    }

    /**
     * Gets the export size.
     * 
     * @return the export size
     */
    final long getExportSize() {
        return exportSize;
    }

    /**
     * Sets the export size.
     * 
     * @param exportSize
     *            the size to set
     */
    final void setExportSize(final long exportSize) {
        this.exportSize = exportSize;
    }

    /**
     * Gets the export name chosen by the client for this connection.
     * 
     * @return a String representing the name of this export
     */
    final String getExportName() {
        return this.exportName;
    }

    /**
     * Set the export name chosen by the client to use for this connection.
     * 
     * @param name
     *            represents the name of this export
     */
    final void setExportName(final String name) {
        this.exportName = name;
    }

    /**
     * Gets the export remote flags.
     * 
     * @return the flags of the remote export
     */
    final long getGlobalRemoteFlags() {
        return globalRemoteFlags;
    }

    /**
     * Sets the global remote flags.
     * 
     * @param globalRemoteFlags
     *            the flags to set
     */
    final void setGlobalRemoteFlags(final long globalRemoteFlags) {
        this.globalRemoteFlags = globalRemoteFlags;
    }

    /**
     * Close the client.
     */
    final void close() {
        // Reset client info
        phase = Phase.IDLE_PHASE;
        setExportName("");
        setExportSize(0);
        setExportFlags(0);

        try {
            if (socketHandle != null) {
                socketHandle.close();
            }
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to close socket", t);
        }
    }

    /**
     * Start the handshake.
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void handshake() throws IOException, InterruptedException, NbdException {

        if (phase != Phase.IDLE_PHASE) {
            throw new NbdException("Client not in a idle phase");
        }
        final SocketChannel clientSocketChannel = SocketChannel.open();
        try {
            clientSocketChannel.configureBlocking(true);
            clientSocketChannel.connect(address);
            clientSocketChannel.socket().setTcpNoDelay(true);

            socketHandle = new SocketHandle(clientSocketChannel, null);

            phase = Phase.HANDSHAKE_PHASE;
            final Future<Void> future = executor.submit(new InitTask(this));
            future.get();
        }
        catch (final IOException e) {
            close();
            throw e;
        }
        catch (final InterruptedException e) {
            close();
            throw e;
        }
        catch (final ExecutionException e) {
            close();
            LOGGER.error("Execution exception", e.getCause());
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }
        }
    }

    /**
     * Gets the list of the available exports during the handshake phase.
     * 
     * @return An array with the name of the exports
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final String[] handshakeGetList() throws IOException, InterruptedException, NbdException {
        return doOptionNegociation(OptionCmd.NBD_OPT_LIST, null);
    }

    /**
     * Connect client to an export during the handshake phase. No other options (getList or abortHandshake) can be used
     * after that. This ends the handshake phase.
     * 
     * @param name
     *            the name of the export to connect the client
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void handshakeExportName(final String exportName) throws IOException, InterruptedException, NbdException {
        doOptionNegociation(OptionCmd.NBD_OPT_EXPORT_NAME, exportName);
    }

    /**
     * Abort the handshake.
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void handshakeAbort() throws IOException, InterruptedException, NbdException {
        doOptionNegociation(OptionCmd.NBD_OPT_ABORT, null);
    }

    /**
     * Send an option to server during the handshake phase.
     * 
     * @param cmd
     *            the option to send
     * @param data
     *            the data to the send
     * @return an array with the eventual answer from the server
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    private final String[] doOptionNegociation(final OptionCmd cmd, final String data) throws IOException,
            InterruptedException, NbdException {

        if (phase != Phase.HANDSHAKE_PHASE) {
            throw new NbdException("Client not in a handshake phase");
        }
        final Future<String[]> future = executor.submit(new OptionNegotiationTask(this, cmd, data));
        try {
            final String[] result = future.get();
            if (cmd == OptionCmd.NBD_OPT_EXPORT_NAME) {
                phase = Phase.DATA_PUSHING_PHASE;
            }
            return result;
        }
        catch (final InterruptedException e) {
            close();
            throw e;
        }
        catch (final ExecutionException e) {
            LOGGER.error("Execution exception", e.getCause());
            close();
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }
        }
        return EMPTY_STRING_ARRAY;
    }

    /**
     * Send a write request to the server if the client is in data pushing phase.
     * 
     * @param buffer
     *            the buffer to transmit. The position and limit must be set correctly: the writing is done from the
     *            buffer.position() to the buffer.limit(). Then the position is reset to 0.
     * @param offset
     *            the first byte to write in the server export
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void writeRequest(final ByteBuffer src, final long offset) throws IOException, InterruptedException,
            NbdException {

        if (phase != Phase.DATA_PUSHING_PHASE) {
            throw new NbdException("Client not in a data pushing phase");
        }
        final Future<Boolean> future = executor.submit(new WriteTask(this, src, offset));
        try {
            future.get();
        }
        catch (final InterruptedException e) {
            close();
            throw e;
        }
        catch (final ExecutionException e) {
            LOGGER.error("Execution exception", e.getCause());
            close();
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }
        }
    }

    /**
     * Send a read request to the server if the client is in data pushing phase.
     * 
     * @param buf
     *            the buffer to receive the data. Position and limit must set correctly : the reading is done from the
     *            buffer.position() to the buffer.limit(). Then the position is reset to 0.
     * @param offset
     *            the position of the first byte to read in a server export
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void readRequest(final ByteBuffer dst, final long offset) throws IOException, NbdException,
            InterruptedException {

        if (phase != Phase.DATA_PUSHING_PHASE) {
            throw new NbdException("Client not in a data pushing phase");
        }
        final Future<Boolean> future = executor.submit(new ReadTask(this, dst, offset));
        try {
            future.get();
        }
        catch (final InterruptedException e) {
            close();
            throw e;
        }
        catch (final ExecutionException e) {
            close();
            LOGGER.error("Execution exception", e.getCause());
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }

        }
    }

    /**
     * Send a trim request to the server if the client is in data pushing phase.
     * 
     * @param offset
     *            the first byte to discard
     * @param length
     *            the number of bytes to discard
     * @throws NbdException
     * @throws InterruptedException
     * @throws IOException
     */
    public final void trimRequest(final long offset, final int length) throws NbdException, InterruptedException,
            IOException {
        if (phase != Phase.DATA_PUSHING_PHASE) {
            throw new NbdException("Client not in a data pushing phase");
        }
        final Future<Boolean> future = executor.submit(new TrimTask(this, offset, length));
        try {
            future.get();
        }
        catch (final InterruptedException e) {
            close();
            throw e;
        }
        catch (final ExecutionException e) {
            close();
            LOGGER.error("Execution exception", e.getCause());
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }
        }
    }

    /**
     * Disconnect the client in a data pushing phase.
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    final void disconnectRequest() throws IOException, NbdException, InterruptedException {
        if (phase != Phase.DATA_PUSHING_PHASE) {
            throw new NbdException("Client not in a data pushing phase");
        }
        final Future<Boolean> future = executor.submit(new DisconnectTask(this));
        try {
            future.get();
        }
        catch (final InterruptedException e) {
            socketHandle.close();
            throw e;
        }
        catch (final ExecutionException e) {
            LOGGER.error("Execution exception", e.getCause());
            close();
            final Throwable t = e.getCause();
            if (t instanceof NbdException) {
                throw (NbdException) t;
            }
            else if (t instanceof IOException) {
                throw (IOException) t;
            }

        }
    }

    /**
     * Write data into the socket up to the limit of each buffer.
     * 
     * @param src
     *            an array of {@link ByteBuffer} which contain the data
     * @throws IOException
     *             If an I/O error occurs
     */
    final long writeSocket(final ByteBuffer[] src) throws IOException {
        return socketHandle.write(src);
    }

    /**
     * Write data into the socket up to the limit of the buffer.
     * 
     * @param src
     *            a {@link ByteBuffer} which contains the data
     * @throws IOException
     *             If an I/O error occurs
     */
    final long writeSocket(final ByteBuffer src) throws IOException {
        return socketHandle.write(src);
    }

    /**
     * Write data into the socket up to the limit of each buffer.
     * 
     * @param dst
     *            a {@link ByteBuffer} to fill with received data
     * @throws IOException
     *             If an I/O error occurs
     */
    final long readSocket(final ByteBuffer[] dst) throws IOException {
        return socketHandle.read(dst);
    }

    /**
     * Read data in the socket and fill the buffer up to its limit.
     * 
     * @param dst
     *            a {@link ByteBuffer} to fill with received data
     * @throws IOException
     *             If an I/O error occurs
     */
    final long readSocket(final ByteBuffer dst) throws IOException {
        return socketHandle.read(dst);
    }

    /**
     * Read with a too long parameter in length (higher than an signed integer)
     * 
     * @param length
     * @param offset
     * @throws IOException
     * @throws NbdException
     */
    public void readTooLong() throws IOException, NbdException {
        // Send write request
        final DataPushingPacket dataPushingPacket = new DataPushingPacket(DataPushingPacket.MAGIC,
                DataPushingCmd.NBD_CMD_READ, 0xFFFFFFF, 0, 2147483648L);

        final ByteBuffer header = DataPushingPacket.serialize(dataPushingPacket);
        try {
            writeSocket(header);
        }
        finally {
            DataPushingPacket.release(header);
        }

        // Wait Answer
        LOGGER.debug("Read Data Pushing Reply");

        final ByteBuffer reply = DataPushingReplyPacket.allocateHeader();

        try {
            readSocket(reply);
            DataPushingReplyPacket.deserialize(reply);
        }
        finally {
            DataPushingReplyPacket.release(reply);
        }

    }
}
