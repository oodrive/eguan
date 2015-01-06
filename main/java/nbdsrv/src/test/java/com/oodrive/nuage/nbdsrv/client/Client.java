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

import com.oodrive.nuage.nbdsrv.packet.NbdException;

/**
 * API for the NBD client.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public class Client {

    /** Internal client */
    private final NbdClient nbdClient;

    public Client(final InetSocketAddress address) {
        super();
        this.nbdClient = new NbdClient(address);
    }

    /**
     * Handshake - First requests to connect a client to the NBD server
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    public final void handshake() throws IOException, InterruptedException, NbdException {
        nbdClient.handshake();
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
    public final String[] getList() throws IOException, InterruptedException, NbdException {
        return nbdClient.handshakeGetList();
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
    public final void setExportName(final String name) throws IOException, InterruptedException, NbdException {
        nbdClient.handshakeExportName(name);
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
    public final void abortHandshake() throws IOException, InterruptedException, NbdException {
        nbdClient.handshakeAbort();
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
    public final void read(final ByteBuffer buf, final long offset) throws IOException, NbdException,
            InterruptedException {
        nbdClient.readRequest(buf, offset);
    }

    /**
     * Send a read with too long length to the server if the client is in data pushing phase.
     * 
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    public final void readTooLong() throws IOException, NbdException, InterruptedException {
        nbdClient.readTooLong();
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
    public final void write(final ByteBuffer buffer, final long offset) throws IOException, InterruptedException,
            NbdException {
        nbdClient.writeRequest(buffer, offset);
    }

    /**
     * Send a trim request to the server if the client is in data pushing phase.
     * 
     * @param offset
     *            the first byte to discard
     * @param length
     *            the number of bytes to discard
     * @throws IOException
     *             If an I/O error occurs
     * @throws InterruptedException
     *             If the current thread was interrupted
     * @throws NbdException
     *             If the NBD protocol is not respected
     */
    public void trim(final long offset, final int length) throws NbdException, InterruptedException, IOException {
        nbdClient.trimRequest(offset, length);
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
    public final void disconnect() throws IOException, NbdException, InterruptedException {
        nbdClient.disconnectRequest();
    }

    /**
     * Get the connected export name.
     * 
     * @return the export name
     */
    public final String getExportName() {
        return nbdClient.getExportName();
    }

    /**
     * Get connected export size
     * 
     * @return the size of the export
     */
    public long getExportSize() {
        return nbdClient.getExportSize();
    }

    /**
     * Get connected export flags
     * 
     * @return the flags of the export
     */
    public long getExportFlags() {
        return nbdClient.getExportFlags();
    }
}
