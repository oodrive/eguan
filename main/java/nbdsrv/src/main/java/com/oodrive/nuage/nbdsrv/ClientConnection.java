package com.oodrive.nuage.nbdsrv;

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
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nbdsrv.packet.NbdException;

/**
 * Represents a client connection. Handle the device and the socket.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class ClientConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnection.class);

    /** The Export Server */
    private final ExportServer server;

    /** A handle on the client socket */
    private final SocketHandle socketHandle;

    /** The current phase */
    private PhaseAbstract phase;

    /** The remote flags set by the client */
    private long remoteFlags;
    /** The name of the export chosen by the client */
    private String exportName;
    /** The device on which the client is connected */
    private NbdDevice device;

    ClientConnection(final SocketHandle socketHandle, final ExportServer server, final boolean isModern) {
        this.socketHandle = socketHandle;
        this.phase = new HandshakePhase(this);
        this.server = server;
    }

    /**
     * Gets the current phase.
     * 
     * @return an {@link PhaseAbstract}
     */
    final PhaseAbstract getPhase() {
        return phase;
    }

    /**
     * Set the new phase.
     * 
     * @param phase
     *            the new {@link PhaseAbstract}
     */
    final void setPhase(final PhaseAbstract phase) {
        this.phase = phase;
    }

    /**
     * Gets the remote flags.
     * 
     * @return a long corresponding to the flags
     */
    final long getRemoteFlags() {
        return this.remoteFlags;
    }

    /**
     * Set the remote flags.
     * 
     * @param flags
     *            a long corresponding to the new flags
     */
    final void setRemoteFlags(final long flags) {
        this.remoteFlags = flags;
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
     * Gets the device for this connection.
     * 
     * @return a {@link NbdDevice}
     */
    final NbdDevice getNbdDevice() {
        return device;
    }

    /**
     * Set the device for this connection.
     * 
     * @param name
     *            the name of the device
     */
    final void setNbdDevice(final String name) {
        this.device = server.getDevice(name);
    }

    /**
     * Write data into the socket up to the limit of each buffer.
     * 
     * @param src
     *            an array of {@link ByteBuffer} which contain the data
     */
    final long write(final ByteBuffer[] src) throws IOException {
        return socketHandle.write(src);
    }

    /**
     * Write data into the socket up to the limit of the buffer.
     * 
     * @param src
     *            a {@link ByteBuffer} which contains the data
     */
    final long write(final ByteBuffer src) throws IOException {
        return socketHandle.write(src);
    }

    /**
     * Read data in the socket and fill the buffer up to its limit.
     * 
     * @param dst
     *            a {@link ByteBuffer} to fill with received data
     */
    final long read(final ByteBuffer dst) throws IOException {
        return socketHandle.read(dst);
    }

    /**
     * Close the connection.
     * 
     */
    final void close() {
        LOGGER.debug("Close connection");
        try {
            socketHandle.close();
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to close socket", t);
        }
        try {
            server.removeConnection(socketHandle.getSocket());
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to remove client connection", t);
        }
        try {
            socketHandle.cancelKey();
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to cancel selected key", t);
        }
    }

    /**
     * Get the address of the connected client.
     * 
     * @return a {@link String} representing the remote address
     */
    final String getRemoteAddress() {
        return socketHandle.getRemoteAddress();
    }

    /**
     * Tells if the socket is readable and disable read.
     * 
     * @return true if the socket is readable
     * 
     */
    final boolean isReadable() {
        return socketHandle.isReadable();
    }

    /**
     * Enable the ability to receive data on the socket.
     * 
     */
    final void enableRead() {
        socketHandle.enableRead();
    }

    /**
     * Gets the export size.
     * 
     * @return the export size
     */
    public final long getExportSize() throws NbdException {
        if (device == null) {
            throw new NbdException("Unknown Export : Export Name option has not been set");
        }
        return device.getSize();
    }

    /**
     * Tells of the export is read-only.
     * 
     * @return <code>true</code> if the export is read-only
     */
    public final boolean isExportReadOnly() throws NbdException {
        if (device == null) {
            throw new NbdException("Unknown Export : Export Name option has not been set");
        }
        return device.isReadOnly();
    }

    /**
     * Gets the list of the exports.
     * 
     * @return an array of string representing the name of all the exports
     */
    public final String[] getExportList() {
        return server.getExportList();
    }

    /**
     * Tells if trim is enabled or not.
     * 
     * @return <code>true</code> if trim is enabled, <code>false</code> otherwise
     */
    public final boolean isTrimEnabled() {
        return server.isTrimEnabled();
    }

}
