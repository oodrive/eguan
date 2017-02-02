package io.eguan.nbdsrv;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle the client socket and its selection key.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class SocketHandle {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketHandle.class);

    /** The current socket used for this client */
    private final SocketChannel socket;
    /** The Selector used in the select() of the ExportServer */
    private final Selector selector;
    /** The selection key corresponding to the socket */
    @GuardedBy(value = "keyLock")
    private SelectionKey selectionKey;
    private final Lock keyLock = new ReentrantLock();

    public SocketHandle(final SocketChannel socketChannel, final Selector selector) {
        this.socket = socketChannel;
        this.selector = selector;
    }

    /**
     * Gets the socket.
     * 
     * @return the socket
     */
    public final SocketChannel getSocket() {
        return socket;
    }

    /**
     * Configure the socket blocking, with tcp no delay and register it to the selector.
     * 
     */
    public final void configure() throws IOException {
        socket.configureBlocking(false);
        socket.socket().setTcpNoDelay(true);

        // Add the client socket channel to the selector pool but do not activate read key
        // Server send request first
        keyLock.lock();
        try {
            this.selectionKey = socket.register(selector, 0);
        }
        finally {
            keyLock.unlock();
        }
    }

    /**
     * Tells if the socket is readable and disable read.
     * 
     * @return true if the socket is readable
     */
    public final boolean isReadable() {
        keyLock.lock();
        try {
            if (selectionKey == null) {
                return false;
            }
            final boolean readable = selectionKey.isReadable();
            if (readable) {
                int opsMask = selectionKey.interestOps();
                opsMask &= ~(SelectionKey.OP_READ);
                selectionKey.interestOps(opsMask);

                LOGGER.trace("Disable Read");
            }
            return readable;
        }
        finally {
            keyLock.unlock();
        }
    }

    /**
     * Enable reception on the socket.
     * 
     */
    public final void enableRead() {
        keyLock.lock();
        try {
            if (selectionKey != null) {
                int opsMask = selectionKey.interestOps();
                opsMask |= (SelectionKey.OP_READ);
                selectionKey.interestOps(opsMask);

                LOGGER.trace("Enable Read");

                // Wake up the main thread blocked in the select
                selector.wakeup();
            }
        }
        finally {
            keyLock.unlock();
        }
    }

    /**
     * Cancel the selection key corresponding to the socket.
     * 
     */
    public final void cancelKey() {
        keyLock.lock();
        try {
            if (selectionKey != null) {
                selectionKey.cancel();
                selectionKey = null;
            }
        }
        finally {
            keyLock.unlock();
        }
    }

    /**
     * Close the socket.
     * 
     */
    public final void close() throws IOException {
        socket.close();
    }

    /**
     * Read data from the socket and fill the buffer, up to the limit of the buffer.
     * 
     * @param dst
     *            The{@link ByteBuffer} to fill with the data
     * 
     */
    public final long read(final ByteBuffer dst) throws IOException, ClosedChannelException {
        long len = 0;
        while (dst.hasRemaining()) {
            final int read = socket.read(dst);
            if (read == -1) {
                throw new ClosedChannelException();
            }
            len += read;
        }
        dst.flip();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Total read=" + len);
        }
        return len;
    }

    /**
     * Write data in the socket up to the limit of the buffer.
     * 
     * @param src
     *            the {@link ByteBuffer} which contains the data
     * 
     */
    public final long write(final ByteBuffer src) throws IOException {
        long length = 0;
        while (src.hasRemaining()) {
            final long res = socket.write(src);
            length += res;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Total write=" + length);
        }
        return length;
    }

    /**
     * Write data in the socket up to the limit of each buffer.
     * 
     * @param src
     *            an array with the {@link ByteBuffer} which contains the data
     * 
     */
    public final long write(final ByteBuffer[] src) throws IOException {

        long expected = 0;
        for (final ByteBuffer buf : src) {
            expected += buf.limit();
        }
        long write = 0;
        while (write < expected) {
            write += socket.write(src);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Total write=" + write);
        }
        return write;
    }

    /**
     * read data in the socket up to the limit of each buffer.
     * 
     * @param src
     *            an array with the {@link ByteBuffer} which contains the data
     * 
     */
    public final long read(final ByteBuffer[] src) throws IOException {

        long expected = 0;
        for (final ByteBuffer buf : src) {
            expected += buf.limit();
        }
        long read = 0;
        while (read < expected) {
            read += socket.read(src);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Total read=" + read);
        }
        for (final ByteBuffer buf : src) {
            buf.flip();
        }
        return read;
    }

    /**
     * Gets the address of the connected client.
     * 
     * @return a String which represents the address of the remote. <unknown> if not found
     * 
     */
    public final String getRemoteAddress() {
        try {
            return socket.getRemoteAddress().toString();
        }
        catch (final Throwable t) {
            return "<unknown>";
        }
    }
}
