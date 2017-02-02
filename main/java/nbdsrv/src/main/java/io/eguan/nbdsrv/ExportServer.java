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

import io.eguan.nbdsrv.NbdServer.NbdConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the server which contains the exports.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
final class ExportServer implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportServer.class);

    private static final ClientConnection[] EMPTY_TARGET_CONNECTION_ARRAY = new ClientConnection[0];

    /** Contains all the registered {@link NbdExport}s. */
    private final Map<String, NbdExport> exports = new TreeMap<>(IGNORECASE_COMPARATOR);
    private final ReadWriteLock exportsLock = new ReentrantReadWriteLock();

    private static final Comparator<String> IGNORECASE_COMPARATOR = new Comparator<String>() {
        @Override
        public final int compare(final String s1, final String s2) {
            return s1.compareToIgnoreCase(s2);
        }
    };

    private static final String[] EMPTY_EXPORT_NAME_ARRAY = new String[0];

    /** A {@link SocketChannel} used for listening to incoming connections. */
    private ServerSocketChannel modernServerSocketChannel;

    /** A {@link Selector} used for listening to incoming connections and reading incoming datas */
    private Selector selector;

    /** Mark the server as cancelled (atomic access to selector) */
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /** Contains all active {@link ClientConnection}s. */
    private final Map<SocketChannel, ClientConnection> connections = new HashMap<>();

    ExportServer(final NbdConfiguration conf) {
        this.config = conf;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Starting NBD-export srver: ");
            LOGGER.debug("   port:           " + getConfig().getPort());
            LOGGER.debug("   loading exports.");
        }

        exportsLock.writeLock().lock();
        try {
            final List<NbdExport> targetInfo = getConfig().getTargets();
            for (final NbdExport curTargetInfo : targetInfo) {

                exports.put(curTargetInfo.getTargetName(), curTargetInfo);
                LOGGER.debug("   target name:    " + curTargetInfo.getTargetName() + " loaded.");
            }
        }
        finally {
            exportsLock.writeLock().unlock();
        }
    }

    /**
     * The NBD Export's global parameters.
     */
    private final NbdConfiguration config;

    public final NbdConfiguration getConfig() {
        return config;
    }

    /**
     * Tells if trim is enabled or not.
     * 
     * @return true if the trim is enabled, false otherwise
     */
    public final boolean isTrimEnabled() {
        return config.isTrimEnabled();
    }

    @Override
    public final Void call() throws Exception {

        final ExecutorService threadPool = Executors.newFixedThreadPool(4);
        try {
            modernServerSocketChannel = ServerSocketChannel.open();
            try {
                modernServerSocketChannel.configureBlocking(false);
                final InetSocketAddress modernAddress = new InetSocketAddress(
                        getConfig().getTargetAddressInetAddress(), getConfig().getPort());
                modernServerSocketChannel.socket().bind(modernAddress);

                LOGGER.debug("Server started ...");

                synchronized (this) {
                    if (cancelled.get()) {
                        return null;
                    }
                    selector = Selector.open();
                }
                try {
                    final SelectionKey acceptableKey = modernServerSocketChannel.register(selector,
                            SelectionKey.OP_ACCEPT);

                    while (selector.select() >= 0 && !cancelled.get()) {
                        final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                        while (keys.hasNext()) {
                            final SelectionKey key = keys.next();
                            keys.remove();

                            // Event for a connection on the server socket
                            if (key.isValid()) {
                                if (key.isAcceptable()) {
                                    assert key == acceptableKey;
                                    LOGGER.debug("accept");
                                    // Get the socket channel
                                    final ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();

                                    // Accept the client connection
                                    final SocketChannel clientSocketChannel = serverChannel.accept();
                                    try {
                                        // Configure the new client socket
                                        final SocketHandle socketHandle = new SocketHandle(clientSocketChannel,
                                                selector);
                                        socketHandle.configure();

                                        if (LOGGER.isDebugEnabled()) {
                                            LOGGER.debug("connection from a client, new socket : "
                                                    + clientSocketChannel.socket());
                                        }

                                        // Create a new client connection
                                        final ClientConnection connection = new ClientConnection(socketHandle, this,
                                                serverChannel.equals(modernServerSocketChannel));

                                        // Save the connection
                                        addConnection(clientSocketChannel, connection);

                                        // Execute first phase
                                        threadPool.submit(connection.getPhase());
                                    }
                                    catch (final Exception e) {
                                        LOGGER.warn("Unexpected exception", e);
                                        removeConnection(clientSocketChannel);
                                        clientSocketChannel.close();
                                        continue;
                                    }
                                }
                                // event for the reception on a client socket
                                else {
                                    try {
                                        final SocketChannel clientSocketChannel = (SocketChannel) key.channel();
                                        // Get the corresponding connection and execute the current phase
                                        final ClientConnection connection = getConnection(clientSocketChannel);
                                        if (connection != null) {
                                            if (connection.isReadable()) {
                                                threadPool.submit(connection.getPhase());
                                            }
                                        }
                                        else {
                                            LOGGER.warn("Connection can not be retrieved: "
                                                    + clientSocketChannel.socket());
                                            clientSocketChannel.close();
                                            continue;
                                        }
                                    }
                                    catch (final Exception e) {
                                        LOGGER.warn("Unexpected exception", e);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
                finally {
                    selector.close();
                    synchronized (this) {
                        selector = null;
                    }
                }
            }
            finally {
                modernServerSocketChannel.close();
            }
        }
        catch (final AsynchronousCloseException e) {
            // this block is entered if the server socket is close by cancel()
            LOGGER.debug("Throws Exception", e);
        }
        catch (final IOException e) {
            // this block is entered if the desired port is already in use
            LOGGER.error("Throws Exception", e);
        }
        finally {
            threadPool.shutdownNow();
        }
        return null;
    }

    /**
     * Add a connection.
     * 
     * @param socketChannel
     *            the {@link SocketChannel} corresponding to the connection
     * 
     * @param connection
     *            the {@link ClientConnection}
     * 
     */
    private final void addConnection(final SocketChannel socketChannel, final ClientConnection connection) {
        synchronized (connections) {
            connections.put(socketChannel, connection);
        }
    }

    /**
     * Gets a connection.
     * 
     * @param socketChannel
     *            the {@link SocketChannel} corresponding to the connection
     * 
     */
    private final ClientConnection getConnection(final SocketChannel socketChannel) {
        synchronized (connections) {
            return connections.get(socketChannel);
        }
    }

    /**
     * Remove a connection.
     * 
     * @param socketChannel
     *            the {@link SocketChannel} corresponding to the connection
     * 
     */
    final void removeConnection(final SocketChannel socketChannel) {
        synchronized (connections) {
            connections.remove(socketChannel);
        }
    }

    /**
     * Cancel the server.
     * 
     */
    final void cancel() {
        final Selector selectorTmp;
        LOGGER.debug("Cancel server");
        synchronized (this) {
            cancelled.set(true);
            selectorTmp = selector;
        }
        if (selectorTmp != null) {
            try {
                selectorTmp.wakeup();
            }
            catch (final Exception e) {
                // Already closed?
                LOGGER.debug("Throws Exception", e);
            }
            // Close the sessions - take a snapshot of the session list
            // to avoid a concurrent access during the close of the sessions
            final ClientConnection[] connectionsTmp;
            synchronized (connections) {
                connectionsTmp = connections.values().toArray(EMPTY_TARGET_CONNECTION_ARRAY);
            }
            for (int i = 0; i < connectionsTmp.length; i++) {
                connectionsTmp[i].close();
            }
        }

    }

    /**
     * Add an export.
     * 
     * @param export
     *            the {@link NbdExport} to add.
     * @return the previous export- May be null
     * 
     * @throws IOException
     * 
     */
    final NbdExport addExport(final NbdExport export) {
        exportsLock.writeLock().lock();
        try {
            return exports.put(export.getTargetName(), export);
        }
        finally {
            exportsLock.writeLock().unlock();
        }
    }

    /**
     * Remove an export.
     * 
     * @param exportName
     *            the name of the {@link NbdExport} to remove
     * @throws IOException
     * 
     */
    final NbdExport removeExport(final String exportName) {

        final NbdExport prev;
        exportsLock.writeLock().lock();
        try {
            prev = exports.remove(exportName);
        }
        finally {
            exportsLock.writeLock().unlock();
        }
        if (prev == null) {
            // Nothing removed
            return null;
        }
        LOGGER.debug("Target name: " + exportName + " removed");

        // Close sessions: get a snapshot of the session list (do
        // not lock the connection list during the close of the connections)
        final ClientConnection[] connectionsTmp;
        synchronized (connections) {
            connectionsTmp = connections.values().toArray(EMPTY_TARGET_CONNECTION_ARRAY);
        }
        for (int i = 0; i < connectionsTmp.length; i++) {
            final ClientConnection connection = connectionsTmp[i];
            final String connectionExportName = connection.getExportName();
            if (exportName.equalsIgnoreCase(connectionExportName)) {
                connection.close();
            }
        }
        return prev;
    }

    /**
     * Gets the current export list.
     * 
     * @return an array of export name
     * 
     */
    final String[] getExportList() {
        exportsLock.readLock().lock();
        try {
            return exports.keySet().toArray(EMPTY_EXPORT_NAME_ARRAY);
        }
        finally {
            exportsLock.readLock().unlock();
        }
    }

    /**
     * Gets the device corresponding to a given name.
     * 
     * @param exportName
     *            the name of the export.
     * 
     */
    final NbdDevice getDevice(final String exportName) {
        NbdExport export;
        exportsLock.readLock().lock();
        try {
            export = exports.get(exportName);
            if (export == null) {
                throw new IllegalArgumentException("Unknown export name '" + exportName + "'");
            }
            return export.getDevice();
        }
        finally {
            exportsLock.readLock().unlock();
        }
    }

    /**
     * Get a snapshot of the current list of sessions.
     * 
     * @return the list of the exports
     */
    public final List<NbdExportStats> getTargetStats() {
        //
        exportsLock.readLock().lock();
        try {
            final List<NbdExportStats> result = new ArrayList<>(exports.size());
            for (final Iterator<NbdExport> iterator = exports.values().iterator(); iterator.hasNext();) {
                final NbdExport target = iterator.next();
                final String targetName = target.getTargetName();
                // Look for connections associated to that target
                final int count = getConnectionsCount(targetName);
                final NbdExportStats stats = new NbdExportStats(targetName, count, target.getSize(),
                        target.isReadOnly());

                result.add(stats);
            }
            return result;
        }
        finally {
            exportsLock.readLock().unlock();
        }
    }

    /**
     * Gets the number of connection on a target.
     * 
     * @param targetName
     *            the target name
     * @return the number of connection
     */
    private final int getConnectionsCount(final String targetName) {
        final ClientConnection[] connectionsTmp;
        synchronized (connections) {
            connectionsTmp = connections.values().toArray(EMPTY_TARGET_CONNECTION_ARRAY);
        }
        int count = 0;
        for (final ClientConnection connection : connectionsTmp) {
            final String exportName = connection.getExportName();
            if (exportName == null) {
                continue;
            }
            if (targetName.equalsIgnoreCase(exportName)) {
                count++;
            }
        }
        return count;
    }

}
