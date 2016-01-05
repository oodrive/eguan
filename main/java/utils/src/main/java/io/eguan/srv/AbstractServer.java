package io.eguan.srv;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.management.AttributeChangeNotification;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for a server of devices. The server binds on a given TCP/IP address and port. The managed devices must
 * implement the interface
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 * @param <S>
 *            TCP/IP server, implementing some protocol to access to the device. The main loop must be implemented as a
 *            {@link Callable}.
 * @param <T>
 *            target representing a device.
 */
public abstract class AbstractServer<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig>
        implements NotificationEmitter, AbstractServerMXBean {

    /** Package logger */
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractServer.class.getPackage().getName());

    // Control timeout to wait for the server start or end
    private static final int LOOP_COUNTER_LIMIT = 100;
    private static final int LOOP_WAIT_TIME = 100;

    private static final Comparator<String> IGNORECASE_COMPARATOR = new Comparator<String>() {
        @Override
        public final int compare(final String s1, final String s2) {
            return s1.compareToIgnoreCase(s2);
        }
    };

    /** Server config */
    private volatile K serverConfig;
    /** Started server config */
    private volatile K serverConfigStarted;

    /** Executor to run the server */
    private final ExecutorService serverExecutor;
    /** Config lock. Share access to target list and config */
    private final ReadWriteLock serverLock = new ReentrantReadWriteLock();
    /** Current server future task or <code>null</code> */
    @GuardedBy(value = "serverLock")
    private Future<Void> serverFuture;
    /** Current targets. The key is the TargetName and the value is the target. */
    @GuardedBy(value = "serverLock")
    private final Map<String, T> targets = new TreeMap<String, T>(IGNORECASE_COMPARATOR);
    /** JMX notifications support */
    private final NotificationBroadcasterSupport notificationEmitter = new NotificationBroadcasterSupport();
    /** JMX notification sequence number */
    private final AtomicInteger notificationSequenceNumber = new AtomicInteger(1);

    /**
     * Create a new server that will bind on the given address and port.
     * 
     * @param displayName
     *            display name of protocol or server
     */
    protected AbstractServer(@Nonnull final K serverConfig, final String displayName) {

        this.serverConfig = serverConfig;

        this.serverExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
            /**
             * Create a non daemon thread to run the server.
             * 
             * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
             */
            @Override
            public final Thread newThread(final Runnable r) {
                final Thread thread = new Thread(r, displayName + " server");
                thread.setDaemon(true);
                thread.setPriority(Thread.NORM_PRIORITY + 2);
                return thread;
            }
        });
    }

    /**
     * Gets the current server config
     * 
     * @return the current config
     */
    protected final K getServerConfig() {
        return serverConfig;
    }

    /**
     * Gets the server bind address.
     * 
     * @return the bind address.
     */
    public final InetAddress getInetAddress() {
        return serverConfig.getAddress();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#getAddress()
     */
    @Override
    public final String getAddress() {
        return serverConfig.getAddress().getHostAddress();
    }

    /**
     * Sets the server bind address. Will be taken into account during the next server start.
     * 
     * @param address
     *            new server bind address.
     * @throws NullPointerException
     *             if address is <code>null</code>
     * @throws ServerConfigurationException
     *             if address is invalid (not a local address)
     */
    public final void setAddress(@Nonnull final InetAddress address) throws NullPointerException,
            ServerConfigurationException {
        setAddress(address, true);
    }

    private final void setAddress(@Nonnull final InetAddress address, final boolean notify)
            throws NullPointerException, ServerConfigurationException {
        final InetAddress addressOld = this.serverConfig.getAddress();
        this.serverConfig.setAddress(address);

        // Send JMX notification
        if (notify) {
            final Notification n = new AttributeChangeNotification(this, notificationSequenceNumber.getAndIncrement(),
                    System.currentTimeMillis(), "Address changed", "Address", "InetAddress", addressOld, address);
            notificationEmitter.sendNotification(n);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#setAddress(java.lang.String)
     */
    @Override
    public final void setAddress(@Nonnull final String address) throws UnknownHostException {
        Objects.requireNonNull(address);
        setAddress(InetAddress.getByName(address));
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#getPort()
     */
    @Override
    public final int getPort() {
        return serverConfig.getPort();
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#setPort(int)
     */
    @Override
    public final void setPort(final int port) throws ServerConfigurationException {
        setPort(port, true);
    }

    private final void setPort(final int port, final boolean notify) throws ServerConfigurationException {

        final int oldPort = this.serverConfig.getPort();
        this.serverConfig.setPort(port);

        // Send JMX notification
        if (notify) {
            final Notification n = new AttributeChangeNotification(this, notificationSequenceNumber.getAndIncrement(),
                    System.currentTimeMillis(), "Port changed", "Port", "int", Integer.valueOf(oldPort),
                    Integer.valueOf(port));
            notificationEmitter.sendNotification(n);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#isRestartRequired
     */
    @Override
    public final boolean isRestartRequired() {
        return serverConfigStarted != null && !getServerConfig().equals(serverConfigStarted);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#start()
     */
    @SuppressWarnings("unchecked")
    @Override
    public final void start() throws IllegalStateException {
        final K serverConfigTemp;
        serverLock.writeLock().lock();
        try {
            LOGGER.debug("Starting " + this);
            if (isStarted()) {
                throw new IllegalStateException("Started");
            }
            // Create a new server for the current configuration
            serverConfigTemp = (K) serverConfig.clone();
            final S server = createServer(serverConfigTemp);
            serverFuture = serverExecutor.submit(server);
        }
        finally {
            serverLock.writeLock().unlock();
        }

        // Wait for the server to start: make sure the configuration is loaded
        for (int counter = 0; counter < LOOP_COUNTER_LIMIT; counter++) {
            Thread.yield();
            try {
                Thread.sleep(LOOP_WAIT_TIME);
            }
            catch (final InterruptedException e) {
                // ignored
            }
            serverLock.readLock().lock();
            try {
                if (isServerStarted()) {
                    // Save configuration of the server
                    serverConfigStarted = serverConfigTemp;

                    // Notify start of server
                    final Notification n = new AttributeChangeNotification(this,
                            notificationSequenceNumber.getAndIncrement(), System.currentTimeMillis(),
                            "Started changed", "Started", "boolean", Boolean.FALSE, Boolean.TRUE);
                    notificationEmitter.sendNotification(n);

                    return;
                }
            }
            finally {
                serverLock.readLock().unlock();
            }
        }

        // Not started nor stopped nor aborted!
        LOGGER.warn("Server start: wait timeout");
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#stop()
     */
    @Override
    public final void stop() {
        serverLock.writeLock().lock();
        try {

            LOGGER.debug("Stopping " + toString());
            if (serverFuture != null) {

                try {
                    // Cancel task and server if it's running
                    serverFuture.cancel(false);
                    serverCancel();

                    // Sleep at least once to make sure that the resources (TCP connections)
                    // have been released
                    for (int counter = 0; counter < LOOP_COUNTER_LIMIT; counter++) {
                        Thread.yield();
                        Thread.sleep(LOOP_WAIT_TIME);
                        if (serverFuture.isDone()) {
                            break;
                        }
                    }
                }
                catch (final Exception e) {
                    // Ignored
                    LOGGER.warn("Error while stopping " + toString(), e);
                }
                finally {
                    serverFuture = null;
                }

                serverConfigStarted = null;

                // Notify stop of server
                final Notification n = new AttributeChangeNotification(this,
                        notificationSequenceNumber.getAndIncrement(), System.currentTimeMillis(), "Started changed",
                        "Started", "boolean", Boolean.TRUE, Boolean.FALSE);
                notificationEmitter.sendNotification(n);
            }
            LOGGER.debug("Stopping " + toString() + " done");
        }
        finally {
            serverLock.writeLock().unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.srv.AbstractServerMXBean#isStarted()
     */
    @Override
    public final boolean isStarted() {
        serverLock.readLock().lock();
        try {
            return !(serverFuture == null || serverFuture.isDone());
        }
        finally {
            serverLock.readLock().unlock();
        }
    }

    /**
     * Creates a new server instance.
     * 
     * @return the new server.
     */
    protected abstract S createServer(K serverConfig);

    /**
     * Tells if the server is started.
     * 
     * @return <code>true</code> if the server has entered its main loop.
     */
    protected abstract boolean isServerStarted();

    /**
     * Cancels the server.
     */
    protected abstract void serverCancel();

    /**
     * Notify that the server have been stopped.
     */
    protected abstract void serverStopped();

    /**
     * Add a target device to the server.
     * 
     * @param target
     *            the target to add.
     * @return the previous target that had the same TargetName or <code>null</code>
     */
    public final T addTarget(@Nonnull final T target) {
        serverLock.writeLock().lock();
        try {
            final T prev = targets.put(target.getTargetName(), target);
            targetAdded(target, prev);
            return prev;
        }
        finally {
            serverLock.writeLock().unlock();
        }
    }

    /**
     * Remove a target device from the server.
     * 
     * @param targetName
     *            name of the target to remove
     * @return the removed target or <code>null</code> if no target have this name
     */
    public final T removeTarget(@Nonnull final String targetName) {
        serverLock.writeLock().lock();
        try {
            final T removed = targets.remove(Objects.requireNonNull(targetName));
            targetRemoved(targetName, removed);
            return removed;
        }
        finally {
            serverLock.writeLock().unlock();
        }
    }

    /**
     * Lock to take to read the target map.
     * 
     * @return the lock to read the target map.
     */
    protected final Lock getTargetSharedLock() {
        return serverLock.readLock();
    }

    /**
     * Read-only view of the targets. Must take the target shared lock to avoid a concurrent access from another thread.
     * 
     * @return the targets by name.
     */
    protected final Map<String, T> getTargetMap() {
        return Collections.unmodifiableMap(targets);
    }

    /**
     * Notify that a new target have been added.
     * 
     * @param targetNew
     *            the added target
     * @param targetPrev
     *            the previous target with the same name. May be <code>null</code>
     */
    protected abstract void targetAdded(final T targetNew, final T targetPrev);

    /**
     * Notify that a target have been removed.
     * 
     * @param name
     *            name of the target to remove.
     * @param target
     *            the target found in the local cache (may be <code>null</code>)
     */
    protected abstract void targetRemoved(final String name, final T target);

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#addNotificationListener(javax.management.NotificationListener,
     * javax.management.NotificationFilter, java.lang.Object)
     */
    @Override
    public final void addNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws IllegalArgumentException {
        notificationEmitter.addNotificationListener(listener, filter, handback);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationEmitter#removeNotificationListener(javax.management.NotificationListener,
     * javax.management.NotificationFilter, java.lang.Object)
     */
    @Override
    public final void removeNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws ListenerNotFoundException {
        notificationEmitter.removeNotificationListener(listener, filter, handback);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#removeNotificationListener(javax.management.NotificationListener)
     */
    @Override
    public final void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
        notificationEmitter.removeNotificationListener(listener);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#getNotificationInfo()
     */
    @Override
    public final MBeanNotificationInfo[] getNotificationInfo() {
        final String[] types = new String[] { AttributeChangeNotification.ATTRIBUTE_CHANGE };
        final String name = AttributeChangeNotification.class.getName();
        final String description = "An attribute has changed";
        final MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);
        return new MBeanNotificationInfo[] { info };
    }

    /**
     * Gets notification broadcaster support.
     * 
     * @return the notification emitter
     */
    protected final NotificationBroadcasterSupport getNotificationBroadcasterSupport() {
        return notificationEmitter;
    }

    /**
     * Gets a new sequence number.
     * 
     * @return the new sequence number
     */
    protected final int getNotificationSequenceNumber() {
        return notificationSequenceNumber.getAndIncrement();
    }
}
