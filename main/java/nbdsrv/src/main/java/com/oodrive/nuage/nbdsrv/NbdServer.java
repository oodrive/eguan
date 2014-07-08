package com.oodrive.nuage.nbdsrv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.management.AttributeChangeNotification;
import javax.management.Notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.srv.AbstractServer;

/**
 * NBD server. The server waits for incoming connections on the configured address and port. The list of targets is
 * dynamic.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class NbdServer extends AbstractServer<ExportServer, NbdExport, NbdServerConfig> implements
        NbdServerMXBean {

    /**
     * Nbd configuration.
     * 
     */
    static final class NbdConfiguration {

        /** Associated server. Needed to get the list of targets */
        private final NbdServer server;
        /** Bind port. Keep here the original value for the getter */
        private final int port;
        /** Bind address. Keep here the original value for the getter */
        private final InetAddress address;
        /** Trim state */
        private final boolean trimEnabled;
        /** toString does not change */
        private final String toStr;
        /** Flag to tell that the configuration have been loaded */
        private final AtomicBoolean loaded = new AtomicBoolean(false);

        NbdConfiguration(final NbdServer server, final int port, final InetAddress address, final boolean trimEnabled) {
            this.server = server;
            this.port = port;
            this.address = address;
            this.trimEnabled = trimEnabled;
            this.toStr = "NbdConfiguration[" + address.getHostAddress() + ":" + port + ",trim=" + trimEnabled + "]";
        }

        /**
         * Tells if the configuration have been loaded.
         * 
         * @return <code>true</code> if the Nbd server is started and has read the configuration.
         */
        final boolean isLoaded() {
            return loaded.get();
        }

        /**
         * Get the loaded targets.
         * 
         * @return the list of the NBD exports
         */
        public final List<NbdExport> getTargets() {
            loaded.set(true);
            return server.getInnerTargets();
        }

        /**
         * Gets the port.
         * 
         * @return the port
         */
        public final int getPort() {
            return port;
        }

        /**
         * Get the address.
         * 
         * @return the {@link InetAddress}
         */
        public final InetAddress getTargetAddressInetAddress() {
            return address;
        }

        /**
         * Tells if trim is enabled.
         * 
         * @return <code>true</code> if trim is enabled, <code>false</code> otherwise
         */
        public final boolean isTrimEnabled() {
            return trimEnabled;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public final String toString() {
            return toStr;
        }

    }

    /** Package logger */
    static final Logger LOGGER = LoggerFactory.getLogger(NbdServer.class.getPackage().getName());

    /** Default port for the new protocol version */
    static final int DEFAULT_NBD_PORT = 10809;

    /** Current NBD server */
    private ExportServer server;

    /** Current NBD configuration */
    private NbdConfiguration serverConfiguration;

    /**
     * Create a new server. The bind address and port are taken from the configuration.
     * 
     * @param configuration
     *            configuration containing the {@link NbdServerConfigurationContext}.
     */
    public NbdServer(@Nonnull final MetaConfiguration configuration) {
        this(NbdServerInetAddressConfigKey.getInstance().getTypedValue(configuration), NbdServerPortConfigKey
                .getInstance().getTypedValue(configuration).intValue(), NbdServerTrimConfigKey.getInstance()
                .getTypedValue(configuration));
    }

    /**
     * Create a new server that will bind on the given address for the default port (10809).
     * 
     * @param address
     *            address to bind to
     */
    public NbdServer(final InetAddress address) {
        this(address, DEFAULT_NBD_PORT);
    }

    /**
     * Create a new server that will bind on the given address and port with trim not enabled.
     * 
     * @param address
     *            address to bind to
     * @param port
     *            port to bind to
     */
    public NbdServer(final InetAddress address, final int port) {
        this(address, port, NbdServerTrimConfigKey.DEFAULT_VALUE.booleanValue());
    }

    /**
     * Create a new server that will bind on the given address and port.
     * 
     * @param address
     *            address to bind to
     * @param port
     *            port to bind to
     * @param trim
     *            is trim enabled
     * 
     */
    public NbdServer(final InetAddress address, final int port, final boolean trim) {
        super(new NbdServerConfig(address, port, trim), "NBD");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#createServer()
     */
    @Override
    protected final ExportServer createServer(final NbdServerConfig nbdServerConfig) {
        // Create a new server for the current configuration
        serverConfiguration = new NbdConfiguration(this, nbdServerConfig.getPort(), nbdServerConfig.getAddress(),
                nbdServerConfig.isTrimEnabled());
        return server = new ExportServer(serverConfiguration);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#isServerStarted()
     */
    @Override
    protected final boolean isServerStarted() {
        if (serverConfiguration == null) {
            // Server stopped
            LOGGER.warn("Server stopped before end of start");
            return false;
        }
        // Start failed for some reason
        if (!isStarted()) {
            // Server abort unexpected (socket bind, ...)
            // TODO: throw something?
            LOGGER.warn("Server start failed: " + serverConfiguration);
            return false;
        }
        return serverConfiguration.isLoaded();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#serverCancel()
     */
    @Override
    protected final void serverCancel() {
        if (server != null) {
            server.cancel();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#serverStopped()
     */
    @Override
    protected final void serverStopped() {
        server = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#targetAdded(com.oodrive.nuage.srv.DeviceTarget,
     * com.oodrive.nuage.srv.DeviceTarget)
     */
    @Override
    protected final void targetAdded(final NbdExport targetNew, final NbdExport targetPrev) {
        // Add in the server if it is started
        if (server != null) {
            // Should be the same as prev.getTarget() or null
            final NbdExport prevTarget = server.addExport(targetNew);
            if (prevTarget != null && targetPrev != null) {
                if (prevTarget != targetPrev) {
                    LOGGER.warn("Multiple definitions of the target " + targetNew.getTargetName());
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.srv.AbstractServer#targetRemoved(java.lang.String, com.oodrive.nuage.srv.DeviceTarget)
     */
    @Override
    protected final void targetRemoved(final String name, final NbdExport target) {
        // Remove from the server if it is started
        if (server != null) {
            // Should be the same as removed.getTarget() or null
            final NbdExport removedTarget = server.removeExport(name);
            if (removedTarget != null && target != null) {
                if (removedTarget != target) {
                    LOGGER.warn("Multiple definitions of the target " + name);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.nbdsrv.NbdServerMXBean#getExports()
     */
    @Override
    public final NbdExportAttributes[] getExports() {
        getTargetSharedLock().lock();
        try {
            if (server == null) {
                // Local target list, no activity
                final Collection<NbdExport> targetList = getTargetMap().values();
                final int count = targetList.size();
                final NbdExportAttributes[] result = new NbdExportAttributes[count];
                final Iterator<NbdExport> ite = targetList.iterator();
                for (int i = count - 1; i >= 0; i--) {
                    final NbdExport target = ite.next();
                    result[i] = new NbdExportAttributes(target.getTargetName(), 0, target.getSize(),
                            target.isReadOnly());
                }
                return result;
            }
            else {
                // Delegate to the server
                final List<NbdExportStats> stats = server.getTargetStats();
                final int statsCount = stats.size();
                final NbdExportAttributes[] result = new NbdExportAttributes[statsCount];
                for (int i = statsCount - 1; i >= 0; i--) {
                    final NbdExportStats stat = stats.get(i);
                    result[i] = new NbdExportAttributes(stat.getName(), stat.getConnectionCount(), stat.getSize(),
                            stat.isReadOnly());
                }
                return result;
            }
        }
        finally {
            getTargetSharedLock().unlock();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.nbdsrv.NbdServerMXBean#isTrimEnabled()
     */
    @Override
    public final boolean isTrimEnabled() {
        return getServerConfig().isTrimEnabled();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.nbdsrv.NbdServerMXBean#setTrimEnabled()
     */
    @Override
    public final void setTrimEnabled(final boolean enabled) {
        getServerConfig().setTrimEnabled(enabled);

        final Notification n;
        if (enabled) {
            n = new AttributeChangeNotification(this, getNotificationSequenceNumber(), System.currentTimeMillis(),
                    "Trim changed", "Trim", "boolean", Boolean.FALSE, Boolean.TRUE);
        }
        else {
            n = new AttributeChangeNotification(this, getNotificationSequenceNumber(), System.currentTimeMillis(),
                    "Trim changed", "Trim", "boolean", Boolean.TRUE, Boolean.FALSE);
        }
        getNotificationBroadcasterSupport().sendNotification(n);
    }

    /**
     * Gets a list of the current Nbd targets.
     * 
     * @return the list of targets. May be empty.
     */
    final List<NbdExport> getInnerTargets() {
        final List<NbdExport> result = new ArrayList<>();
        getTargetSharedLock().lock();
        try {
            final Collection<NbdExport> targetsTmp = getTargetMap().values();
            for (final NbdExport target : targetsTmp) {
                result.add(target);
            }
        }
        finally {
            getTargetSharedLock().unlock();
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + getAddress().hashCode();
        result = prime * result + getPort();
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof NbdServer))
            return false;
        final NbdServer other = (NbdServer) obj;
        if (getPort() != other.getPort())
            return false;
        return getAddress().equals(other.getAddress());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return "NbdServer[" + getAddress() + ":" + getPort() + "]";
    }
}
