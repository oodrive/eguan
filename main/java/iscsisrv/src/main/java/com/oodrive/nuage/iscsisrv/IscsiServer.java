package com.oodrive.nuage.iscsisrv;

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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.jscsi.target.Configuration;
import org.jscsi.target.Target;
import org.jscsi.target.TargetServer;
import org.jscsi.target.TargetStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.srv.AbstractServer;

/**
 * iSCSI server. The server waits for incoming connections on the configured address and port. The list of targets is
 * dynamic.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public final class IscsiServer extends AbstractServer<TargetServer, IscsiTarget, IscsiServerConfig> implements
        IscsiServerMXBean {

    /** Default iSCSI port (see RFC 3720) */
    public static final int DEFAULT_ISCSI_PORT = 3260;

    /**
     * jSCSI configuration. Override management of the list of targets.
     * 
     */
    static class IscsiConfiguration extends Configuration {

        /** Associated server. Needed to get the list of targets */
        private final IscsiServer server;
        /** Bind address. Keep here the original value for the getter */
        private final InetAddress address;
        /** toString does not change */
        private final String toStr;
        /** Flag to tell that the configuration have been loaded */
        private final AtomicBoolean loaded = new AtomicBoolean(false);

        IscsiConfiguration(final IscsiServer server, final int port, final InetAddress address) {
            super(port, address);
            this.server = server;
            this.address = address;
            this.toStr = "IscsiConfiguration[" + getTargetAddress() + ":" + getPort() + "]";
        }

        /**
         * Tells if the configuration have been loaded.
         * 
         * @return <code>true</code> if the jSCSI server is started and has read the configuration.
         */
        final boolean isLoaded() {
            return loaded.get();
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.jscsi.target.Configuration#getTargets()
         */
        @Override
        public final List<Target> getTargets() {
            loaded.set(true);
            return server.getInnerTargets();
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.jscsi.target.Configuration#getTargetAddressInetAddress()
         */
        @Override
        public final InetAddress getTargetAddressInetAddress() {
            return address;
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
    static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class.getPackage().getName());

    /** Current jSCSI server */
    private TargetServer server;
    /** Current jSCSI configuration */
    private IscsiConfiguration serverConfiguration;

    /**
     * Create a new server. The bind address and port are taken from the configuration.
     * 
     * @param configuration
     *            configuration containing the {@link IscsiServerConfigurationContext}.
     */
    public IscsiServer(@Nonnull final MetaConfiguration configuration) {
        this(IscsiServerInetAddressConfigKey.getInstance().getTypedValue(configuration), IscsiServerPortConfigKey
                .getInstance().getTypedValue(configuration).intValue());
    }

    /**
     * Create a new server that will bind on the given address for the default port (3260).
     * 
     * @param address
     *            address to bind to
     */
    public IscsiServer(@Nonnull final InetAddress address) {
        this(address, DEFAULT_ISCSI_PORT);
    }

    /**
     * Create a new server that will bind on the given address and port.
     * 
     * @param address
     *            address to bind to
     * @param port
     *            port to bind to
     */
    public IscsiServer(@Nonnull final InetAddress address, final int port) {
        super(new IscsiServerConfig(address, port), "iSCSI");
    }

    @Override
    protected final TargetServer createServer(final IscsiServerConfig iscsiServerConfig) {
        // Create a new server for the current configuration
        serverConfiguration = new IscsiConfiguration(this, iscsiServerConfig.getPort(), iscsiServerConfig.getAddress());
        return server = new TargetServer(serverConfiguration);
    }

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

    @Override
    protected final void serverCancel() {
        if (server != null) {
            server.cancel();
        }
    }

    @Override
    protected final void serverStopped() {
        server = null;
        serverConfiguration = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.iscsisrv.IscsiServerMXBean#getTargets()
     */
    @Override
    public final IscsiTargetAttributes[] getTargets() {
        getTargetSharedLock().lock();
        try {
            if (server == null) {
                // Local target list, no activity
                final Collection<IscsiTarget> targetList = getTargetMap().values();
                final int count = targetList.size();
                final IscsiTargetAttributes[] result = new IscsiTargetAttributes[count];
                final Iterator<IscsiTarget> ite = targetList.iterator();
                for (int i = count - 1; i >= 0; i--) {
                    final IscsiTarget target = ite.next();
                    result[i] = new IscsiTargetAttributes(target.getTargetName(), target.getTargetAlias(), 0,
                            target.getSize(), target.isReadOnly());
                }
                return result;
            }
            else {
                // Delegate to the server
                final List<TargetStats> stats = server.getTargetStats();
                final int statsCount = stats.size();
                final IscsiTargetAttributes[] result = new IscsiTargetAttributes[statsCount];
                for (int i = statsCount - 1; i >= 0; i--) {
                    final TargetStats stat = stats.get(i);
                    result[i] = new IscsiTargetAttributes(stat.getName(), stat.getAlias(), stat.getConnectionCount(),
                            stat.getSize(), stat.isWriteProtected());
                }
                return result;
            }
        }
        finally {
            getTargetSharedLock().unlock();
        }
    }

    @Override
    protected final void targetAdded(final IscsiTarget targetNew, final IscsiTarget targetPrev) {
        // Add in the server if it is started
        if (server != null) {
            // Should be the same as prev.getTarget() or null
            final Target prevTarget = server.addTarget(targetNew.getTarget());
            if (prevTarget != null && targetPrev != null) {
                if (prevTarget != targetPrev.getTarget()) {
                    LOGGER.warn("Multiple definitions of the target " + targetNew.getTargetName());
                }
            }
        }
    }

    @Override
    protected final void targetRemoved(final String name, final IscsiTarget target) {
        // Remove from the server if it is started
        if (server != null) {
            // Should be the same as removed.getTarget() or null
            final Target removedTarget = server.removeTarget(name);
            if (removedTarget != null && target != null) {
                if (removedTarget != target.getTarget()) {
                    LOGGER.warn("Multiple definitions of the target " + name);
                }
            }
        }
    }

    /**
     * Gets a list of the current jSCSI targets.
     * 
     * @return the list of targets. May be empty.
     */
    final List<Target> getInnerTargets() {
        final List<Target> result = new ArrayList<>();
        getTargetSharedLock().lock();
        try {
            final Collection<IscsiTarget> targetsTmp = getTargetMap().values();
            for (final IscsiTarget target : targetsTmp) {
                result.add(target.getTarget());
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
        if (!(obj instanceof IscsiServer))
            return false;
        final IscsiServer other = (IscsiServer) obj;
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
        return "IscsiServer[" + getAddress() + ":" + getPort() + "]";
    }
}
