package com.oodrive.nuage.vold;

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
import java.net.InetSocketAddress;
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.net.MsgNode;
import com.oodrive.nuage.net.MsgServerEndpoint;
import com.oodrive.nuage.net.MsgServerHandler;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;

/**
 * VOLD synchronization server. Listen to an address and port defined by the configuration keys
 * {@link ServerEndpointInetAddressConfigKey} and {@link ServerEndpointPortConfigKey}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * 
 */
public final class VoldSyncServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(VoldSyncServer.class.getSimpleName());

    /** Id of the node */
    private final UUID nodeId;
    /** Remote messages handler */
    private final MsgServerHandler msgServerHandler;
    /** Server bind address */
    private final InetSocketAddress bindAddress;

    /** Endpoint listening to incoming messages. <code>null</code> when the server is stopped. */
    @GuardedBy(value = "startLock")
    private MsgServerEndpoint endpoint;
    /** true when the server is started */
    private boolean started;

    private final Object startLock = new Object();

    /**
     * Create a new synchronization server. Waits for incoming messages on the given address and port.
     * 
     * @param address
     *            address to bind to
     * @param port
     *            port to bind to
     */
    VoldSyncServer(final UUID nodeId, final MsgServerHandler msgServerHandler, final InetAddress address, final int port) {
        super();
        this.nodeId = nodeId;
        this.msgServerHandler = msgServerHandler;
        this.bindAddress = new InetSocketAddress(address, port);
    }

    /**
     * Start the VOLD server endpoint.
     * 
     * @throws IllegalStateException
     */
    final void start() throws IllegalStateException {
        synchronized (startLock) {
            if (started) {
                throw new IllegalStateException("started");
            }
            assert endpoint == null;

            // Initialize server
            try {
                final MsgNode msgNode = new MsgNode(nodeId, bindAddress);
                endpoint = new MsgServerEndpoint(msgNode, msgServerHandler, RemoteOperation.getDefaultInstance());
                endpoint.start();
                started = true;
            }
            finally {
                if (!started) {
                    endpoint = null;
                }
            }
        }
    }

    /**
     * Stops the server.
     */
    final void stop() {
        synchronized (startLock) {
            if (!started) {
                // Ignore silently
                return;
            }
            assert endpoint != null;

            // Stop the server
            try {
                endpoint.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while stopping the server", t);
            }
            endpoint = null;
            started = false;
        }
    }

    /**
     * Register the MXBean for the server.
     * 
     * @return {@link ObjectName} of the MXBean
     */
    final ObjectName registerMXBean(final MBeanServer mbeanServer) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException {
        return endpoint.registerMXBean(mbeanServer);
    }
}
