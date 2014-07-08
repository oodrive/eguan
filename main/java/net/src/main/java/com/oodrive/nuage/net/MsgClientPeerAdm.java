package com.oodrive.nuage.net;

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

import java.beans.ConstructorProperties;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of a peer. This version is exported as a MXBean attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class MsgClientPeerAdm {

    private final String uuid;
    private final String ipAddress;
    private final int port;
    private final boolean isConnected;

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this node
     * @param ipAddress
     *            the IP Address of the node
     * @param port
     *            the port of this node.
     * @param isConnected
     *            <code>true</code> if the peer is connected
     */
    @ConstructorProperties({ "uuid", "ipAddress", "port", "connected" })
    public MsgClientPeerAdm(@Nonnull final String uuid, @Nonnull final String ipAddress, final int port,
            final boolean isConnected) {
        this.uuid = Objects.requireNonNull(uuid);
        this.ipAddress = Objects.requireNonNull(ipAddress);
        this.port = port;
        this.isConnected = isConnected;
    }

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this node
     * @param address
     *            the IP Address of the node
     * @param isConnected
     *            true if peer connected, false if not connected
     */
    MsgClientPeerAdm(final UUID uuid, final InetSocketAddress address, final boolean isConnected) {
        this(uuid.toString(), address == null ? "" : address.getAddress().getHostAddress(), address == null ? 0
                : address.getPort(), isConnected);
    }

    /**
     * Gets the uuid of the peer. May not be null.
     * 
     * @return the uuid of the peer
     */
    public final String getUuid() {
        return uuid;
    }

    /**
     * Gets the Ip address of the peer. May not be null.
     * 
     * @return the ip Address of the peer
     */
    public final String getIpAddress() {
        return ipAddress;
    }

    /**
     * Gets the port of the peer.
     * 
     * @return the port of the peer
     */
    public final int getPort() {
        return port;
    }

    /**
     * Tells if the peer is connected.
     * 
     * @return <code>true</code> if the peer is connected.
     */

    public final boolean isConnected() {
        return isConnected;
    }
}
