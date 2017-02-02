package io.eguan.dtx;

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

import java.beans.ConstructorProperties;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of a peer in a DTX cluster. This version is exported as a MXBean attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class DtxPeerAdm {

    public static enum DtxPeerStatus {
        OFFLINE, ONLINE
    }

    private final String uuid;

    private final String ipAddress;

    private final int port;

    private final DtxPeerStatus status;

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this node
     * @param ipAddress
     *            the IP Address of the node
     * @param port
     *            the port of this node.
     * @param status
     *            the current status of the node
     */
    @ConstructorProperties({ "uuid", "ipAddress", "port", "status" })
    public DtxPeerAdm(@Nonnull final String uuid, @Nonnull final String ipAddress, final int port,
            @Nonnull final DtxPeerStatus status) {
        this.uuid = Objects.requireNonNull(uuid);
        this.ipAddress = Objects.requireNonNull(ipAddress);
        this.port = port;
        this.status = status;
    }

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this node
     * @param address
     *            the IP Address of the node
     * @param online
     *            true if peer online, false if offline
     */
    DtxPeerAdm(final UUID uuid, final InetSocketAddress address, final boolean online) {
        this(uuid.toString(), address == null ? "" : address.getAddress().getHostAddress(), address == null ? 0
                : address.getPort(), online == false ? DtxPeerStatus.OFFLINE : DtxPeerStatus.ONLINE);
    }

    /**
     * Gets the uuid of the node. May not be null.
     * 
     * @return the uuid of the node
     */
    public final String getUuid() {
        return uuid;
    }

    /**
     * Gets the IP address of the node. May not be null.
     * 
     * @return the IP address of the node
     */
    public final String getIpAddress() {
        return ipAddress;
    }

    /**
     * Gets the port of the node. May not be null.
     * 
     * @return the port of the node
     */
    public final int getPort() {
        return port;
    }

    /**
     * Gets the current of the node. May not be null.
     * 
     * @return the current status of the node
     */
    public final DtxPeerStatus getStatus() {
        return status;
    }

}
