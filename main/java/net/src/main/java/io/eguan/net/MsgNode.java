package io.eguan.net;

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

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Identifies a node on the message node pool. A node is identified by a unique id and can be reached at the given
 * address.
 * 
 * @author oodrive
 * @author llambert
 */
@Immutable
public final class MsgNode {

    /** ID of the node. */
    private final UUID nodeId;

    /** Address of the node. */
    private final InetSocketAddress address;

    /**
     * Create a new message node object.
     * 
     * @param nodeId
     *            the {@link UUID} of the node
     * @param address
     *            the valid {@link InetSocketAddress} describing the network location of this node
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */
    public MsgNode(@Nonnull final UUID nodeId, @Nonnull final InetSocketAddress address) throws NullPointerException {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.address = Objects.requireNonNull(address);
    }

    /**
     * Gets the unique ID of this node.
     * 
     * @return a valid, non-<code>null</code> {@link UUID}
     */
    @Nonnull
    public final UUID getNodeId() {
        return nodeId;
    }

    /**
     * Gets the network address associated to this node.
     * 
     * @return a valid, non-<code>null</code> {@link InetSocketAddress}
     */
    @Nonnull
    public final InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public final int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MsgNode)) {
            return false;
        }
        final MsgNode other = (MsgNode) obj;
        return nodeId.equals(other.nodeId);
    }

    @Override
    public final String toString() {
        return "MsgNode[" + nodeId.toString() + "@" + address.getAddress().getHostAddress() + ":" + address.getPort()
                + "]";
    }

}
