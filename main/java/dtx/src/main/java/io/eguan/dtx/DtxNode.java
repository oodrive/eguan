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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.Address;

/**
 * Unique identifier for nodes of the DTX cluster.
 * 
 * This class combines a unique node ID with a specific network location to uniquely identify cluster nodes.
 * 
 * Note: There is no guarantee of correctness or freshness for the information contained by instances of this class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class DtxNode implements Serializable {

    private static final long serialVersionUID = -4811197881202171231L;

    private static final int HASHCODE_SEED = 17;
    private static final int HASHCODE_MULTIPLICATOR = 127;

    /**
     * ID of the node.
     */
    private final UUID nodeId;

    /**
     * Address of the node.
     */
    private final InetSocketAddress address;

    /**
     * Constructs an immutable instance associating the given node ID to a concrete network address.
     * 
     * Note: no verifications as to the
     * 
     * @param nodeId
     *            the {@link UUID} of the node supposedly reachable
     * @param address
     *            the valid {@link InetSocketAddress} describing the network location of this node
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */

    public DtxNode(@Nonnull final UUID nodeId, @Nonnull final InetSocketAddress address) throws NullPointerException {
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
    public final String toString() {
        // returns the canonical representation of a cluster node
        return nodeId.toString() + "@" + address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    @Override
    public final boolean equals(final Object obj) {
        if ((obj == null) || !(obj instanceof DtxNode)) {
            return false;
        }
        final DtxNode dtxNode = (DtxNode) obj;
        return address.equals(dtxNode.getAddress()) && this.nodeId.equals(dtxNode.getNodeId());
    }

    @Override
    public final int hashCode() {
        int result = HASHCODE_SEED;
        result = HASHCODE_MULTIPLICATOR * result + this.nodeId.hashCode();
        result = HASHCODE_MULTIPLICATOR * result + this.address.hashCode();
        return result;
    }

    /**
     * Constructs a Hazelcast {@link Member} representation of this {@link DtxNode}.
     * 
     * @param localMember
     *            the {@link Member} considered to be the "local member", i.e. running on the local machine
     * @return a non-<code>null</code> {@link Member} with the same {@link DtxNode#getAddress() address} as this
     *         {@link DtxNode}
     */
    final Member asHazelcastMember(final Member localMember) {
        return new MemberImpl(new Address(this.address), localMember == null ? false : this.address.equals(localMember
                .getInetSocketAddress()));

    }

}
