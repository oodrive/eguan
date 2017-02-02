package io.eguan.dtx.proto;

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

import io.eguan.dtx.DtxNode;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.dtx.DistTxWrapper.TxNode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

/**
 * Utility class for converting to/from protobuf objects.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TxProtobufUtils {

    private TxProtobufUtils() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Converts a {@link UUID} to a protobuf-defined {@link Uuid}.
     * 
     * @param uuid
     *            a non-<code>null</code> {@link UUID}
     * @return a valid {@link Uuid}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public static final Uuid toUuid(@Nonnull final UUID uuid) throws NullPointerException {
        return Uuid.newBuilder().setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits()).build();
    }

    /**
     * Converts a protobuf-defined {@link Uuid} to a {@link UUID}.
     * 
     * @param uuid
     *            a non-<code>null</code> {@link Uuid}
     * @return a valid {@link UUID}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public static final UUID fromUuid(@Nonnull final Uuid uuid) throws NullPointerException {
        return new UUID(uuid.getMsb(), uuid.getLsb());
    }

    /**
     * Converts a {@link DtxNode} into a protobuf-defined {@link TxNode}.
     * 
     * @param dtxNode
     *            a non-<code>null</code> {@link DtxNode}
     * @return a valid {@link TxNode}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public static final TxNode toTxNode(@Nonnull final DtxNode dtxNode) throws NullPointerException {
        final InetSocketAddress nodeAddress = dtxNode.getAddress();
        return TxNode.newBuilder().setNodeId(toUuid(dtxNode.getNodeId()))
                .setIpAddress(ByteString.copyFrom(nodeAddress.getAddress().getAddress()))
                .setPort(nodeAddress.getPort()).build();
    }

    /**
     * Converts a protobuf-defined {@link TxNode} to a {@link DtxNode}.
     * 
     * @param txNode
     *            a non-<code>null</code> {@link TxNode}
     * @return a valid {@link DtxNode}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the included hostname cannot be identified
     */
    public static final DtxNode fromTxNode(@Nonnull final TxNode txNode) throws NullPointerException,
            IllegalArgumentException {
        try {
            final InetAddress nodeIpAddress = InetAddress.getByAddress(txNode.getIpAddress().toByteArray());
            final InetSocketAddress dtxNodeAddress = new InetSocketAddress(nodeIpAddress, txNode.getPort());
            return new DtxNode(fromUuid(txNode.getNodeId()), dtxNodeAddress);
        }
        catch (final UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
