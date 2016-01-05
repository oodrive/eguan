package io.eguan.net;

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

import java.net.SocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.protobuf.ByteString;

/**
 * A {@link MsgServerRemoteStatus} is returned when an exception occurs into a remote peer or if the server have
 * returned a replyBytes. It contains the peer address and the originating exception or the replyBytes.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
@Immutable
public final class MsgServerRemoteStatus {
    private final UUID nodeId;
    private final String exceptionName;
    private final ByteString replyBytes;
    private final SocketAddress remotePeer;

    MsgServerRemoteStatus(@Nonnull final UUID nodeId, @Nonnull final String exceptionName,
            @Nonnull final SocketAddress remotePeer) {
        super();
        this.nodeId = Objects.requireNonNull(nodeId);
        this.exceptionName = Objects.requireNonNull(exceptionName);
        this.replyBytes = null;
        this.remotePeer = Objects.requireNonNull(remotePeer);
    }

    MsgServerRemoteStatus(@Nonnull final UUID nodeId, @Nonnull final ByteString replyBytes,
            @Nonnull final SocketAddress remotePeer) {
        super();
        this.nodeId = Objects.requireNonNull(nodeId);
        this.exceptionName = null;
        this.replyBytes = Objects.requireNonNull(replyBytes);
        this.remotePeer = Objects.requireNonNull(remotePeer);
    }

    public final UUID getNodeId() {
        return nodeId;
    }

    public final String getExceptionName() {
        return exceptionName;
    }

    public final ByteString getReplyBytes() {
        return replyBytes;
    }

    public final SocketAddress getRemotePeer() {
        return remotePeer;
    }
}
