package io.eguan.net;

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

import io.eguan.net.MsgClientStartpoint.PeerReconnection;
import io.eguan.proto.net.MsgWrapper;
import io.eguan.proto.net.MsgWrapper.MsgReply;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

/**
 * The Netty client handler which receive all remote events.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class MsgClientHandler extends SimpleChannelHandler {

    /** <code>true</code> if the handler is the main broadcast channel */
    private final boolean mainChannel;
    /** Map used to wait for acknowledgments from remote peers through the use of a count down latch. */
    private final Map<Long, CountDownLatch> mapQueue;
    private final ClientBootstrap clientBootstrap;
    /** A ReadWrite lock to protect the variable channelGroup. */
    private final ReadWriteLock lockClientStarted;
    /** A set which contains all remote channels. */
    @GuardedBy(value = "lockClientStarted")
    private final ChannelGroup channelGroup;
    @GuardedBy(value = "peersConnect")
    private final Map<InetSocketAddress, PeerReconnection> peersConnect;
    @GuardedBy(value = "peersConnect")
    private final Map<UUID, InetSocketAddress> peerNodes;
    private final Map<Long, Queue<MsgServerRemoteStatus>> mapExceptions;
    /** Timer used to generated reconnection to remote peers. */
    private final AtomicReference<Timer> timerRef;
    /** Message client id. */
    private final UUID msgClientId;
    /** Message peer id. Set when the handler is connected. */
    private UUID msgPeerId = null;
    /** Tells whether the client is started. */
    @GuardedBy(value = "lockClientStarted")
    private final AtomicBoolean clientStarted;

    MsgClientHandler(final boolean mainChannel, final UUID msgClientId, final Map<Long, CountDownLatch> mapQueue,
            final Map<Long, Queue<MsgServerRemoteStatus>> mapExceptions, final ClientBootstrap clientBootstrap,
            final ChannelGroup channelGroup, final ReadWriteLock lockClientStarted, final AtomicBoolean clientStarted,
            final Map<InetSocketAddress, PeerReconnection> peersConnect, final Map<UUID, InetSocketAddress> peerNodes,
            final AtomicReference<Timer> timerRef) {
        this.mainChannel = mainChannel;
        this.msgClientId = msgClientId;
        this.mapQueue = mapQueue;
        this.mapExceptions = mapExceptions;
        this.clientBootstrap = clientBootstrap;
        this.channelGroup = channelGroup;
        this.lockClientStarted = lockClientStarted;
        this.clientStarted = clientStarted;
        this.peersConnect = peersConnect;
        this.peerNodes = peerNodes;
        this.timerRef = timerRef;
    }

    @Override
    public final void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        final MsgWrapper.MsgReply msgReply = (MsgReply) e.getMessage();
        final Long msgMsgId = Long.valueOf(msgReply.getMsgId());

        // If an error occurs into the remote peer then add the associated exception to the mapExceptions
        if (!msgReply.getStatus()) {
            final Collection<MsgServerRemoteStatus> msgServerRemoteStatus = mapExceptions.get(msgMsgId);
            if (msgServerRemoteStatus != null) {
                msgServerRemoteStatus.add(new MsgServerRemoteStatus(msgPeerId, msgReply.getException(), e
                        .getRemoteAddress()));
            }

            MsgClientStartpoint.LOGGER.warn("Msg client [{}] receive exception '{}' from '{}/{}'", new Object[] {
                    msgClientId, msgReply.getException(), msgPeerId, e.getRemoteAddress() });
        }
        else if (msgReply.hasRepData()) {
            final Collection<MsgServerRemoteStatus> msgServerRemoteStatus = mapExceptions.get(msgMsgId);
            if (msgServerRemoteStatus != null) {
                msgServerRemoteStatus.add(new MsgServerRemoteStatus(msgPeerId, msgReply.getRepData(), e
                        .getRemoteAddress()));
            }

            if (MsgClientStartpoint.LOGGER.isDebugEnabled()) {
                MsgClientStartpoint.LOGGER.debug("Msg client [{}] received reply from '{}/{}'", new Object[] {
                        msgClientId, msgPeerId, e.getRemoteAddress() });
            }
        }

        // ACK received then increment the counter of the count down latch associated to the message
        final CountDownLatch countDownLatch = mapQueue.get(msgMsgId);
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }

    }

    @Override
    public final void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {

        // If the client is stopped then close all new connected channels in order to be able to release allocated
        // resources when the client stop.
        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                ctx.getChannel().close();
                return;
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }

        final InetSocketAddress peer;
        synchronized (peersConnect) {
            assert msgPeerId == null;

            peer = (InetSocketAddress) ctx.getChannel().getRemoteAddress();

            // Connection completed
            if (mainChannel) {
                final PeerReconnection peerReconnection = peersConnect.get(peer);
                if (peerReconnection == null) {
                    ctx.getChannel().close();
                    return;
                }

                // Reset the exponential backoff algorithm for that remote peer
                peerReconnection.connected();
            }

            // Look for the UUID of the peer
            for (final Map.Entry<UUID, InetSocketAddress> peerNode : peerNodes.entrySet()) {
                if (peerNode.getValue().equals(peer)) {
                    msgPeerId = peerNode.getKey();
                    break;
                }
            }
            // Peer UUID should be found...
            if (msgPeerId == null) {
                ctx.getChannel().close();
                MsgClientStartpoint.LOGGER.error("Failed to find the node ID of '{}'", peer);
                return;
            }
        }

        // Add the new channel to the channel group
        if (mainChannel) {
            lockClientStarted.writeLock().lock();
            try {
                channelGroup.add(ctx.getChannel());
            }
            finally {
                lockClientStarted.writeLock().unlock();
            }
        }
        MsgClientStartpoint.LOGGER.info("Msg client [{}] connected to '{}@{}'", msgClientId, msgPeerId, peer);
    }

    @Override
    public final void channelClosed(final ChannelHandlerContext ctx, final ChannelStateEvent e) {

        // Reset the UUID of the peer
        msgPeerId = null;

        // If the client is stopped then close all new connected channels in order to be able to release allocated
        // resources when the client stop.
        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                ctx.getChannel().close();
                return;
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }

        if (mainChannel) {
            final InetSocketAddress peer = (InetSocketAddress) clientBootstrap.getOption("remoteAddress");
            final PeerReconnection peerReconnection;
            synchronized (peersConnect) {
                peerReconnection = peersConnect.get(peer);
            }

            // The peer was removed from the map peers
            if (peerReconnection == null) {
                return;
            }
            final long delay = peerReconnection.getDelay();

            try {
                final Timer timer = timerRef.get();
                if (timer != null) {
                    timer.newTimeout(new TimerTask() {
                        @Override
                        public final void run(final Timeout timeout) throws Exception {
                            clientBootstrap.connect();
                        }
                    }, delay, TimeUnit.SECONDS);
                }
            }
            catch (final Throwable t) {
                MsgClientStartpoint.LOGGER.error("Unable to run new task", t);
            }
        }

        if (MsgClientStartpoint.LOGGER.isTraceEnabled()) {
            MsgClientStartpoint.LOGGER.trace("Msg client [{}] Will try a reconnection to '{}'", msgClientId,
                    clientBootstrap.getOption("remoteAddress"));
        }
    }
}
