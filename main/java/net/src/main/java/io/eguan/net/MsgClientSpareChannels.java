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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.jboss.netty.channel.Channel;

/**
 * Stores the channels created to send long lasting requests to peers. The channels are closed automatically after a
 * while.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class MsgClientSpareChannels {

    /**
     * Channel connected to a peer node.
     * 
     */
    interface PeerChannel {
        /**
         * {@link UUID} of the peer.
         * 
         * @return the {@link UUID} of the peer.
         */
        UUID getNode();

        /**
         * A {@link Channel} connected to the peer.
         * 
         * @return an opened {@link Channel} to the peer.
         */
        Channel getChannel();
    }

    /**
     * New channel and node.
     * 
     */
    private final class PeerChannelImpl implements PeerChannel {
        private final UUID node;
        private final Channel channel;
        /** <code>true</code> when the close of the channel is done or in progress */
        @GuardedBy(value = "this")
        private boolean closed;

        PeerChannelImpl(final UUID node, final Channel channel) {
            super();
            this.node = node;
            this.channel = channel;
            this.closed = false;
        }

        @Override
        public final UUID getNode() {
            return node;
        }

        @Override
        public final Channel getChannel() {
            return channel;
        }

        final synchronized void enqueue() {
            if (!closed && channel.isConnected()) {
                spareChannels.add(new PeerChannelRef(this));
            }
        }

        /**
         * Remove this channel from the spare ones.
         * 
         * @return <code>true</code> if the channel is still connected
         */
        final synchronized boolean dequeue(final PeerChannelRef peerChannelRef) {
            if (!closed && channel.isConnected()) {
                final PeerChannelImpl peerChannelImpl = peerChannelRef.removePeerChannel();
                assert peerChannelImpl == this || peerChannelImpl == null;
                // May have been removed by someone else?
                return peerChannelImpl != null;
            }
            return false;
        }

        final synchronized void close() {
            if (!closed) {
                channel.close();
                closed = true;
            }
        }
    }

    /**
     * Object queued that reference a PeerChannel. The reference is <code>null</code> if the channel have been re-used.
     * 
     */
    private final class PeerChannelRef implements Delayed {
        private final AtomicReference<PeerChannelImpl> peerChannelRef;
        /** Date of expiration, in seconds */
        private final long timeEnd;

        PeerChannelRef(final PeerChannelImpl peerChannelImpl) {
            super();
            this.peerChannelRef = new AtomicReference<>(peerChannelImpl);
            this.timeEnd = System.currentTimeMillis() + delay;
        }

        final PeerChannelImpl getPeerChannel() {
            return peerChannelRef.get();
        }

        final PeerChannelImpl removePeerChannel() {
            return peerChannelRef.getAndSet(null);
        }

        @Override
        public final int compareTo(final Delayed o) {
            if (this == o) {
                return 0;
            }

            final PeerChannelRef other = (PeerChannelRef) o;
            final long endOther = other.timeEnd;
            return (int) (timeEnd - endOther);
        }

        @Override
        public final long getDelay(final TimeUnit unit) {
            return unit.convert(timeEnd - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Closes the spare channels.
     * 
     * 
     */
    final class PeerChannelCloser implements Runnable {
        private final AtomicBoolean shutdown;

        PeerChannelCloser() {
            super();
            this.shutdown = new AtomicBoolean();
        }

        final void shutdown() {
            shutdown.set(true);
        }

        @Override
        public final void run() {
            shutdown.set(false);
            while (!shutdown.get()) {
                try {
                    final PeerChannelRef toClose = spareChannels.poll(2, TimeUnit.SECONDS);
                    if (toClose != null) {
                        final PeerChannelImpl peerChannelImpl = toClose.removePeerChannel();
                        if (peerChannelImpl != null) {
                            peerChannelImpl.close();
                        }
                    }
                }
                catch (final Throwable t) {
                    MsgClientStartpoint.LOGGER.warn("Error while handling spare channels", t);
                }
            }
        }

    }

    private static final long POLL = 2; // 2 seconds

    private final MsgClientStartpoint clientStartpoint;

    /** Name of the closer thread. */
    private final String closerName;
    /** Spare channels/ */
    private final DelayQueue<PeerChannelRef> spareChannels = new DelayQueue<>();

    private final PeerChannelCloser peerChannelCloser;

    /** Thread closing channels */
    private Thread closer;

    private final long delay;

    /**
     * Can change delay for unit tests purpose.
     * 
     * @param clientStartpoint
     * @param delay
     */
    MsgClientSpareChannels(final MsgClientStartpoint clientStartpoint, final long delay) {
        super();
        this.clientStartpoint = clientStartpoint;
        this.closerName = "SpareChannels[" + clientStartpoint.getMsgClientId() + "]";
        this.peerChannelCloser = new PeerChannelCloser();
        this.delay = delay;
    }

    /**
     * Start management of spare channels.
     */
    final void start() {
        closer = new Thread(peerChannelCloser, closerName);
        try {
            closer.setDaemon(true);
        }
        finally {
            closer.start();
        }
    }

    /**
     * Release resources related to spare channels.
     */
    final void stop() {
        // First, shutdown thread
        try {
            final Thread closerTmp = closer;
            closer = null;
            if (closerTmp != null) {
                peerChannelCloser.shutdown();
                closerTmp.join(POLL * 1000);
            }
        }
        catch (final Throwable t) {
            MsgClientStartpoint.LOGGER.warn("Error while stopping " + closerName);
        }

        // Close the remaining channels
        final Iterator<PeerChannelRef> ite = spareChannels.iterator();
        while (ite.hasNext()) {
            final PeerChannelRef peerChannelRef = ite.next();
            final PeerChannelImpl peerChannelImpl = peerChannelRef.removePeerChannel();
            if (peerChannelImpl != null) {
                try {
                    peerChannelImpl.close();
                }
                catch (final Throwable t) {
                    MsgClientStartpoint.LOGGER.warn("Error while closing connection to " + peerChannelImpl.getNode());
                }
            }
        }
        spareChannels.clear();
    }

    /**
     * Gets a channel connected to the given peer.
     * 
     * @param node
     *            peer to connect to
     * @return the Channel and peer ID.
     * @throws InterruptedException
     * @throws ConnectException
     */
    final PeerChannel getChannel(@Nonnull final UUID node, @Nonnull final InetSocketAddress nodeAddr)
            throws ConnectException, InterruptedException {
        Objects.requireNonNull(node);
        Objects.requireNonNull(nodeAddr);

        // Look for a spare channel
        final Iterator<PeerChannelRef> ite = spareChannels.iterator();
        while (ite.hasNext()) {
            final PeerChannelRef peerChannelRef = ite.next();
            final PeerChannelImpl peerChannelImpl = peerChannelRef.getPeerChannel();
            if (peerChannelImpl != null && peerChannelImpl.getNode().equals(node)) {
                if (peerChannelImpl.dequeue(peerChannelRef)) {
                    assert peerChannelRef.getPeerChannel() == null;
                    assert peerChannelImpl.getChannel().isConnected();
                    return peerChannelImpl;
                }
            }
        }

        // Open a new channel
        final Channel peerChannel = clientStartpoint.newSecondaryChannelFuture(nodeAddr);
        return new PeerChannelImpl(node, peerChannel);
    }

    /**
     * Release a {@link PeerChannel}, making it available for a future use.
     * 
     * @param peerChannel
     */
    final void releaseChannel(final PeerChannel peerChannel) {
        ((PeerChannelImpl) peerChannel).enqueue();
    }

    /**
     * For unit tests.
     * 
     * @return the number of opened channels.
     */
    final int getChannelCount() {
        int count = 0;
        final Iterator<PeerChannelRef> ite = spareChannels.iterator();
        while (ite.hasNext()) {
            final PeerChannelRef peerChannelRef = ite.next();
            final PeerChannelImpl peerChannelImpl = peerChannelRef.getPeerChannel();
            if (peerChannelImpl != null) {
                count++;
            }
        }
        return count;
    }
}
