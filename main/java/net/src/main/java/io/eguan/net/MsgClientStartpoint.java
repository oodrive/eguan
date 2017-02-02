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

import io.eguan.net.MsgClientSpareChannels.PeerChannel;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.net.MsgWrapper;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.MessageLite;

/**
 * Message client endpoint which initiate connections to remote peers.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class MsgClientStartpoint implements MsgClientMXBean {
    static final Logger LOGGER = LoggerFactory.getLogger(MsgClientStartpoint.class.getName());

    /** Keep thread factories names */
    static {
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
    }

    /**
     * Factory used to create a Netty pipeline for each channel.
     * 
     * 
     */
    final class MsgClientPipelineFactory implements ChannelPipelineFactory {

        private final boolean mainChannel;
        private final ClientBootstrap bootstrap;

        public MsgClientPipelineFactory(final boolean mainChannel, final ClientBootstrap bootstrap) {
            this.mainChannel = mainChannel;
            this.bootstrap = bootstrap;
        }

        @Override
        public final ChannelPipeline getPipeline() throws Exception {

            final ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            pipeline.addLast("protobufDecoder", new ProtobufDecoder(MsgWrapper.MsgReply.getDefaultInstance()));
            pipeline.addLast("msgClientHandler", new MsgClientHandler(mainChannel, msgClientId, mapQueue,
                    mapExceptions, bootstrap, channelGroup, lockClientStarted, clientStarted, peersConnect, peerNodes,
                    timerRef));
            pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            return pipeline;
        }
    }

    /**
     * Class used to manage the exponential backoff of the delay. Its composed of two integer : the first is used to
     * count the number of reconnection attempts, the second is the exponent which is used to generated the max value of
     * the delay calculated by (2**exponent). Every five failed reconnection, the exponent is incremented. The delay is
     * limited to (2**11) = 2048 seconds in order to prevent very high delay.
     * 
     * 
     */
    static final class PeerReconnection {
        private int attempt;
        private int exponent;
        private final AtomicBoolean connected;
        private static final Random random = new Random();

        PeerReconnection() {
            super();
            connected = new AtomicBoolean(false);
        }

        /**
         * Called when the peer is connected.
         */
        final void connected() {
            attempt = 0;
            exponent = 0;
            connected.set(true);
        }

        /**
         * Reset the current values.
         */
        final void reset() {
            attempt = 0;
            exponent = 0;
            connected.set(false);
        }

        /**
         * Advice if the client is connected to the peer.
         * 
         * @return <code>true</code> if the client is probably connected to the peer.
         */
        final boolean isConnected() {
            return connected.get();
        }

        /**
         * Generate a delay in exponential manner.
         * 
         * @return
         */
        final long getDelay() {
            connected.set(false);

            // attempt € [0, 5]
            attempt = (attempt + 1) % 6;
            // Max delay = 2**11 = 2048 seconds ~ 30 minutes
            if (attempt == 5) {
                exponent = (exponent < 11) ? (exponent + 1) : 11;
            }

            // Backoff exponentially the delay of the reconnection
            final int maxValue = (2 << exponent);

            // Random is not documented as thread safe
            synchronized (random) {
                return random.nextInt(maxValue);
            }
        }
    }

    /** Set used to gather every channel created. */
    @GuardedBy(value = "lockClientStarted")
    private final ChannelGroup channelGroup = new DefaultChannelGroup(MsgClientStartpoint.class.getName());
    /** Map used to wait for acknowledgments from remote peers through the use of a count down latch. */
    private final ConcurrentMap<Long, CountDownLatch> mapQueue = new ConcurrentHashMap<>();
    /** Map which contains the remotely raised exception for a given message. */
    private final ConcurrentMap<Long, Queue<MsgServerRemoteStatus>> mapExceptions = new ConcurrentHashMap<>();
    /** Timer used to generated reconnection to remote peers. */
    private final AtomicReference<Timer> timerRef = new AtomicReference<>();
    /** Executors used by Netty. */
    @GuardedBy(value = "lockClientStarted")
    private ExecutorService nettyBoss;
    @GuardedBy(value = "lockClientStarted")
    private ExecutorService nettyWorker;
    /** Netty factory used to create channels. */
    @GuardedBy(value = "lockClientStarted")
    private ChannelFactory factory;
    /**
     * Map which contains the address of every remote peers and a {@link PeerReconnection} object for the management of
     * reconnection.
     */
    @GuardedBy(value = "peersConnect")
    private final Map<InetSocketAddress, PeerReconnection> peersConnect = new HashMap<>();

    @GuardedBy(value = "peersConnect")
    private final Map<UUID, InetSocketAddress> peerNodes = new HashMap<>();

    /**
     * Generator of unique id for every message sent.
     */
    private final AtomicLong currentMsgId = new AtomicLong();

    /** Unique id of the message client. */
    private final UUID msgClientId;

    /** Opening/closing of spare channels */
    private final MsgClientSpareChannels msgClientSpareChannels;

    /** A ReadWrite lock to protect the variable clientStarted and channelGroup. */
    private final ReadWriteLock lockClientStarted = new ReentrantReadWriteLock();

    /** Tells whether the client is started. */
    @GuardedBy(value = "lockClientStarted")
    private final AtomicBoolean clientStarted = new AtomicBoolean(false);

    /** Delay before closing a spare channel */
    private static final long DELAY_DEFAULT = 30 * 1000; // 30 seconds

    private volatile long TIMEOUT_REPLY = 2000; // ms

    private volatile long TIMEOUT_CONNECT = 20000; // 20s

    /**
     * Create a new client start point with a random UUID. For test purpose only.
     * 
     * @param peers
     */
    MsgClientStartpoint(final List<MsgNode> peers) {
        super();
        this.msgClientId = UUID.randomUUID();
        this.msgClientSpareChannels = new MsgClientSpareChannels(this, 3 * 1000L); // shorter delay for unit tests

        if (peers != null) {
            for (final MsgNode peer : peers) {
                addPeer(peer);
            }
        }
    }

    public MsgClientStartpoint(@Nonnull final UUID msgClientId, final List<MsgNode> peers) {
        super();
        this.msgClientId = Objects.requireNonNull(msgClientId);
        this.msgClientSpareChannels = new MsgClientSpareChannels(this, DELAY_DEFAULT);

        if (peers != null) {
            for (final MsgNode peer : peers) {
                addPeer(peer);
            }
        }
    }

    /**
     * Gets the {@link UUID} identifying this sender.
     * 
     * @return {@link UUID} identifying this sender.
     */
    public final UUID getMsgClientId() {
        return msgClientId;
    }

    /**
     * For unit tests only.
     */
    final MsgClientSpareChannels getMsgClientSpareChannels() {
        return msgClientSpareChannels;
    }

    /**
     * Gets the {@link UUID} identifying this {@link MsgClientStartPoint}.
     * 
     * @return a {@link String} identifying this {@link MsgClientStartPoint}.
     */
    @Override
    public final String getUuid() {
        return getMsgClientId().toString();
    }

    /**
     * Get the timeout to specify the maximum allowed duration of a sending in ms, by default 2000 ms.
     * 
     * @param timeout
     */
    @Override
    public final long getTimeout() {
        return this.TIMEOUT_REPLY;
    }

    /**
     * Set the timeout to specify the maximum allowed duration of a sending in ms, by default 2000 ms.
     * 
     * @param timeout
     */
    @Override
    public final void setTimeout(final long timeout) {
        this.TIMEOUT_REPLY = timeout;
    }

    /**
     * Gets Peers count.
     * 
     * @return the number of peers which are connected.
     */
    @Override
    public final int getConnectedPeersCount() {
        int result = 0;
        synchronized (peersConnect) {
            for (final Map.Entry<InetSocketAddress, PeerReconnection> entry : peersConnect.entrySet()) {
                if (entry.getValue().isConnected()) {
                    result++;
                }
            }
        }
        return result;
    }

    /**
     * Tells if the client is started.
     * 
     * @return <code>true</code> if started.
     */
    @Override
    public final boolean isStarted() {
        return clientStarted.get();
    }

    /**
     * Gets Peers.
     * 
     * @return the list of {@link MsgClientPeerAdm}
     */
    @Override
    public final MsgClientPeerAdm[] getPeers() {
        MsgClientPeerAdm[] msgClientPeerAdm;
        int i = 0;
        synchronized (peersConnect) {
            msgClientPeerAdm = new MsgClientPeerAdm[peerNodes.size()];
            for (final Map.Entry<UUID, InetSocketAddress> entry : peerNodes.entrySet()) {
                final InetSocketAddress address = entry.getValue();
                final PeerReconnection peerReconnection = peersConnect.get(address);
                msgClientPeerAdm[i++] = new MsgClientPeerAdm(entry.getKey(), address, peerReconnection.isConnected());
            }
        }
        return msgClientPeerAdm;
    }

    /**
     * Restart the client.
     * 
     */
    @Override
    public final void restart() {
        stop();
        start();
    }

    /**
     * Add new peer to the list of remote peers and connect to it if the client is started.
     * 
     * @param peer
     *            The peer to add, if null the function does nothing.
     * @throws IllegalArgumentException
     *             if peer references a node already added (with the same address or not).
     */
    public final void addPeer(final MsgNode peer) throws IllegalArgumentException {
        if (peer != null) {
            synchronized (peersConnect) {

                final UUID nodeId = peer.getNodeId();
                if (peerNodes.containsKey(nodeId)) {
                    throw new IllegalArgumentException(nodeId.toString());
                }
                final InetSocketAddress addr = peer.getAddress();
                peerNodes.put(nodeId, addr);
                peersConnect.put(addr, new PeerReconnection());
            }

            // Take a write lock as connect() accesses the channelGroup
            lockClientStarted.writeLock().lock();
            try {
                // Run a connection to that peer if the client is already started
                if (clientStarted.get()) {
                    connect(peer.getAddress());
                }
            }
            finally {
                lockClientStarted.writeLock().unlock();
            }
            LOGGER.debug("Msg client [{}] add peer '{}'", msgClientId, peer);
        }
    }

    /**
     * Remote a peer from the list of remote peers and shutdown the connection.
     * 
     * @param peer
     */
    public final void removePeer(final MsgNode peer) {
        if (peer != null) {

            final InetSocketAddress peerAddr = peer.getAddress();
            synchronized (peersConnect) {
                final UUID nodeId = peer.getNodeId();
                if (peerAddr.equals(peerNodes.get(nodeId))) {
                    peerNodes.remove(nodeId);
                    peersConnect.remove(peerAddr);
                }
                else {
                    throw new IllegalArgumentException("Node " + peer + " not found");
                }
            }

            lockClientStarted.writeLock().lock();
            try {
                for (final Channel channel : channelGroup) {
                    final InetSocketAddress remotePeer = (InetSocketAddress) channel.getRemoteAddress();
                    if (peerAddr.equals(remotePeer)) {
                        channel.close();
                        break;
                    }
                }
            }
            finally {
                lockClientStarted.writeLock().unlock();
            }
            MsgClientStartpoint.LOGGER.debug("Msg client [{}] remove peer '{}'", msgClientId, peer);
        }
    }

    /**
     * Tells if the node is connected.
     * 
     * @param nodeId
     * @return <code>true</code> if the node is known and connected
     */
    public final boolean isPeerConnected(final UUID nodeId) {
        final PeerReconnection peerReconnection;
        synchronized (peersConnect) {
            // Get peer address
            final InetSocketAddress addr = peerNodes.get(nodeId);
            if (addr == null) {
                return false;
            }
            peerReconnection = peersConnect.get(addr);
        }
        return peerReconnection.isConnected();
    }

    /**
     * Initialize the client and connect to remote peers.
     */
    public final void start() {

        lockClientStarted.writeLock().lock();
        try {
            if (clientStarted.get()) {
                throw new IllegalStateException("started");
            }

            // New timerRef
            if (timerRef.get() == null) {
                timerRef.set(new HashedWheelTimer());
            }

            // Create a new factory, with new thread pools
            nettyBoss = Executors.newCachedThreadPool(new NetThreadFactory("MsgClt[" + msgClientId + "]-b-"));
            nettyWorker = Executors.newCachedThreadPool(new NetThreadFactory("MsgClt[" + msgClientId + "]-w-"));
            factory = new NioClientSocketChannelFactory(nettyBoss, nettyWorker);

            msgClientSpareChannels.start();

            clientStarted.set(true);
        }
        finally {
            lockClientStarted.writeLock().unlock();
        }

        synchronized (peersConnect) {
            for (final Map.Entry<InetSocketAddress, PeerReconnection> peer : peersConnect.entrySet()) {
                peer.getValue().reset();
                connect(peer.getKey());
            }
        }

        LOGGER.info("Msg client [{}] started...", msgClientId);
    }

    /**
     * Close every channels and release allocated resources.
     */
    public final void stop() {

        final ExecutorService nettyBossTmp;
        final ExecutorService nettyWorkerTmp;
        final ChannelFactory factoryTmp;

        lockClientStarted.writeLock().lock();
        try {
            if (!clientStarted.get()) {
                return;
            }

            msgClientSpareChannels.stop();

            clientStarted.set(false);
            channelGroup.close().awaitUninterruptibly();
            channelGroup.clear();

            // Release factory and threads under shared lock to avoid deadlock with MsgClientHandler shutdown
            nettyBossTmp = nettyBoss;
            nettyBoss = null;
            nettyWorkerTmp = nettyWorker;
            nettyWorker = null;
            factoryTmp = factory;
            factory = null;
        }
        finally {
            lockClientStarted.writeLock().unlock();
        }

        lockClientStarted.readLock().lock();
        try {
            if (factoryTmp != null) {
                factoryTmp.releaseExternalResources();
            }
            else {
                if (nettyWorkerTmp != null) {
                    nettyWorkerTmp.shutdown();
                }
                if (nettyBossTmp != null) {
                    nettyBossTmp.shutdown();
                }
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }

        // Stop reconnection timerRef
        final Timer timerTmp = timerRef.getAndSet(null);
        if (timerTmp != null) {
            timerTmp.stop();
        }

        LOGGER.info("Msg client [{}] stopped...", msgClientId);
    }

    /**
     * Register the start point MXBean.
     * 
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     * @throws NotCompliantMBeanException
     * @throws MalformedObjectNameException
     * 
     * @return {@link ObjectName} of the MXBean
     * 
     */
    public final ObjectName registerMXBean(final MBeanServer mbeanServer) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException {
        final ObjectName clientObjName = new ObjectName(this.getClass().getPackage().getName() + ":type=Client");
        mbeanServer.registerMBean(this, clientObjName);
        return clientObjName;
    }

    /**
     * Send a Protobuf message in a synchronous manner. The method send a message to <b>all</b> peers and wait for
     * acknowledgments from <b>all</b> peers.
     * 
     * @param message
     *            The protobuf message
     * @return {@link Collection} of {@link MsgServerRemoteStatus} if errors occurs into remote peers or if some peers
     *         return an reply
     * @throws InterruptedException
     *             If the current thread is interrupted
     * @throws MsgServerTimeoutException
     *             If a the sending time out for <b>ALL</b> the peers.
     */
    public final Collection<MsgServerRemoteStatus> sendSyncMessage(@Nonnull final MessageLite message)
            throws MsgServerTimeoutException, InterruptedException {
        Objects.requireNonNull(message, "message");

        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                throw new IllegalStateException("stopped");
            }

            final Long msgId = Long.valueOf(currentMsgId.incrementAndGet());
            // CountDownLatch used to wait for acknowledgments
            final int count = channelGroup.size();
            final CountDownLatch countDownLatch = new CountDownLatch(count);
            mapQueue.put(msgId, countDownLatch);
            try {
                // Creation of the entry corresponding to the potential exceptions raised by remote peers
                mapExceptions.put(msgId, new ConcurrentLinkedQueue<MsgServerRemoteStatus>());
                try {
                    // Broadcast the message through the use of the channel group
                    final ChannelBuffer buffer = serializeMessage(message, msgId, true);
                    channelGroup.write(buffer);

                    // Wait for the application logic ACKs
                    final boolean receiveAllAcks = countDownLatch.await(TIMEOUT_REPLY, TimeUnit.MILLISECONDS);
                    if (!receiveAllAcks && countDownLatch.getCount() == count) {
                        throw new MsgServerTimeoutException("Msg time out: " + msgId);
                    }

                    final Collection<MsgServerRemoteStatus> msgServerRemoteStatus = mapExceptions.get(msgId);
                    return msgServerRemoteStatus;
                }
                finally {
                    // Remove the message entry from the map exceptions
                    mapExceptions.remove(msgId);
                }
            }
            finally {
                // Remove the message entry from the map queue
                mapQueue.remove(msgId);
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }
    }

    /**
     * Send a Protobuf message in a synchronous manner to given node. The method send a message to the peer and wait its
     * reply.
     * 
     * @param message
     *            The protobuf message
     * @return {@link MsgServerRemoteStatus} set if an error occurred on the remote peers or if the peer returns a reply
     * @throws InterruptedException
     *             If the current thread is interrupted
     * @throws MsgServerTimeoutException
     *             If a the sending time out.
     */
    public final MsgServerRemoteStatus sendSyncMessage(@Nonnull final UUID node, @Nonnull final MessageLite message)
            throws MsgServerTimeoutException, InterruptedException {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(message, "message");

        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                throw new IllegalStateException("stopped");
            }

            // Identify the destination channel
            Channel destination = null;
            {
                final InetSocketAddress addr = peerNodes.get(node);
                if (addr == null) {
                    throw new IllegalArgumentException("node=" + node);
                }
                // Should find the associated channel
                for (final Channel channel : channelGroup) {
                    if (addr.equals(channel.getRemoteAddress())) {
                        destination = channel;
                        break;
                    }
                }
                // Found?
                if (destination == null) {
                    throw new AssertionError("node=" + node + ", address=" + addr);
                }
            }

            final Long msgId = Long.valueOf(currentMsgId.incrementAndGet());
            // CountDownLatch used to wait for acknowledgments
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            mapQueue.put(msgId, countDownLatch);
            try {
                // Creation of the entry corresponding to the potential exceptions raised by the remote peer
                mapExceptions.put(msgId, new ConcurrentLinkedQueue<MsgServerRemoteStatus>());
                try {
                    // Send the message to the selected node
                    final ChannelBuffer buffer = serializeMessage(message, msgId, true);
                    destination.write(buffer);

                    // Wait for all application logic ACKs
                    final boolean receiveAllAcks = countDownLatch.await(TIMEOUT_REPLY, TimeUnit.MILLISECONDS);
                    if (!receiveAllAcks && countDownLatch.getCount() > 0) {
                        throw new MsgServerTimeoutException("Msg time out: " + msgId);
                    }

                    final Queue<MsgServerRemoteStatus> msgServerRemoteStatus = mapExceptions.get(msgId);
                    final int msgServerCount = msgServerRemoteStatus.size();
                    if (msgServerCount == 0) {
                        return null;
                    }
                    else if (msgServerCount == 1) {
                        return msgServerRemoteStatus.remove();
                    }
                    else {
                        throw new AssertionError("size=" + msgServerCount);
                    }
                }
                finally {
                    // Remove the message entry from the map exceptions
                    mapExceptions.remove(msgId);
                }
            }
            finally {
                // Remove the message entry from the map queue
                mapQueue.remove(msgId);
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }
    }

    /**
     * Send a Protobuf message in a synchronous manner to given node on a new {@link Channel}. This message may be long
     * to handle without blocking the others messages, but the message ordering is not guaranteed.
     * 
     * @param message
     *            The protobuf message
     * @return {@link MsgServerRemoteStatus} set if an error occurred on the remote peers or if the peer returns a reply
     * @throws ConnectException
     *             if a connection timeout occurs.
     */
    public final MsgServerRemoteStatus sendSyncMessageNewChannel(@Nonnull final UUID node,
            @Nonnull final MessageLite message) throws InterruptedException, ConnectException {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(message, "message");

        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                throw new IllegalStateException("stopped");
            }

            // Create a new channel
            final InetSocketAddress peerAddr = peerNodes.get(node);
            if (peerAddr == null) {
                throw new IllegalArgumentException("node=" + node);
            }

            final PeerChannel peerChannel = msgClientSpareChannels.getChannel(node, peerAddr);
            try {
                final Long msgId = Long.valueOf(currentMsgId.incrementAndGet());
                // CountDownLatch used to wait for acknowledgments
                final CountDownLatch countDownLatch = new CountDownLatch(1);
                mapQueue.put(msgId, countDownLatch);
                try {
                    // Creation of the entry corresponding to the potential exceptions raised by the remote peer
                    mapExceptions.put(msgId, new ConcurrentLinkedQueue<MsgServerRemoteStatus>());
                    try {
                        // Send the message to the selected node
                        final ChannelBuffer buffer = serializeMessage(message, msgId, true);
                        final Channel channel = peerChannel.getChannel();
                        checkChannelFuture(channel.write(buffer), TIMEOUT_CONNECT);

                        // Wait for peer ACK
                        countDownLatch.await();

                        final Queue<MsgServerRemoteStatus> msgServerRemoteStatus = mapExceptions.get(msgId);
                        final int msgServerCount = msgServerRemoteStatus.size();
                        if (msgServerCount == 0) {
                            return null;
                        }
                        else if (msgServerCount == 1) {
                            return msgServerRemoteStatus.remove();
                        }
                        else {
                            throw new AssertionError("size=" + msgServerCount);
                        }
                    }
                    finally {
                        // Remove the message entry from the map exceptions
                        mapExceptions.remove(msgId);
                    }
                }
                finally {
                    // Remove the message entry from the map queue
                    mapQueue.remove(msgId);
                }
            }
            finally {
                // Release channel for future use
                msgClientSpareChannels.releaseChannel(peerChannel);
            }
        }
        finally {
            lockClientStarted.readLock().unlock();
        }
    }

    /**
     * Send a Protobuf message in an asynchronous manner. The method send a message to all peers and doesn't wait for
     * acknowledgments from all peers.
     * 
     * @param message
     *            The protobuf message.
     */
    public final void sendAsyncMessage(@Nonnull final MessageLite message) {
        Objects.requireNonNull(message, "message");

        lockClientStarted.readLock().lock();
        try {
            if (!clientStarted.get()) {
                throw new IllegalStateException("stopped");
            }

            final Long msgId = Long.valueOf(currentMsgId.incrementAndGet());

            // Broadcast the message through the use of the channel group
            final ChannelBuffer buffer = serializeMessage(message, msgId, false);
            channelGroup.write(buffer);
        }
        finally {
            lockClientStarted.readLock().unlock();
        }
    }

    /**
     * Create a packed message for netty. The message will not be serialized again by netty.
     * 
     * @param message
     * @param msgId
     * @param sync
     * @return the packed message
     */
    private final ChannelBuffer serializeMessage(final MessageLite message, final Long msgId, final boolean sync) {
        // Serialize the protobuf wrapper message
        final MsgWrapper.MsgRequest msgRequest = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(msgId.longValue())
                .setSynchronous(sync).setMsgData(message.toByteString()).build();
        final byte[] msgSerialized = msgRequest.toByteArray();
        return ChannelBuffers.wrappedBuffer(msgSerialized);
    }

    /**
     * Connect to a peer.
     * 
     * @param peerAddr
     *            The address of the peer
     */
    private final void connect(final InetSocketAddress peerAddr) {
        final ChannelFuture channelFuture = newChannelFuture(peerAddr, true);
        channelGroup.add(channelFuture.getChannel());
    }

    /**
     * Create a new extra channel to a given peer
     * 
     * @param peerAddr
     * @return a new connected {@link Channel}.
     * @throws InterruptedException
     * @throws ConnectException
     */
    final Channel newSecondaryChannelFuture(final InetSocketAddress peerAddr) throws ConnectException,
            InterruptedException {
        final ChannelFuture peerChannelFuture = newChannelFuture(peerAddr, false);
        checkChannelFuture(peerChannelFuture, TIMEOUT_CONNECT);
        return peerChannelFuture.getChannel();
    }

    /**
     * Create a new {@link ChannelFuture} to a peer.
     * 
     * @param peerAddr
     *            The address of the peer
     */
    private final ChannelFuture newChannelFuture(final InetSocketAddress peerAddr, final boolean mainChannel) {
        final ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new MsgClientPipelineFactory(mainChannel, bootstrap));
        bootstrap.setOption("tcpNoDelay", Boolean.TRUE);
        bootstrap.setOption("keepAlive", Boolean.TRUE);
        bootstrap.setOption("remoteAddress", peerAddr);
        return bootstrap.connect(peerAddr);
    }

    /**
     * Check {@link ChannelFuture} completion.
     * 
     * @param channelFuture
     * @throws ConnectException
     * @throws InterruptedException
     */
    private final void checkChannelFuture(final ChannelFuture channelFuture, final long timeout)
            throws ConnectException, InterruptedException {
        if (channelFuture.await(timeout)) {
            assert channelFuture.isDone();
            if (!channelFuture.isSuccess()) {
                final ConnectException ce = new ConnectException("Failed");
                ce.initCause(channelFuture.getCause());
                throw ce;
            }
        }
        else {
            throw new ConnectException("Timeout");
        }
    }
}
