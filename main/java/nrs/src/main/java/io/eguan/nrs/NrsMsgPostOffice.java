package io.eguan.nrs;

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

import io.eguan.net.MsgClientStartpoint;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.nrs.NrsRemote;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsCluster;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsH1Header;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey.NrsKeyHeader;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsUpdate;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.UuidT;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Send messages to remote peers for {@link NrsAbstractFile} update or synchronization.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * @author ebredzinski
 */
final class NrsMsgPostOffice {

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsMsgPostOffice.class);

    /**
     * Messages update for a {@link NrsAbstractFile}.
     * 
     */
    private static class Msgs {
        private final UUID peerUuid;
        private final long expireTime;
        private final AtomicInteger count;
        private final RemoteOperation.Builder opBuilder;
        private final NrsRemote.NrsFileUpdate.Builder nuBuilder;
        /** true if the message must be sent in sync mode */
        private boolean sync = false;
        private final Lock syncLock;
        private Condition syncCond;

        Msgs(@Nonnull final UuidT<?> fileUuid, final UUID peerUuid, final boolean broadcast, final Lock syncLock) {
            super();
            this.peerUuid = peerUuid;
            this.expireTime = System.currentTimeMillis() + SEND_LIMIT_TIME;
            this.count = new AtomicInteger();
            this.opBuilder = RemoteOperation.newBuilder();
            this.nuBuilder = NrsRemote.NrsFileUpdate.newBuilder().setBroadcast(broadcast);
            this.syncLock = syncLock;

            // Initialize builder
            opBuilder.setUuid(newUuidT(fileUuid));
            opBuilder.setVersion(ProtocolVersion.VERSION_1);
            opBuilder.setType(Type.NRS);
            opBuilder.setOp(OpCode.SET);
        }

        final long getExpireTime() {
            return expireTime;
        }

        final UUID getPeer() {
            return peerUuid;
        }

        final boolean isSync() {
            return sync;
        }

        /**
         * Initialize the sending of the message in wait mode.
         */
        final void initSync() {
            assert peerUuid == null;
            assert syncLock != null;
            assert syncCond == null;

            this.sync = true;
            syncCond = syncLock.newCondition();
            syncLock.lock();
        }

        final void waitSync() throws InterruptedException {
            try {
                syncCond.await();
            }
            finally {
                syncLock.unlock();
            }
        }

        final void doneSync() {
            syncLock.lock();
            try {
                syncCond.signal();
            }
            finally {
                syncLock.unlock();
            }
        }

        final boolean isFull() {
            return count.get() >= SEND_LIMIT_COUNT;
        }

        /**
         * Add a new {@link NrsUpdate} message to send.
         * 
         * @param nrsUpdate
         */
        final void add(final NrsUpdate nrsUpdate) {
            nuBuilder.addUpdates(nrsUpdate);
            count.incrementAndGet();
        }

        /**
         * Set the end-of-sync field of the message to send.
         * 
         * @param aborted
         *            if the synchronization have been aborted.
         */
        final void setEOS(final boolean aborted) {
            nuBuilder.setEos(true);
            nuBuilder.setAborted(aborted);
            count.incrementAndGet();
        }

        /**
         * Tells if the end-of-sync have been reached.
         * 
         * @return true if EOS is set.
         */
        final boolean isEOS() {
            if (nuBuilder.hasEos()) {
                assert nuBuilder.getEos();
                assert nuBuilder.hasAborted();
                return true;
            }
            return false;
        }

        /**
         * Complete the building of the messages to send
         * 
         * @return the builder of the message ready to be sent.
         */
        final RemoteOperation.Builder getBuilder() {
            opBuilder.setNrsFileUpdate(nuBuilder);
            return opBuilder;
        }
    }

    private class MsgsSender implements Runnable {

        private static final int EMPTY_COUNT_LIMIT = 6;
        /** Hit count of empty message list before stopping the sender task */
        private int emptyCount = 0;

        /** true when the sender thread is started */
        private final AtomicBoolean started = new AtomicBoolean(false);
        /** Set to true when the runnable have to stop */
        private final AtomicBoolean shutdown = new AtomicBoolean(false);

        private final LinkedBlockingQueue<Msgs> toSendQueue = new LinkedBlockingQueue<>();

        MsgsSender() {
            super();
        }

        final void start() {
            if (started.getAndSet(true)) {
                // Already started
                throw new IllegalStateException("started");
            }
            shutdown.set(false);
            final Thread thread = new Thread(this, senderName);
            thread.setDaemon(true);
            thread.start();
        }

        final boolean isStarted() {
            return started.get();
        }

        final void stop() {
            shutdown.set(true);
        }

        @Override
        public final void run() {
            try {
                while (!shutdown.get()) {
                    // Post expired messages (timeout)
                    if (fileMessagesLock.tryLock()) {
                        try {
                            sendBroadcastMessages(null, false, false);
                            sendUnicastMessages(null, null, false);
                        }
                        finally {
                            fileMessagesLock.unlock();
                        }
                    }

                    // Send on wire the messages
                    long duration = SEND_LIMIT_TIME / 2;
                    final long end = System.currentTimeMillis() + duration;
                    do {
                        try {
                            final Msgs toSend = toSendQueue.poll(duration, TimeUnit.MILLISECONDS);
                            if (toSend != null) {
                                send(toSend);
                            }
                        }
                        catch (final InterruptedException e) {
                            // Should not happen
                            LOGGER.warn("Thread interrupted (ignored)");
                        }
                        duration = end - System.currentTimeMillis();
                    } while (duration > 0 || !toSendQueue.isEmpty());

                    // Cancel sender if the maps are empty (will be restarted when needed)
                    if (fileMessagesLock.tryLock()) {
                        try {
                            if (fileMessages.isEmpty() && filePeerMessages.isEmpty()) {
                                emptyCount++;
                                if (emptyCount > EMPTY_COUNT_LIMIT) {
                                    emptyCount = 0;
                                    return;
                                }
                            }
                            else {
                                emptyCount = 0;
                            }
                        }
                        finally {
                            fileMessagesLock.unlock();
                        }
                    }
                }
            }
            finally {
                started.set(false);
            }
        }

        /**
         * Post the message on the wire. Does not wait for the end of the transmission.
         * 
         * @param msgs
         *            message to send
         */
        final void post(final Msgs msgs) {
            post(msgs, false);
        }

        /**
         * Post the message on the wire and may wait for the end of the transmission.
         * 
         * @param msgs
         *            message to send
         * @param wait
         *            <code>true</code> to wait for the end of the message transmission
         */
        final void post(final Msgs msgs, final boolean wait) {
            if (wait) {
                msgs.initSync();
                try {
                    toSendQueue.add(msgs);
                }
                finally {
                    try {
                        msgs.waitSync();
                    }
                    catch (final InterruptedException e) {
                        // Ignored
                        LOGGER.warn("Interrupted while sending messages", e);
                    }
                }
            }
            else {
                toSendQueue.add(msgs);
            }
        }

        /**
         * Send a message on the wire.
         * 
         * @param toSend
         */
        private final void send(final Msgs toSend) {
            final UUID peer = toSend.getPeer();
            final RemoteOperation.Builder builder = toSend.getBuilder();
            enhancer.enhance(builder);
            if (peer != null) {
                try {
                    startpoint.sendSyncMessageNewChannel(peer, builder.build());
                }
                catch (final Exception e) {
                    LOGGER.warn("Error while sending messages from " + startpoint.getMsgClientId() + " to " + peer, e);
                }
            }
            else {
                if (toSend.isSync()) {
                    try {
                        startpoint.sendSyncMessage(builder.build());
                    }
                    catch (final Exception e) {
                        LOGGER.warn("Error while sending messages from " + startpoint.getMsgClientId(), e);
                    }
                    finally {
                        toSend.doneSync();
                    }
                }
                else {
                    startpoint.sendAsyncMessage(builder.build());
                }
            }
        }

        final void sendFileMessages(final UuidT<?> fileUuid, final boolean sync) {
            sendBroadcastMessages(fileUuid, false, sync);
            sendUnicastMessages(fileUuid, null, false);
        }

        final void sendAllMessages() {
            sendBroadcastMessages(null, true, false);
            sendUnicastMessages(null, null, true);
        }

        final void sendFilePeerMessages(final UuidT<?> fileUuid, final UUID peerUuid) {
            sendUnicastMessages(fileUuid, peerUuid, false);
        }

        private final void sendBroadcastMessages(final UuidT<?> fileUuid, final boolean all, final boolean sync) {
            final long now = System.currentTimeMillis();
            boolean fileMsgsFound = false;

            fileMessagesLock.lock();
            try {
                for (final Iterator<Map.Entry<UuidT<?>, Msgs>> iterator = fileMessages.entrySet().iterator(); iterator
                        .hasNext();) {
                    final Map.Entry<UuidT<?>, Msgs> entry = iterator.next();
                    final Msgs msgs = entry.getValue();
                    final boolean fileMsgs = entry.getKey().equals(fileUuid);
                    final boolean expired = all || fileMsgs || now >= msgs.getExpireTime() || msgs.isFull();
                    if (expired) {
                        iterator.remove();
                        // Lock the msgs before removal from the map
                        post(msgs, sync);
                        // Check if the messages are for the specified file
                        fileMsgsFound |= fileMsgs;
                    }
                }
            }
            finally {
                fileMessagesLock.unlock();
            }

            // If the caller requested the synchronization on the messages for a given file, send an empty list if no
            // messages have been found
            if (fileUuid != null && sync && !fileMsgsFound) {
                final Msgs empty = new Msgs(fileUuid, null, true, fileMessagesLock);
                post(empty, sync);
            }
        }

        private final void sendUnicastMessages(final UuidT<?> fileUuid, final UUID peerUuid, final boolean all) {
            final long now = System.currentTimeMillis();

            fileMessagesLock.lock();
            try {
                if (!filePeerMessages.isEmpty()) {
                    for (final Iterator<Map.Entry<UuidT<?>, Map<UUID, Msgs>>> iterator = filePeerMessages.entrySet()
                            .iterator(); iterator.hasNext();) {
                        final Map.Entry<UuidT<?>, Map<UUID, Msgs>> entry = iterator.next();
                        final UuidT<?> currentNrsAbstractFileUuid = entry.getKey();
                        final Map<UUID, Msgs> peerMsgs = entry.getValue();

                        for (final Iterator<Map.Entry<UUID, Msgs>> iterator2 = peerMsgs.entrySet().iterator(); iterator2
                                .hasNext();) {
                            final Map.Entry<UUID, Msgs> entry2 = iterator2.next();
                            final UUID msgsPeer = entry2.getKey();
                            final Msgs msgs = entry2.getValue();

                            final boolean expired = all || currentNrsAbstractFileUuid.equals(fileUuid)
                                    || msgsPeer.equals(peerUuid) || now >= msgs.getExpireTime() || msgs.isFull();
                            if (expired) {
                                iterator2.remove();
                                post(msgs);
                                // Add a new empty msgs if the eos have not been reached
                                if (!msgs.isEOS()) {
                                    ensurePeerMsgs(currentNrsAbstractFileUuid, msgsPeer);
                                }
                            }
                        }

                        // Prune empty map
                        if (peerMsgs.isEmpty()) {
                            iterator.remove();
                        }
                    }
                }
            }
            finally {
                fileMessagesLock.unlock();
            }
        }
    }

    /** Maximum limit to send a list of messages */
    static final int SEND_LIMIT_COUNT = Integer.getInteger("io.eguan.nrs.sendLimitCount", Integer.valueOf(64))
            .intValue(); // 64 by default

    /** Maximum duration before sending a pending message (in milliseconds) */
    static final long SEND_LIMIT_TIME = Long.getLong("io.eguan.nrs.sendLimitTime", Long.valueOf(5))
            .longValue() * 1000L; // 5s by default

    /** Notify remote peers */
    private final MsgClientStartpoint startpoint;
    private final NrsMsgEnhancer enhancer;

    /** Messages locker */
    private final ReentrantLock fileMessagesLock;

    /**
     * Messages for each {@link NrsAbstractFile}. The key is the uuid of the file, the value is the current message
     * builder for this file.
     */
    @GuardedBy(value = "fileMessagesLock")
    private final Map<UuidT<?>, Msgs> fileMessages;

    /** Same as {@link #fileMessages}, but for the update of a peer. */
    @GuardedBy(value = "fileMessagesLock")
    private final Map<UuidT<?>, Map<UUID, Msgs>> filePeerMessages;

    /** Task sending messages. */
    @GuardedBy(value = "fileMessagesLock")
    private final MsgsSender msgsSender;
    private final String senderName;

    NrsMsgPostOffice(@Nonnull final MsgClientStartpoint startpoint, final NrsMsgEnhancer enhancer) {
        super();
        this.startpoint = Objects.requireNonNull(startpoint);
        this.enhancer = enhancer;
        final UUID sourceUUID = startpoint.getMsgClientId();
        this.senderName = "NrsMessage sender " + sourceUUID;
        this.fileMessages = new HashMap<>();
        this.filePeerMessages = new HashMap<>();
        this.fileMessagesLock = new ReentrantLock();
        this.msgsSender = new MsgsSender();
    }

    /**
     * Send the pending messages for the given {@link NrsAbstractFile}.
     * 
     * @param fileUuid
     *            {@link UUID} of the file related file.
     */
    final void flush(final UuidT<?> fileUuid) {
        fileMessagesLock.lock();
        try {
            // Send the pending messages for the given file
            startSender();
            msgsSender.sendFileMessages(fileUuid, true);
        }
        finally {
            fileMessagesLock.unlock();
        }
    }

    /**
     * Send the pending messages.
     */
    final void flush() {
        fileMessagesLock.lock();
        try {
            // Cancel senderRef if any then run the task that sends messages
            if (!stopSender()) {
                // No message left
                return;
            }
            // Send all the pending messages
            msgsSender.sendAllMessages();
        }
        finally {
            fileMessagesLock.unlock();
        }
    }

    /**
     * Initialize a session to send cluster and key update to a given peer.
     * 
     * @param fileUuid
     *            file to update
     * @param peerUuid
     *            destination peer.
     */
    final void initPeerSync(final UuidT<?> fileUuid, final UUID peerUuid) {
        fileMessagesLock.lock();
        try {
            // Create the Msgs object to start the notifications to the peer (keep alive)
            ensurePeerMsgs(fileUuid, peerUuid);
            startSender();
        }
        finally {
            fileMessagesLock.unlock();
        }

    }

    /**
     * Close the update session and release resources.
     * 
     * @param fileUuid
     *            file to update
     * @param peerUuid
     *            destination peer.
     * @param aborted
     *            <code>true</code> true if the {@link NrsAbstractFile} scan have been aborted.
     */
    final void finiPeerSync(final UuidT<?> fileUuid, final UUID peerUuid, final boolean aborted) {
        // Send end-of-sync message
        fileMessagesLock.lock();
        try {
            final Msgs msgs = ensurePeerMsgs(fileUuid, peerUuid);
            msgs.setEOS(aborted);
            msgsSender.sendFilePeerMessages(fileUuid, peerUuid);
        }
        finally {
            fileMessagesLock.unlock();
        }
    }

    /**
     * Update one cluster.
     * 
     * @param fileUuid
     *            file to update
     * @param peerUuid
     *            destination peer.
     * @param index
     * @param contents
     */
    final void postNrsCluster(final UuidT<?> fileUuid, final UUID peerUuid, final long index, final ByteBuffer contents) {
        // Create NrsCluster message
        final NrsCluster nrsCluster;
        {
            final NrsCluster.Builder builder = NrsCluster.newBuilder();
            builder.setIndex(index);
            builder.setContents(ByteString.copyFrom(contents));
            nrsCluster = builder.build();
        }
        final NrsUpdate.Builder builder = NrsUpdate.newBuilder();
        builder.setClusterUpdate(nrsCluster);

        postNrsUpdate(fileUuid, peerUuid, builder.build(), false);
    }

    /**
     * Update the H1 header of a {@link NrsAbstractFile}.
     * 
     * @param fileUuid
     * @param peerUuid
     * @param contents
     */
    void postNrsHeader(final UuidT<?> fileUuid, final UUID peerUuid, final ByteBuffer contents) {
        // Create NrsH1Header message
        final NrsH1Header nrsHeader;
        {
            final NrsH1Header.Builder builder = NrsH1Header.newBuilder();
            builder.setHeader(ByteString.copyFrom(contents));
            nrsHeader = builder.build();
        }
        final NrsUpdate.Builder builder = NrsUpdate.newBuilder();
        builder.setH1HeaderUpdate(nrsHeader);

        postNrsUpdate(fileUuid, peerUuid, builder.build(), true);
    }

    private final void postNrsUpdate(final UuidT<?> fileUuid, final UUID peerUuid, final NrsUpdate nrsUpdate,
            final boolean force) {

        // Get/create NrsUpdate message list
        fileMessagesLock.lock();
        try {
            final Msgs msgs = ensurePeerMsgs(fileUuid, peerUuid);
            msgs.add(nrsUpdate);
            if (force || msgs.isFull()) {
                msgsSender.sendFilePeerMessages(fileUuid, peerUuid);
            }
        }
        finally {
            fileMessagesLock.unlock();
        }
    }

    /**
     * Send a message to notify a key update to peers.
     * 
     * @param fileUuid
     *            file to update
     * @param version
     * @param blockIndex
     * @param key
     *            value to send. May be <code>null</code>, a byte array or a {@link ByteBuffer}.
     */
    final void postNrsKey(final UuidT<?> fileUuid, final long version, final long blockIndex,
            final NrsKeyHeader header, final Object key) {
        // Create NrsKey message
        final NrsKey nrsKey;
        {
            final NrsKey.Builder builder = NrsKey.newBuilder();
            builder.setVersion(version);
            builder.setBlockIndex(blockIndex);
            builder.setHeader(header);
            if (key != null) {
                if (key instanceof byte[]) {
                    builder.setKey(ByteString.copyFrom((byte[]) key));
                }
                else if (key instanceof ByteBuffer) {
                    builder.setKey(ByteString.copyFrom((ByteBuffer) key));
                }
                else {
                    throw new AssertionError("key=" + key.getClass());
                }
            }
            nrsKey = builder.build();
        }
        final NrsUpdate.Builder builder = NrsUpdate.newBuilder();
        builder.setKeyUpdate(nrsKey);

        postNrsUpdate(fileUuid, builder.build());
    }

    /**
     * Post a message update for the given file. The message is added in the list for broadcast messages and for the
     * update of a file on a peer node.
     * 
     * @param fileUuid
     * @param nrsUpdate
     */
    private final void postNrsUpdate(final UuidT<?> fileUuid, final NrsUpdate nrsUpdate) {

        fileMessagesLock.lock();
        try {
            // Add the new NrsFileUpdate for broadcast
            final Msgs msgs = ensureMsgs(fileUuid);
            msgs.add(nrsUpdate);
            boolean full = msgs.isFull();

            // Look for messages for peers
            final Map<UUID, Msgs> msgsPeersMap = filePeerMessages.get(fileUuid);
            if (msgsPeersMap != null) {
                final Collection<Msgs> msgsCollection = msgsPeersMap.values();
                for (final Msgs msgsPeer : msgsCollection) {
                    msgsPeer.add(nrsUpdate);
                    full |= msgsPeer.isFull();
                }
            }
            if (full) {
                msgsSender.sendFileMessages(fileUuid, false);
            }
        }
        finally {
            fileMessagesLock.unlock();
        }
    }

    /**
     * Starts the message sender if necessary.
     */
    private final void startSender() {
        assert fileMessagesLock.isHeldByCurrentThread();

        if (!msgsSender.isStarted()) {
            msgsSender.start();
        }
    }

    /**
     * Stops the message sender.
     * 
     * @return <code>true</code> if the sender have been stopped.
     */
    private final boolean stopSender() {
        assert fileMessagesLock.isHeldByCurrentThread();

        if (fileMessages.isEmpty() && filePeerMessages.isEmpty()) {
            // TODO: wait for end of thread?
            msgsSender.stop();
            return true;
        }
        return false;
    }

    private final Map<UUID, Msgs> ensureFilePeerMap(final UuidT<?> fileUuid, final UUID peerUuid) {
        assert fileMessagesLock.isHeldByCurrentThread();

        // Get or create a map for this file
        Map<UUID, Msgs> peerMap = filePeerMessages.get(fileUuid);
        if (peerMap == null) {
            peerMap = new ConcurrentHashMap<>();
            filePeerMessages.put(fileUuid, peerMap);
        }
        return peerMap;
    }

    private final Msgs ensurePeerMsgs(final UuidT<?> fileUuid, final UUID peerUuid) {
        assert fileMessagesLock.isHeldByCurrentThread();

        final Map<UUID, Msgs> peerMap = ensureFilePeerMap(fileUuid, peerUuid);
        // Get or create a Msgs for this peer and file
        Msgs msgs = peerMap.get(peerUuid);
        if (msgs == null) {
            msgs = new Msgs(fileUuid, peerUuid, false, fileMessagesLock);
            peerMap.put(peerUuid, msgs);
        }
        return msgs;
    }

    private final Msgs ensureMsgs(final UuidT<?> fileUuid) {
        assert fileMessagesLock.isHeldByCurrentThread();

        Msgs msgs = fileMessages.get(fileUuid);
        if (msgs == null) {
            msgs = new Msgs(fileUuid, null, true, fileMessagesLock);
            fileMessages.put(fileUuid, msgs);
            if (fileMessages.size() == 1) {
                // First message list: need to start the sender
                startSender();
            }
        }
        return msgs;
    }

    /**
     * Utility method: {@link UUID} to {@link Uuid}.
     * 
     * @param uuid
     * @return {@link Uuid} corresponding to <code>uuid</code>
     */
    static final Uuid newUuid(@Nonnull final UUID uuid) {
        return Uuid.newBuilder().setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits()).build();
    }

    /**
     * Utility method: {@link UuidT} to {@link Uuid}.
     * 
     * @param uuid
     * @return {@link Uuid} corresponding to <code>uuid</code>
     */
    static final Uuid newUuidT(@Nonnull final UuidT<?> uuid) {
        return Uuid.newBuilder().setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits()).build();
    }

}
