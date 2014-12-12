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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.oodrive.nuage.net.MsgClientSpareChannels.PeerChannel;
import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.net.MsgWrapper;
import com.oodrive.nuage.proto.net.MsgWrapper.MsgRequest;

public class TestMessagingService {
    public static final Logger LOGGER = LoggerFactory.getLogger(TestMessagingService.class.getSimpleName());

    private final MsgNode SERVER_1 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55255));
    private final MsgNode SERVER_2 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55256));

    private final boolean TEST_REPLY_STATUS = true;
    private final long TEST_REPLY_ID = 12L;

    final class MsgHandlerTestExpectedObject implements MsgServerHandler {

        private final Object expected;
        private final boolean reply;
        private final long replyId;
        private boolean success = false;

        MsgHandlerTestExpectedObject(final Object expected) {
            this(expected, false);
        }

        MsgHandlerTestExpectedObject(final Object expected, final boolean reply) {
            this(expected, reply, TEST_REPLY_ID);
        }

        MsgHandlerTestExpectedObject(final Object expected, final boolean reply, final long replyId) {
            this.expected = expected;
            this.reply = reply;
            this.replyId = replyId;
        }

        public final boolean isSuccess() {
            return success;
        }

        public final void resetSuccess() {
            success = false;
        }

        @Override
        public final MessageLite handleMessage(final MessageLite message) {
            final MsgWrapper.MsgRequest msgRequest = (MsgRequest) message;
            success = expected.equals(msgRequest.getMsgData());
            if (reply) {
                final MsgWrapper.MsgReply.Builder builder = MsgWrapper.MsgReply.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(replyId)
                        .setStatus(TEST_REPLY_STATUS);
                return builder.build();
            }
            else {
                return null;
            }
        }
    }

    @Test
    public void testSynchronousRequestTwoServers() throws Throwable {
        LOGGER.info("Run testSynchronousRequestTwoServers()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    msgClientStartpoint.setTimeout(10000);
                    waitConnected(2, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testSynchronousRequestClientStartedBeforeServer() throws Throwable {
        LOGGER.info("Run testSynchronousRequestClientStartedBeforeServer()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1, request);
        serverEndpoint1.start();

        try {
            final List<MsgNode> peers = new ArrayList<>(2);
            peers.add(SERVER_1);
            peers.add(SERVER_2);
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
            msgClientStartpoint.start();

            try {
                final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
                final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2, request);
                serverEndpoint2.start();
                waitConnected(2, msgClientStartpoint);

                try {
                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());
                }
                finally {
                    serverEndpoint2.stop();
                }
            }
            finally {
                msgClientStartpoint.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testSynchronousRequestOnePeer() throws Throwable {
        LOGGER.info("Run testSynchronousRequestOnePeer()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            // Expected: should fail (message should not be received)
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    Assert.assertNull(msgClientStartpoint.sendSyncMessage(SERVER_1.getNodeId(), request));
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertFalse(msgHandlerImpl2.isSuccess());
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testSynchronousRequestReplyOnePeer() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeer()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString, true);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            // Expected: should fail (message should not be received)
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    final MsgServerRemoteStatus status = msgClientStartpoint.sendSyncMessage(SERVER_1.getNodeId(),
                            request);
                    Assert.assertNotNull(status);
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertFalse(msgHandlerImpl2.isSuccess());

                    // Read reply
                    final ByteString reply = status.getReplyBytes();
                    Assert.assertNotNull(reply);
                    Assert.assertEquals(SERVER_1.getNodeId(), status.getNodeId());
                    Assert.assertEquals(SERVER_1.getAddress(), status.getRemotePeer());
                    Assert.assertNull(status.getExceptionName());

                    final MsgWrapper.MsgReply msg = MsgWrapper.MsgReply.newBuilder().mergeFrom(reply).build();
                    Assert.assertTrue(msg.getStatus());
                    Assert.assertEquals(TEST_REPLY_ID, msg.getMsgId());
                    Assert.assertFalse(msg.hasRepData());

                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testSynchronousRequestReplyPeers() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyPeers()");

        final long expectedId1 = 12L;
        final long expectedId2 = 13L;

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString, true,
                expectedId1);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString, true,
                    expectedId2);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    final Collection<MsgServerRemoteStatus> status = msgClientStartpoint.sendSyncMessage(request);
                    Assert.assertNotNull(status);
                    Assert.assertEquals(2, status.size());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());

                    // Read reply
                    boolean found1 = false;
                    boolean found2 = false;
                    for (final MsgServerRemoteStatus msgServerRemoteStatus : status) {
                        final SocketAddress peer = msgServerRemoteStatus.getRemotePeer();
                        Assert.assertNotNull(peer);
                        final ByteString reply = msgServerRemoteStatus.getReplyBytes();
                        Assert.assertNotNull(reply);
                        Assert.assertNull(msgServerRemoteStatus.getExceptionName());

                        final MsgWrapper.MsgReply msg = MsgWrapper.MsgReply.newBuilder().mergeFrom(reply).build();
                        Assert.assertTrue(msg.getStatus());

                        if (peer.equals(SERVER_1.getAddress())) {
                            Assert.assertEquals(SERVER_1.getNodeId(), msgServerRemoteStatus.getNodeId());
                            Assert.assertEquals(expectedId1, msg.getMsgId());
                            found1 = true;
                        }
                        else if (peer.equals(SERVER_2.getAddress())) {
                            Assert.assertEquals(SERVER_2.getNodeId(), msgServerRemoteStatus.getNodeId());
                            Assert.assertEquals(expectedId2, msg.getMsgId());
                            found2 = true;
                        }
                        else {
                            throw new AssertionFailedError("Unknown address: " + peer);
                        }
                        Assert.assertFalse(msg.hasRepData());
                    }
                    Assert.assertTrue(found1);
                    Assert.assertTrue(found2);
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testSynchronousRequestReplyOnePeerNewChannel() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeerNewChannel()");

        final long id1 = 123L;
        final long id2 = 456L;
        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request1 = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(id1).setSynchronous(true)
                .setMsgData(expectedString).build();
        final MsgWrapper.MsgRequest request2 = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(id2).setSynchronous(true)
                .setMsgData(expectedString).build();

        // Send reply immediately
        final MsgServerHandler msgHandlerImpl1 = new MsgServerHandler() {
            @Override
            public final MessageLite handleMessage(final MessageLite message) {
                final MsgWrapper.MsgRequest msgRequest = (MsgRequest) message;
                final long msgId = msgRequest.getMsgId();
                final MsgWrapper.MsgReply.Builder builder = MsgWrapper.MsgReply.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(msgId * 40L)
                        .setStatus(TEST_REPLY_STATUS);
                return builder.build();
            }
        };
        // Send reply after some time
        final long timeout = 6 * 1000;
        final MsgServerHandler msgHandlerImpl2 = new MsgServerHandler() {
            @Override
            public final MessageLite handleMessage(final MessageLite message) {
                final MsgWrapper.MsgRequest msgRequest = (MsgRequest) message;
                final long msgId = msgRequest.getMsgId();
                final MsgWrapper.MsgReply.Builder builder = MsgWrapper.MsgReply.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(msgId * 40L)
                        .setStatus(TEST_REPLY_STATUS);

                // Wait before replying
                try {
                    Thread.sleep(timeout);
                }
                catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return builder.build();
            }
        };
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            // Expected: should fail (message should not be received)
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();

                try {
                    msgClientStartpoint.setTimeout(timeout * 2);
                    waitConnected(2, msgClientStartpoint);

                    // Send the messages in background
                    final Thread[] senders = new Thread[2];
                    final long[] requestTime = new long[2];
                    @SuppressWarnings("unchecked")
                    final Collection<MsgServerRemoteStatus>[] statusCollection = new Collection[2];
                    final Throwable[] senderErr = new Throwable[2];

                    // Synchronized message for both
                    senders[0] = new Thread(new Runnable() {
                        @Override
                        public final void run() {
                            try {
                                statusCollection[0] = msgClientStartpoint.sendSyncMessage(request1);
                                requestTime[0] = System.currentTimeMillis();
                                LOGGER.info("Reply #0");
                            }
                            catch (final Throwable t) {
                                senderErr[0] = t;
                            }
                        }
                    }, "Sender1");

                    // Synchronized message for S1 on a new channel
                    senders[1] = new Thread(new Runnable() {
                        @Override
                        public final void run() {
                            try {
                                statusCollection[1] = new ArrayList<MsgServerRemoteStatus>(1);
                                statusCollection[1].add(msgClientStartpoint.sendSyncMessageNewChannel(
                                        SERVER_1.getNodeId(), request2));
                                requestTime[1] = System.currentTimeMillis();
                                LOGGER.info("Reply #1");
                            }
                            catch (final Throwable t) {
                                senderErr[1] = t;
                            }
                        }
                    }, "Sender2");

                    senders[0].start();
                    Thread.sleep(timeout / 5L);
                    senders[1].start();
                    senders[0].join();
                    senders[1].join(); // this thread should be terminated

                    // Error?
                    for (int i = 0; i < senderErr.length; i++) {
                        final Throwable throwable = senderErr[i];
                        if (throwable != null) {
                            throw new AssertionError("Sender #" + i + " failed", throwable);
                        }
                    }

                    // The reply of the second message should be arrived first
                    Assert.assertTrue(requestTime[0] > requestTime[1]);

                    // Check replies
                    // Reply for request1
                    Assert.assertEquals(2, statusCollection[0].size());
                    for (final MsgServerRemoteStatus status : statusCollection[0]) {
                        final UUID nodeId = status.getNodeId();
                        if (SERVER_1.getNodeId().equals(nodeId)) {
                            Assert.assertEquals(SERVER_1.getAddress(), status.getRemotePeer());
                        }
                        else if (SERVER_2.getNodeId().equals(nodeId)) {
                            Assert.assertEquals(SERVER_2.getAddress(), status.getRemotePeer());
                        }
                        else {
                            throw new AssertionFailedError("Unexpected nodeId=" + nodeId);
                        }
                        Assert.assertNull(status.getExceptionName());

                        // Read reply
                        final ByteString reply = status.getReplyBytes();
                        Assert.assertNotNull(reply);
                        final MsgWrapper.MsgReply msg = MsgWrapper.MsgReply.newBuilder().mergeFrom(reply).build();
                        Assert.assertTrue(msg.getStatus());
                        Assert.assertEquals(id1 * 40L, msg.getMsgId());
                        Assert.assertFalse(msg.hasRepData());
                    }

                    // Reply for request2
                    Assert.assertEquals(1, statusCollection[1].size());
                    for (final MsgServerRemoteStatus status : statusCollection[1]) {
                        final UUID nodeId = status.getNodeId();
                        Assert.assertEquals(SERVER_1.getNodeId(), nodeId);
                        Assert.assertEquals(SERVER_1.getAddress(), status.getRemotePeer());
                        Assert.assertNull(status.getExceptionName());

                        // Read reply
                        final ByteString reply = status.getReplyBytes();
                        Assert.assertNotNull(reply);
                        final MsgWrapper.MsgReply msg = MsgWrapper.MsgReply.newBuilder().mergeFrom(reply).build();
                        Assert.assertTrue(msg.getStatus());
                        Assert.assertEquals(id2 * 40L, msg.getMsgId());
                        Assert.assertFalse(msg.hasRepData());
                    }

                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    /**
     * Test that a spare channel is reused.
     * 
     * @throws Throwable
     */
    @Test
    public void testSynchronousRequestNewChannelReuse() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeerNewChannelReuse()");

        final PeerChannel p14;
        final List<MsgNode> peers = new ArrayList<>(2);
        peers.add(SERVER_1);
        peers.add(SERVER_2);
        final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
        msgClientStartpoint.start();

        try {

            final ByteString expectedString1 = ByteString.copyFrom("hello test".getBytes());
            final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                    .setSynchronous(true).setMsgData(expectedString1).build();

            final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString1);
            final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint1.start();

            try {
                // Expected: should fail (message should not be received)
                final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString1);
                final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                        MsgWrapper.MsgRequest.getDefaultInstance());
                serverEndpoint2.start();

                try {
                    waitConnected(2, msgClientStartpoint);

                    // Create some channel, check re-use
                    final MsgClientSpareChannels clientSpareChannels = msgClientStartpoint.getMsgClientSpareChannels();
                    final PeerChannel p11 = clientSpareChannels.getChannel(SERVER_1.getNodeId(), SERVER_1.getAddress());
                    clientSpareChannels.releaseChannel(p11);

                    // Reuse same channel or may open a new one
                    PeerChannel p12;
                    final PeerChannel p11bis = clientSpareChannels.getChannel(SERVER_1.getNodeId(),
                            SERVER_1.getAddress());
                    try {
                        p12 = clientSpareChannels.getChannel(SERVER_1.getNodeId(), SERVER_1.getAddress());
                        clientSpareChannels.releaseChannel(p12);
                    }
                    finally {
                        clientSpareChannels.releaseChannel(p11bis);
                    }
                    Assert.assertSame(p11, p11bis);
                    Assert.assertNotSame(p11, p12);
                    Assert.assertEquals(SERVER_1.getNodeId(), p11.getNode());
                    Assert.assertEquals(SERVER_1.getNodeId(), p12.getNode());
                    Assert.assertEquals(2, clientSpareChannels.getChannelCount());

                    final PeerChannel p21 = clientSpareChannels.getChannel(SERVER_2.getNodeId(), SERVER_2.getAddress());
                    clientSpareChannels.releaseChannel(p21);
                    Assert.assertEquals(SERVER_2.getNodeId(), p21.getNode());
                    Assert.assertEquals(3, clientSpareChannels.getChannelCount());

                    Assert.assertNull(msgClientStartpoint.sendSyncMessageNewChannel(SERVER_1.getNodeId(), request));
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertFalse(msgHandlerImpl2.isSuccess());

                    msgHandlerImpl1.resetSuccess();

                    // Wait for channels to be closed (3s here)
                    Thread.sleep(3500);
                    Assert.assertEquals(0, clientSpareChannels.getChannelCount());

                    Assert.assertNull(msgClientStartpoint.sendSyncMessageNewChannel(SERVER_1.getNodeId(), request));
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertFalse(msgHandlerImpl2.isSuccess());

                    Assert.assertEquals(1, clientSpareChannels.getChannelCount());

                    final PeerChannel p13 = clientSpareChannels.getChannel(SERVER_1.getNodeId(), SERVER_1.getAddress());
                    clientSpareChannels.releaseChannel(p13);
                    Assert.assertNotSame(p13, p11);
                    Assert.assertNotSame(p13, p12);
                    Assert.assertEquals(SERVER_1.getNodeId(), p13.getNode());

                    Assert.assertEquals(1, clientSpareChannels.getChannelCount());

                    Assert.assertFalse(p11.getChannel().isConnected());
                    Assert.assertFalse(p12.getChannel().isConnected());
                    Assert.assertTrue(p13.getChannel().isConnected());

                    // Releasing a closed channel does nothing
                    clientSpareChannels.releaseChannel(p11);
                    Assert.assertEquals(1, clientSpareChannels.getChannelCount());

                    // Close a released opened channel: should get a new one
                    p13.getChannel().close().await();
                    p14 = clientSpareChannels.getChannel(SERVER_1.getNodeId(), SERVER_1.getAddress());
                    clientSpareChannels.releaseChannel(p14);
                    Assert.assertNotSame(p13, p14);
                    Assert.assertFalse(p13.getChannel().isConnected());
                    Assert.assertTrue(p14.getChannel().isConnected());

                }
                finally {
                    serverEndpoint2.stop();
                }
            }
            finally {
                serverEndpoint1.stop();
            }
        }
        finally {
            msgClientStartpoint.stop();
        }

        // All the channels should now be closed
        Assert.assertFalse(p14.getChannel().isConnected());
    }

    @Test(expected = ConnectException.class)
    public void testSynchronousRequestNewChannelConnectFail1() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeerNewChannelReuse()");

        final List<MsgNode> peers = new ArrayList<>(2);
        peers.add(SERVER_1);
        peers.add(SERVER_2);
        final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
        msgClientStartpoint.start();

        try {
            final MsgClientSpareChannels clientSpareChannels = msgClientStartpoint.getMsgClientSpareChannels();
            clientSpareChannels.getChannel(SERVER_1.getNodeId(), SERVER_1.getAddress());
        }
        finally {
            msgClientStartpoint.stop();
        }
    }

    @Test(expected = ConnectException.class)
    public void testSynchronousRequestNewChannelConnectFail2() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeerNewChannelReuse()");

        final List<MsgNode> peers = new ArrayList<>(1);
        peers.add(SERVER_1);
        final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
        msgClientStartpoint.start();

        try {
            final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                    .setSynchronous(true).setMsgData(ByteString.copyFromUtf8(" ")).build();
            msgClientStartpoint.sendSyncMessageNewChannel(SERVER_1.getNodeId(), request);
        }
        finally {
            msgClientStartpoint.stop();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSynchronousRequestNewChannelInvalidNode() throws Throwable {
        LOGGER.info("Run testSynchronousRequestReplyOnePeerNewChannelReuse()");

        final List<MsgNode> peers = new ArrayList<>(1);
        peers.add(SERVER_1);
        final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
        msgClientStartpoint.start();

        try {
            final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                    .setSynchronous(true).setMsgData(ByteString.copyFromUtf8(" ")).build();
            msgClientStartpoint.sendSyncMessageNewChannel(SERVER_2.getNodeId(), request);
        }
        finally {
            msgClientStartpoint.stop();
        }
    }

    @Test
    public void testSynchronousRequestServerStop() throws Throwable {
        LOGGER.info("Run testSynchronousRequestServerStop()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1, request);
        serverEndpoint1.start();

        try {
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2, request);
            boolean server2Started = true;
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    // Restart server2 to disconnect client
                    serverEndpoint2.stop();
                    server2Started = false;
                    serverEndpoint2.start();
                    server2Started = true;

                    waitConnected(2, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                if (server2Started) {
                    serverEndpoint2.stop();
                }
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testAsynchronousRequestTwoServers() throws InterruptedException {
        LOGGER.info("Run testAsynchronousRequestTwoServers()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(false)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1, request);
        serverEndpoint1.start();

        try {
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2, request);
            boolean server2Started = true;
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {

                    // Restart server2 to disconnect client
                    serverEndpoint2.stop();
                    server2Started = false;
                    Thread.sleep(1000);
                    serverEndpoint2.start();
                    server2Started = true;

                    waitConnected(2, msgClientStartpoint);

                    msgClientStartpoint.sendAsyncMessage(request);
                    Thread.sleep(2000);
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                if (server2Started) {
                    serverEndpoint2.stop();
                }
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testHandleException() throws Throwable {
        LOGGER.info("Run testHandleException()");

        final ByteString data = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(data).build();

        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, new MsgServerHandler() {

            @Override
            public MessageLite handleMessage(final MessageLite message) {
                throw new IllegalArgumentException();
            }
        }, MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final List<MsgNode> peers = new ArrayList<>(1);
            peers.add(SERVER_1);
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
            msgClientStartpoint.start();
            try {
                msgClientStartpoint.setTimeout(15000);
                waitConnected(1, msgClientStartpoint);

                final Collection<MsgServerRemoteStatus> msgServerRemoteStatus = msgClientStartpoint
                        .sendSyncMessage(request);
                Assert.assertNotNull(msgServerRemoteStatus);
                Assert.assertEquals(1, msgServerRemoteStatus.size());
                final MsgServerRemoteStatus error = msgServerRemoteStatus.iterator().next();
                Assert.assertEquals("java.lang.IllegalArgumentException", error.getExceptionName());
                Assert.assertEquals(SERVER_1.getAddress(), error.getRemotePeer());
            }
            finally {
                msgClientStartpoint.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    final class MsgHandlerTestMsgIdHandling implements MsgServerHandler {

        private volatile int[] testArray;
        private final AtomicInteger currentIndex = new AtomicInteger(0);

        public MsgHandlerTestMsgIdHandling(final int size) {
            this.testArray = new int[size];
            resetArray();
        }

        public int[] getTestArray() {
            return testArray;
        }

        public int getCurrentIndex() {
            return currentIndex.get();
        }

        public final void resetArray() {
            for (int i = 0; i < testArray.length; i++) {
                testArray[i] = -1;
            }
        }

        @Override
        public MessageLite handleMessage(final MessageLite message) {
            final MsgWrapper.MsgRequest msgRequest = (MsgRequest) message;
            final int val = Integer.valueOf(msgRequest.getMsgData().toStringUtf8()).intValue();
            final int i = currentIndex.getAndIncrement();
            testArray[i] = val;
            return null;
        }
    }

    @Test
    public void testMsgIdHandlingMsgSingleThread() throws Throwable {
        LOGGER.info("Run testMsgIdHandlingMsgSingleThread()");

        final int MAX_INDEX = 10;

        final MsgHandlerTestMsgIdHandling msgHandlerImpl1 = new MsgHandlerTestMsgIdHandling(MAX_INDEX);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final List<MsgNode> peers = new ArrayList<>(1);
            peers.add(SERVER_1);
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
            msgClientStartpoint.start();
            try {
                msgClientStartpoint.setTimeout(10000);
                waitConnected(1, msgClientStartpoint);

                for (int i = 0; i < MAX_INDEX; i++) {
                    final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                            .setSynchronous(false).setMsgData(ByteString.copyFrom(Integer.toString(i).getBytes()))
                            .build();
                    msgClientStartpoint.sendAsyncMessage(request);
                }

                while (msgHandlerImpl1.getCurrentIndex() < MAX_INDEX) {
                    Thread.sleep(300);
                }

                final int[] resultArray = msgHandlerImpl1.getTestArray();
                for (int i = 0; i < MAX_INDEX; i++) {
                    Assert.assertEquals(i, resultArray[i]);
                }
            }
            finally {
                LOGGER.info("Stopping the client endpoint...");
                msgClientStartpoint.stop();
            }
        }
        finally {
            LOGGER.info("Stopping the server endpoint...");
            serverEndpoint1.stop();
        }
    }

    final class SendThread implements Runnable {

        private final MsgClientStartpoint msgClientStartpoint;
        private final int[] sharedArray;
        private final AtomicInteger currentIndex;

        public SendThread(final MsgClientStartpoint msgClientStartpoint, final int[] sharedArray,
                final AtomicInteger currentIndex) {
            this.msgClientStartpoint = msgClientStartpoint;
            this.sharedArray = sharedArray;
            this.currentIndex = currentIndex;
        }

        @Override
        public void run() {
            int i;
            while (currentIndex.get() < sharedArray.length) {
                synchronized (msgClientStartpoint) {
                    i = currentIndex.getAndIncrement();
                    if (i < sharedArray.length) {
                        final int alea = (int) (Math.random() * 10);
                        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                                .setSynchronous(true)
                                .setMsgData(ByteString.copyFrom(Integer.valueOf(alea).toString().getBytes())).build();

                        sharedArray[i] = alea;
                        try {
                            msgClientStartpoint.sendAsyncMessage(request);
                        }
                        catch (final Throwable t) {
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMsgIdHandlingMsgMultiThread() throws Throwable {
        LOGGER.info("Run testMsgIdHandlingMsgMultiThread()");

        final int MAX_INDEX = 15;

        final MsgHandlerTestMsgIdHandling msgHandlerImpl1 = new MsgHandlerTestMsgIdHandling(MAX_INDEX);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final List<MsgNode> peers = new ArrayList<>(1);
            peers.add(SERVER_1);
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
            msgClientStartpoint.start();
            try {
                msgClientStartpoint.setTimeout(10000);
                waitConnected(1, msgClientStartpoint);

                final int[] sharedArray = new int[MAX_INDEX];
                for (int i = 0; i < sharedArray.length; i++) {
                    sharedArray[i] = -1;
                }
                final AtomicInteger currentIndex = new AtomicInteger(0);
                final ExecutorService executor = Executors.newFixedThreadPool(3);
                executor.execute(new SendThread(msgClientStartpoint, sharedArray, currentIndex));
                executor.execute(new SendThread(msgClientStartpoint, sharedArray, currentIndex));
                executor.execute(new SendThread(msgClientStartpoint, sharedArray, currentIndex));

                while (msgHandlerImpl1.getCurrentIndex() < MAX_INDEX) {
                    Thread.sleep(300);
                }

                executor.shutdown();

                final int[] resultArray = msgHandlerImpl1.getTestArray();
                for (int i = 0; i < MAX_INDEX; i++) {
                    Assert.assertEquals(sharedArray[i], resultArray[i]);
                }
            }
            finally {
                msgClientStartpoint.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testAddRemovePeer() throws Throwable {
        LOGGER.info("Run testAddRemovePeer()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();

        try {
            final List<MsgNode> peers = new ArrayList<>(2);
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
            msgClientStartpoint.start();
            try {
                msgClientStartpoint.addPeer(SERVER_1);
                msgClientStartpoint.addPeer(SERVER_2);

                final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
                final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                        MsgWrapper.MsgRequest.getDefaultInstance());
                serverEndpoint2.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());

                    msgHandlerImpl1.resetSuccess();
                    msgHandlerImpl2.resetSuccess();

                    msgClientStartpoint.removePeer(SERVER_1);
                    waitConnected(1, msgClientStartpoint);
                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertFalse(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());

                    msgHandlerImpl1.resetSuccess();
                    msgHandlerImpl2.resetSuccess();

                    msgClientStartpoint.addPeer(SERVER_1);
                    waitConnected(2, msgClientStartpoint);
                    msgClientStartpoint.removePeer(SERVER_2);
                    waitConnected(1, msgClientStartpoint);
                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertFalse(msgHandlerImpl2.isSuccess());
                }
                finally {
                    serverEndpoint2.stop();
                }
            }
            finally {
                msgClientStartpoint.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    @Test
    public void testIsConnected() throws InterruptedException {
        LOGGER.info("Run testIsConnected()");

        // Define server1
        final MsgServerHandler msgHandlerImpl1 = new MsgServerHandler() {
            @Override
            public final MessageLite handleMessage(final MessageLite message) {
                return null;
            }
        };
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());

        final List<MsgNode> peers = new ArrayList<>(0);
        final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
        msgClientStartpoint.start();
        try {
            Assert.assertFalse(msgClientStartpoint.isPeerConnected(SERVER_1.getNodeId()));
            msgClientStartpoint.addPeer(SERVER_2);
            Assert.assertFalse(msgClientStartpoint.isPeerConnected(SERVER_1.getNodeId()));
            msgClientStartpoint.addPeer(SERVER_1);
            Assert.assertFalse(msgClientStartpoint.isPeerConnected(SERVER_1.getNodeId()));

            // Start server1
            serverEndpoint1.start();
            try {
                waitConnected(1, msgClientStartpoint);
                Assert.assertTrue(msgClientStartpoint.isPeerConnected(SERVER_1.getNodeId()));
            }
            finally {
                serverEndpoint1.stop();
            }

            // Not connected anymore
            Assert.assertFalse(msgClientStartpoint.isPeerConnected(SERVER_1.getNodeId()));
        }
        finally {
            msgClientStartpoint.stop();
        }

    }

    final class MsgHandlerTestTimeout implements MsgServerHandler {
        @Override
        public MessageLite handleMessage(final MessageLite message) {
            try {
                Thread.sleep(4000);
            }
            catch (final InterruptedException e) {
            }
            return null;
        }
    }

    @Test(expected = MsgServerTimeoutException.class)
    public void testTimeoutWithOneServer() throws InterruptedException, MsgServerTimeoutException {
        LOGGER.info("Run testTimeoutWithOneServer()");

        final MsgServerHandler msgHandlerImpl1 = new MsgHandlerTestTimeout();
        final MsgServerEndpoint serverEndpoint = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint.start();

        try {
            final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(null);
            msgClientStartpoint.addPeer(SERVER_1);
            msgClientStartpoint.setTimeout(1000);
            msgClientStartpoint.start();
            try {
                waitConnected(1, msgClientStartpoint);

                final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
                final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                        .setSynchronous(true).setMsgData(expectedString).build();
                Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
            }
            finally {
                msgClientStartpoint.stop();
            }
        }
        finally {
            serverEndpoint.stop();
        }
    }

    /**
     * This test ensures that when a message is sent to a set of N peers and M peers crashed during the sending then if
     * the client receive at least (N-M) ACKs the sending is still valid.
     * 
     * Here an example: a client is connected to 5 peers and has sent a synchronous message. 2 peers crashed during the
     * operation. If the client receives 3 ACKs from the 3 remaining peers, the message has been correctly delivered and
     * there is no timeout to trigger.
     * 
     * @throws InterruptedException
     * @throws MsgServerTimeoutException
     */
    @Test
    public void testSyncSendWithOneServerLost() throws InterruptedException, MsgServerTimeoutException {
        LOGGER.info("Run testSyncSendWithOneServerLost()");

        boolean isSuccess = true;
        final MsgServerHandler msgHandlerImpl1 = new MsgHandlerTestTimeout();
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1,
                MsgWrapper.MsgRequest.getDefaultInstance());

        serverEndpoint1.start();
        try {
            final MsgServerHandler msgHandlerImpl2 = new MsgHandlerTestTimeout();
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2,
                    MsgWrapper.MsgRequest.getDefaultInstance());

            serverEndpoint2.start();
            try {
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(null);
                msgClientStartpoint.addPeer(SERVER_1);
                msgClientStartpoint.addPeer(SERVER_2);
                msgClientStartpoint.setTimeout(6000);
                msgClientStartpoint.start();
                try {
                    waitConnected(2, msgClientStartpoint);

                    final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
                    final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345)
                            .setSynchronous(true).setMsgData(expectedString).build();

                    // Run a thread which stop the second server
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(500);
                            }
                            catch (final InterruptedException e) {
                            }
                            serverEndpoint2.stop();
                        }
                    }).start();

                    // Despite the fact that the second server stopped, the sending is still valid
                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                }
                catch (final MsgServerTimeoutException e) {
                    isSuccess = false;
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                serverEndpoint2.stop();
            }
        }
        finally {
            serverEndpoint1.stop();
        }
        Assert.assertTrue(isSuccess);
    }

    @Test
    public void testClientRestart() throws Throwable {
        LOGGER.info("Run testClientRestart()");

        final ByteString expectedString = ByteString.copyFrom("hello test".getBytes());
        final MsgWrapper.MsgRequest request = MsgWrapper.MsgRequest.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(12345).setSynchronous(true)
                .setMsgData(expectedString).build();

        final MsgHandlerTestExpectedObject msgHandlerImpl1 = new MsgHandlerTestExpectedObject(expectedString);
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, msgHandlerImpl1, request);
        serverEndpoint1.start();

        try {
            final MsgHandlerTestExpectedObject msgHandlerImpl2 = new MsgHandlerTestExpectedObject(expectedString);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, msgHandlerImpl2, request);
            boolean server2Started = true;
            serverEndpoint2.start();

            try {
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {

                    waitConnected(2, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());

                    // Stop server2 to disconnect client
                    serverEndpoint2.stop();
                    server2Started = false;

                    waitConnected(1, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());

                }
                finally {
                    msgClientStartpoint.stop();
                }

                // Restart client
                Thread.sleep(1000);
                msgClientStartpoint.start();
                try {
                    waitConnected(1, msgClientStartpoint);

                    msgClientStartpoint.sendSyncMessage(SERVER_1.getNodeId(), request);
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());

                    // Restart server2
                    serverEndpoint2.start();
                    server2Started = true;

                    waitConnected(2, msgClientStartpoint);

                    Assert.assertTrue(msgClientStartpoint.sendSyncMessage(request).isEmpty());
                    Assert.assertTrue(msgHandlerImpl1.isSuccess());
                    Assert.assertTrue(msgHandlerImpl2.isSuccess());
                }
                finally {
                    msgClientStartpoint.stop();
                }
            }
            finally {
                if (server2Started) {
                    serverEndpoint2.stop();
                }
            }
        }
        finally {
            serverEndpoint1.stop();
        }
    }

    /**
     * Wait at most 20 seconds for the client to be connected to the expected number of peers.
     * 
     * @param expectedCount
     * @param clientStartpoint
     * @throws InterruptedException
     */
    public static final void waitConnected(final int expectedCount, final MsgClientStartpoint clientStartpoint)
            throws InterruptedException {
        for (int i = 0; i < 40; i++) {
            if (clientStartpoint.getConnectedPeersCount() == expectedCount) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionFailedError("Not connected");
    }

}
