package com.oodrive.nuage.net;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.AssertionFailedError;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.MessageLite;
import com.oodrive.nuage.proto.net.MsgWrapper;

public class TestMsgClient {

    public static final Logger LOGGER = LoggerFactory.getLogger(TestMsgClient.class.getSimpleName());

    private final MsgNode SERVER_1 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55255));
    private final MsgNode SERVER_2 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55256));

    final static class MsgHandlerTest implements MsgServerHandler {

        MsgHandlerTest() {
            super();
        }

        @Override
        public final MessageLite handleMessage(final MessageLite message) {
            return null;
        }
    }

    @Test
    public void testMXBeans() throws Exception {
        LOGGER.info("Run testMXBeans()");

        // First Server
        final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, new MsgHandlerTest(),
                MsgWrapper.MsgRequest.getDefaultInstance());
        serverEndpoint1.start();
        try {

            // Second server
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, new MsgHandlerTest(),
                    MsgWrapper.MsgRequest.getDefaultInstance());
            serverEndpoint2.start();
            try {
                // Client
                final List<MsgNode> peers = new ArrayList<>(2);
                peers.add(SERVER_1);
                peers.add(SERVER_2);
                final MsgClientStartpoint msgClientStartpoint = new MsgClientStartpoint(peers);
                msgClientStartpoint.start();
                try {
                    // register MXBean
                    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                    final ObjectName msgClientName = msgClientStartpoint.registerMXBean(server);
                    try {
                        final MsgClientMXBean msgClientMXBean = JMX
                                .newMXBeanProxy(ManagementFactory.getPlatformMBeanServer(), msgClientName,
                                        MsgClientMXBean.class, false);

                        waitConnected(2, msgClientMXBean);

                        checkPeers(peers, msgClientMXBean);
                        checkAttributes(msgClientStartpoint, msgClientMXBean);
                        checkRestart(msgClientMXBean);

                        waitConnected(2, msgClientMXBean);

                    }
                    finally {
                        server.unregisterMBean(msgClientName);
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

    private static final void checkPeers(final List<MsgNode> peers, final MsgClientMXBean msgClientMXBean) {
        final MsgClientPeerAdm[] peersAdm = msgClientMXBean.getPeers();
        boolean found = false;
        for (final MsgNode peer : peers) {
            for (final MsgClientPeerAdm peerAdm : peersAdm) {
                if (peer.getNodeId().toString().equals(peerAdm.getUuid())) {
                    assertEquals(peer.getAddress().getAddress().getHostAddress(), peerAdm.getIpAddress());
                    assertEquals(peer.getAddress().getPort(), peerAdm.getPort());
                    assertTrue(peerAdm.isConnected());
                    found = true;
                }
            }
            assertTrue(found);
            found = false;
        }
    }

    private static final void checkAttributes(final MsgClientStartpoint msgClientStartpoint,
            final MsgClientMXBean msgClientMXBean) throws Exception {
        assertEquals(msgClientStartpoint.getUuid(), msgClientMXBean.getUuid());
        assertTrue(msgClientMXBean.isStarted());
        assertEquals(2, msgClientMXBean.getConnectedPeersCount());
        final long timeout = msgClientMXBean.getTimeout();
        msgClientMXBean.setTimeout(timeout + 500);
        assertEquals(timeout + 500, msgClientMXBean.getTimeout());

    }

    private static final void checkRestart(final MsgClientMXBean msgClientMXBean) throws Exception {
        assertTrue(msgClientMXBean.isStarted());
        msgClientMXBean.restart();
        assertTrue(msgClientMXBean.isStarted());
    }

    /**
     * Wait at most 20 seconds for the client to be connected to the expected number of peers.
     * 
     * @param expectedCount
     * @param clientStartpoint
     * @throws InterruptedException
     */
    public static final void waitConnected(final int expectedCount, final MsgClientMXBean client)
            throws InterruptedException {
        for (int i = 0; i < 40; i++) {
            if (client.getConnectedPeersCount() == expectedCount) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionFailedError("Not connected");
    }
}
