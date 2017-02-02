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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.eguan.net.TestMsgClient.MsgHandlerTest;
import io.eguan.proto.net.MsgWrapper;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMsgServer {

    public static final Logger LOGGER = LoggerFactory.getLogger(TestMsgServer.class.getSimpleName());

    private final static MsgNode SERVER = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55255));

    @Test
    public void testMXBeans() throws Exception {
        LOGGER.info("Run testMXBeans()");

        // Server
        final MsgServerEndpoint msgServerEndpoint = new MsgServerEndpoint(SERVER, new MsgHandlerTest(),
                MsgWrapper.MsgRequest.getDefaultInstance());
        msgServerEndpoint.start();
        try {

            // Client 1
            final List<MsgNode> peers1 = new ArrayList<>(1);
            peers1.add(SERVER);
            final MsgClientStartpoint msgClientStartpoint1 = new MsgClientStartpoint(peers1);
            msgClientStartpoint1.start();
            try {
                TestMessagingService.waitConnected(1, msgClientStartpoint1);

                final List<MsgNode> peers2 = new ArrayList<>(1);
                peers2.add(SERVER);
                final MsgClientStartpoint msgClientStartpoint2 = new MsgClientStartpoint(peers2);
                msgClientStartpoint2.start();
                try {
                    TestMessagingService.waitConnected(1, msgClientStartpoint2);

                    // register MXBean
                    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                    final ObjectName msgServerName = msgServerEndpoint.registerMXBean(server);

                    try {
                        final MsgServerMXBean msgServerMXBean = JMX
                                .newMXBeanProxy(ManagementFactory.getPlatformMBeanServer(), msgServerName,
                                        MsgServerMXBean.class, false);

                        checkAttributes(msgServerMXBean);
                        checkRestart(msgServerMXBean);

                        TestMessagingService.waitConnected(1, msgClientStartpoint1);
                        TestMessagingService.waitConnected(1, msgClientStartpoint2);

                    }
                    finally {
                        server.unregisterMBean(msgServerName);
                    }
                }
                finally {
                    msgClientStartpoint2.stop();
                }
            }
            finally {
                msgClientStartpoint1.stop();
            }
        }
        finally {
            msgServerEndpoint.stop();
        }
    }

    private static final void checkAttributes(final MsgServerMXBean msgServerMXBean) throws Exception {
        assertEquals(SERVER.getNodeId().toString(), msgServerMXBean.getUuid());
        assertTrue(msgServerMXBean.isStarted());
        assertEquals(SERVER.getAddress().getAddress().getHostAddress(), msgServerMXBean.getIpAddress());
        assertEquals(SERVER.getAddress().getPort(), msgServerMXBean.getPort());
    }

    private static final void checkRestart(final MsgServerMXBean msgServerMXBean) throws Exception {
        assertTrue(msgServerMXBean.isStarted());
        msgServerMXBean.restart();
        assertTrue(msgServerMXBean.isStarted());
    }

}
