package com.oodrive.nuage.vold.model;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.JMException;
import javax.management.JMX;
import javax.management.MBeanServer;

import org.junit.Test;

import com.oodrive.nuage.vold.EnableIscsiConfigKey;
import com.oodrive.nuage.vold.EnableNbdConfigKey;
import com.oodrive.nuage.vold.OwnerConfigKey;
import com.oodrive.nuage.vold.PeersConfigKey;
import com.oodrive.nuage.vold.VoldConfigurationContext;
import com.oodrive.nuage.vold.VoldMXBean;

public class TestVoldMXBean extends AbstractVoldTest {

    public TestVoldMXBean() throws Exception {
        super();
    }

    protected static String getPeersList(final VoldMXBean vold, final VoldConfigurationContext voldContext) {
        String peersList = null;
        final Map<String, String> config = vold.getVoldConfiguration();
        for (final Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().equals(voldContext.getPropertyKey(PeersConfigKey.getInstance()))) {
                peersList = entry.getValue();
                break;
            }
        }
        assertTrue(peersList != null);
        return peersList;
    }

    @Test
    public void testStopStart() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        vold.stop();
        vold.start();

        // Check vvr and vvr manager appear
        assertTrue(helper.waitMXBeanRegistration(helper.newVvrManagerObjectName()));
        assertTrue(helper.waitMXBeanRegistration(VvrObjectNameFactory.newVvrObjectName(helper.VOLD_OWNER_UUID_TEST,
                vvrUuid)));
    }

    @Test
    public void testConfig() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        final Map<String, String> config = vold.getVoldConfiguration();
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        // Check some parameters..
        for (final Entry<String, String> entry : config.entrySet()) {
            if (entry.getKey().equals(voldContext.getPropertyKey(OwnerConfigKey.getInstance()))) {
                assertEquals(helper.VOLD_OWNER_UUID_TEST_STR, entry.getValue());
            }
            else if (entry.getKey().equals(voldContext.getPropertyKey(EnableIscsiConfigKey.getInstance()))) {
                assertEquals("yes", entry.getValue());
            }
            else if (entry.getKey().equals(voldContext.getPropertyKey(EnableNbdConfigKey.getInstance()))) {
                assertEquals("yes", entry.getValue());
            }
            else {
                if (entry.getKey().equals(voldContext.getPropertyKey(PeersConfigKey.getInstance()))) {
                    assertEquals("", entry.getValue());
                }
            }
        }
    }

    @Test(expected = NullPointerException.class)
    public void testAddPeerUuidNull() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer(null, "127.0.0.1", 1234);
    }

    @Test(expected = NullPointerException.class)
    public void testRemovePeerUuidNull() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.removePeer(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddPeerAddressNull() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer("264d9380-cc62-11e3-be5d-2b6d1d81d4f3", null, 1234);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddPeerNegativePort() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer("4005750e-cc62-11e3-853a-5b25c07380f3", "127.0.0.1", -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddPeerSystemPort() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer("5652a5a2-cc62-11e3-8796-bfc651097fe3", "127.0.0.1", 80);
    }

    @Test
    public void testSimpleAddPeer() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer("62bfc806-cc62-11e3-81db-07a46df05e79", "127.0.0.1", 1234);
    }

    @Test(expected = AssertionError.class)
    public void testAddPeerUnresolvedHost() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());

        vold.addPeer("62bfc806-cc62-11e3-81db-07a46df05e79", "4412dd2c", 1234);
    }

    @Test
    public void testAddPeerCheckSavedPeersList() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        assertEquals("", getPeersList(vold, voldContext));

        final String expectedPeerList = "7679c694-cc62-11e3-b758-13b28bcfc119@127.0.0.1:1234";
        vold.addPeer("7679c694-cc62-11e3-b758-13b28bcfc119", "127.0.0.1", 1234);

        assertEquals(expectedPeerList, getPeersList(vold, voldContext));
    }

    @Test(expected = IllegalStateException.class)
    public void testAddTwoPeersNoQuorum() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        assertEquals("", getPeersList(vold, voldContext));

        final String expectedPeerList = "a32c9338-cc62-11e3-b614-af8270e2dec8@127.0.0.1:1234";
        vold.addPeer("a32c9338-cc62-11e3-b614-af8270e2dec8", "127.0.0.1", 1234);

        assertEquals(expectedPeerList, getPeersList(vold, voldContext));

        // the previous added peer is not online
        // adding a second one should throw IllegalStateException
        // because no quorum exists
        try {
            vold.addPeer("1be58c50-cbcb-11e3-b54a-6fe18592fa13", "127.0.0.1", 5678);
        }
        catch (final IllegalStateException e) {
            throw e;
        }
        finally {
            // test configuration is still consistent
            assertEquals(expectedPeerList, getPeersList(vold, voldContext));
        }
    }

    public void testAddRemovePeers() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        assertEquals("", getPeersList(vold, voldContext));

        final String expectedPeerList = "af7d88f4-ceb5-11e3-9332-cb5b19f82d58@127.0.0.1:1234";
        vold.addPeer("af7d88f4-ceb5-11e3-9332-cb5b19f82d58", "127.0.0.1", 1234);

        assertEquals(expectedPeerList, getPeersList(vold, voldContext));

        vold.removePeer("af7d88f4-ceb5-11e3-9332-cb5b19f82d58");

        assertEquals("", getPeersList(vold, voldContext));
    }

    public void testModifyByAddRemoveAddPeers() throws IOException, JMException, InterruptedException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final VoldMXBean vold = JMX.newMXBeanProxy(server, helper.newVoldObjectName(), VoldMXBean.class, false);
        assertEquals(vold.getPath(), helper.getVoldPath());
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        assertEquals("", getPeersList(vold, voldContext));

        final String expectedPeerList1 = "af7d88f4-ceb5-11e3-9332-cb5b19f82d58@127.0.0.1:1234";
        vold.addPeer("af7d88f4-ceb5-11e3-9332-cb5b19f82d58", "127.0.0.1", 1234);

        assertEquals(expectedPeerList1, getPeersList(vold, voldContext));

        vold.removePeer("af7d88f4-ceb5-11e3-9332-cb5b19f82d58");

        assertEquals("", getPeersList(vold, voldContext));

        final String expectedPeerList2 = "af7d88f4-ceb5-11e3-9332-cb5b19f82d58@127.0.0.1:5678";
        vold.addPeer("af7d88f4-ceb5-11e3-9332-cb5b19f82d58", "127.0.0.1", 5678);

        assertEquals(expectedPeerList2, getPeersList(vold, voldContext));
    }
}
