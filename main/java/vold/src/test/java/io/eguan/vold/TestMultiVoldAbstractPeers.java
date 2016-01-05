package io.eguan.vold;

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

import static org.junit.Assert.assertTrue;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.vold.EnableIscsiConfigKey;
import io.eguan.vold.EnableNbdConfigKey;
import io.eguan.vold.PeersConfigKey;
import io.eguan.vold.Vold;
import io.eguan.vold.VoldConfigurationContext;
import io.eguan.vold.VoldLocation;
import io.eguan.vold.VoldMXBean;
import io.eguan.vold.model.Constants;
import io.eguan.vold.model.DummyMBeanServer;
import io.eguan.vold.model.VoldTestHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;

public abstract class TestMultiVoldAbstractPeers extends TestMultiVoldAbstract {
    private static final Logger LOGGER = Constants.LOGGER;

    protected DummyMBeanServer server1;
    protected DummyMBeanServer server2;
    protected DummyMBeanServer server3;
    protected VoldTestHelper helper1;
    protected VoldTestHelper helper2;
    protected VoldTestHelper helper3;
    protected ObjectName voldMxBeanObjectName1;
    protected ObjectName voldMxBeanObjectName2;
    protected ObjectName voldMxBeanObjectName3;
    protected Vold vold1;
    protected Vold vold2;
    protected Vold vold3;

    @BeforeClass
    public static void init() throws Exception {
        // setup a quorum of 3 peers
        setUpVolds(3, 0, false);
    }

    @Before
    public void initialize() throws Exception {
        server1 = getDummyMBeanServer(0);
        Assert.assertNotNull(server1);

        server2 = getDummyMBeanServer(1);
        Assert.assertNotNull(server2);

        server3 = getDummyMBeanServer(2);
        Assert.assertNotNull(server3);

        helper1 = getVoldTestHelper(0);
        Assert.assertNotNull(helper1);

        helper2 = getVoldTestHelper(1);
        Assert.assertNotNull(helper2);

        helper3 = getVoldTestHelper(2);
        Assert.assertNotNull(helper3);

        voldMxBeanObjectName1 = newVoldObjectName();
        voldMxBeanObjectName2 = newVoldObjectName();
        voldMxBeanObjectName3 = newVoldObjectName();
    }

    @After
    public void cleanup() {
        try {
            if (helper1 != null)
                helper1.finiVold();
        }
        catch (final Throwable e) {
            LOGGER.debug("finiVold 1 failed", e);
        }
        try {
            if (helper2 != null)
                helper2.finiVold();
        }
        catch (final Throwable e) {
            LOGGER.debug("finiVold 2 failed", e);
        }
        try {
            if (helper3 != null)
                helper3.finiVold();
        }
        catch (final Throwable e) {
            LOGGER.debug("finiVold 3 failed", e);
        }
        vold1 = null;
        vold2 = null;
        vold3 = null;
        helper1 = null;
        helper2 = null;
        helper3 = null;
    }

    /**
     * Get peers list from a VoldMXBean
     * 
     * @param vold
     * @param voldContext
     * @return
     */
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

    /**
     * Change peers list from a VoldMXBean configuration.
     * 
     * @param vold
     * @param peers
     */
    protected void changePeersList(final VoldMXBean vold, final ArrayList<VoldLocation> peers) {
        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
        newKeyValueMap.put(PeersConfigKey.getInstance(), peers);
        try {
            ((Vold) vold).updateConfiguration(newKeyValueMap);
        }
        catch (IOException | ConfigValidationException e) {
            LOGGER.debug("updateConfiguration failed");
        }
    }

    protected Vold initVold(final VoldTestHelper helper, final DummyMBeanServer server, final ObjectName name)
            throws JMException, IOException, InterruptedException {
        helper.initVold(server, name);
        final Vold vold = (Vold) server.getMXBean(name);
        Assert.assertNotNull(vold);
        disableServers(vold);
        return vold;
    }

    protected void addPeer(final VoldMXBean voldMX, final VoldLocation node) throws JMException {
        voldMX.addPeer(node.getNode().toString(), node.getSockAddr().getAddress().getHostAddress(), node.getSockAddr()
                .getPort());
    }

    protected void removePeer(final VoldMXBean voldMX, final VoldLocation node) throws JMException {
        voldMX.removePeer(node.getNode().toString());
    }

    private static final ObjectName newVoldObjectName() {
        final UUID nodeUuid = UUID.randomUUID();
        final String managerObjNameStr = "io.eguan.vold:type=Vold" + nodeUuid.toString();
        try {
            return new ObjectName(managerObjNameStr);
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Disable ISCSI and NBD server in a VoldMXBean configuration
     * 
     * @param vold
     */
    private void disableServers(final VoldMXBean vold) {
        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
        newKeyValueMap.put(EnableIscsiConfigKey.getInstance(), Boolean.FALSE);
        newKeyValueMap.put(EnableNbdConfigKey.getInstance(), Boolean.FALSE);
        try {
            ((Vold) vold).updateConfiguration(newKeyValueMap);
        }
        catch (IOException | ConfigValidationException e) {
            LOGGER.debug("updateConfiguration failed");
        }
    }
}
