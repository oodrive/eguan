package com.oodrive.nuage.vold;

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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.UUID;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.dtx.DtxManager;
import com.oodrive.nuage.dtx.DtxNode;
import com.oodrive.nuage.dtx.config.DtxConfigurationContext;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.iscsisrv.IscsiServer;
import com.oodrive.nuage.iscsisrv.IscsiServerConfigurationContext;
import com.oodrive.nuage.nbdsrv.NbdServer;
import com.oodrive.nuage.nbdsrv.NbdServerConfigurationContext;
import com.oodrive.nuage.vold.MultiVoldUtils.VoldClientServer;
import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.model.DummyMBeanServer;
import com.oodrive.nuage.vold.model.SnapshotMXBean;
import com.oodrive.nuage.vold.model.VoldTestHelper;
import com.oodrive.nuage.vold.model.VvrMXBean;
import com.oodrive.nuage.vold.model.VvrManagementException;
import com.oodrive.nuage.vold.model.VvrManager;
import com.oodrive.nuage.vold.model.VvrManagerHelper;
import com.oodrive.nuage.vold.model.VvrManagerTestUtils;

public abstract class TestMultiVoldAbstract {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TestMultiVoldAbstract.class);

    protected static final int BLOCKSIZE = 4096; // == BlockSize
    protected static final int LENGTH = 64 * 4; // 64KB == 4MB

    // Default test block count
    protected static final int NUMBLOCKS = 4; // 4096 == 64KB,

    // Test block count for single IO
    protected static final int NUMBLOCKS_SINGLE = 1; // 4096

    protected static final int BLOCKSIZE_PARTIAL = 512; // BlockSize/8

    protected static int nbOfNodes;
    protected static int nbOfNodesInit;
    protected static Boolean vvrIsStarted;

    private static final int DEFAULT_HZ_PORT = 12341;
    private static final int HZ_PORT_OFFSET = 10;

    private static final String ISCSI_ADDRESS = InetAddress.getLoopbackAddress().getHostAddress();
    private static final Integer ISCSI_PORT = Integer.valueOf(3260);

    private static final String NBD_ADDRESS = InetAddress.getLoopbackAddress().getHostAddress();
    private static final Integer NBD_PORT = Integer.valueOf(10809);

    private static DtxManager dtxManagers[];
    private static VvrManager vvrManagers[];
    private static VoldTestHelper voldTestHelpers[];
    private static DummyMBeanServer dummyMBeanServers[];
    private static VoldClientServer voldClientServers[];
    private static IscsiServer iscsiServers[];
    private static NbdServer nbdServers[];

    protected static boolean nodeStarted[];

    protected static final ArrayList<DtxNode> peerList = new ArrayList<DtxNode>();
    protected static final ArrayList<DtxNode> dtxPeerList = new ArrayList<DtxNode>();

    protected static final UUID VOLD_OWNER = UUID.randomUUID();

    private static ObjectName dtxLocalNodeObjectNames[];
    private static ObjectName dtxManagerObjectNames[];
    private static ObjectName msgServerObjectNames[];
    private static ObjectName msgClientObjectNames[];

    protected UUID vvrUuid;
    protected UUID rootUuid;

    DtxManager getDtxManager(final int index) {
        return dtxManagers[index];
    }

    VvrManager getVvrManager(final int index) {
        return vvrManagers[index];
    }

    static VoldTestHelper getVoldTestHelper(final int index) {
        return voldTestHelpers[index];
    }

    static DummyMBeanServer getDummyMBeanServer(final int index) {
        return dummyMBeanServers[index];
    }

    static int getNbdServerPort(final int serverIndex) {
        return NBD_PORT.intValue() + 2 * serverIndex;
    }

    static int getIscsiServerPort(final int serverIndex) {
        return ISCSI_PORT.intValue() + 2 * serverIndex;
    }

    @Before
    public final void checkNodes() throws Exception {
        // Check node
        for (int i = 0; i < nbOfNodesInit; i++) {
            if (!nodeStarted[i]) {
                startNode(i);
                Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(i),
                        VoldTestHelper.MXBEANS_NUMBER_INIT));
            }
        }
        for (int i = nbOfNodesInit; i < nbOfNodes; i++) {
            if (nodeStarted[i]) {
                stopNode(i);
            }
        }
    }

    @After
    public final void deleteVvrs() throws Exception {
        if (vvrUuid != null) {
            VvrMXBean vvr;
            if (rootUuid != null) {
                for (int i = 0; i < nbOfNodes; i++) {
                    // Restart the stopped node, to check its vvr
                    if (!nodeStarted[i]) {
                        startNode(i);
                        // Wait vvr is registered
                        vvr = getVoldTestHelper(i).waitVvrMXBeanRegistration(getDummyMBeanServer(i), vvrUuid);
                    }
                    else {
                        vvr = VvrManagerTestUtils.getVvrMXBean(getDummyMBeanServer(i), VOLD_OWNER, vvrUuid);
                    }
                    Assert.assertNotNull(vvr);

                    // If the vvr is started, deactivate all its devices and stop it
                    if (vvr.isStarted()) {
                        getVoldTestHelper(i).deactivateDevice(getDummyMBeanServer(i), vvrUuid, rootUuid.toString());
                        getVoldTestHelper(i).stopVvr(getDummyMBeanServer(i), vvrUuid);

                    }
                }
            }
            // Delete the vvr
            getVoldTestHelper(0).deleteVvr(getDummyMBeanServer(0), vvrUuid);
        }

    }

    protected final SnapshotMXBean createVvrStarted() throws VvrManagementException {
        final VvrMXBean vvrMXBean = getVoldTestHelper(0).createVvr(getDummyMBeanServer(0), "name", "description");
        vvrUuid = UUID.fromString(vvrMXBean.getUuid());
        final SnapshotMXBean snapshotRoot = VvrManagerTestUtils.getSnapshotRoot(getDummyMBeanServer(0),
                getVoldTestHelper(0), vvrUuid);
        rootUuid = UUID.fromString(snapshotRoot.getUuid());
        return snapshotRoot;
    }

    protected static final void setUpVolds(final int nbOfNodes, final int nbOfNodesInit, final boolean vvrIsStarted)
            throws Exception {

        TestMultiVoldAbstract.nbOfNodes = nbOfNodes;
        TestMultiVoldAbstract.nbOfNodesInit = nbOfNodesInit;
        TestMultiVoldAbstract.vvrIsStarted = Boolean.valueOf(vvrIsStarted);

        // Initialize arrays with nb of nodes
        dtxManagers = new DtxManager[nbOfNodes];
        vvrManagers = new VvrManager[nbOfNodes];
        voldTestHelpers = new VoldTestHelper[nbOfNodes];
        dummyMBeanServers = new DummyMBeanServer[nbOfNodes];
        voldClientServers = new VoldClientServer[nbOfNodes];
        iscsiServers = new IscsiServer[nbOfNodes];
        nbdServers = new NbdServer[nbOfNodes];
        dtxLocalNodeObjectNames = new ObjectName[nbOfNodes];
        dtxManagerObjectNames = new ObjectName[nbOfNodes];
        msgClientObjectNames = new ObjectName[nbOfNodes];
        msgServerObjectNames = new ObjectName[nbOfNodes];

        nodeStarted = new boolean[nbOfNodes];

        // Initialize peers lists
        for (int i = 0; i < nbOfNodes; i++) {
            peerList.add(new DtxNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", DEFAULT_HZ_PORT
                    + (i * HZ_PORT_OFFSET))));
        }

        for (int i = 0; i < nbOfNodes; i++) {
            dtxPeerList.add(new DtxNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", DEFAULT_HZ_PORT + 1
                    + (i * HZ_PORT_OFFSET))));
        }

        // Init all the vold Test Helper
        for (int i = 0; i < nbOfNodes; i++) {
            initVoldTestHelper(i);
            dummyMBeanServers[i] = new DummyMBeanServer();
        }
        // Start only the Node which must be started at the beginning
        for (int i = 0; i < nbOfNodesInit; i++) {
            startNode(i);
        }
        // Wait for all the nodes are started correctly
        for (int i = 0; i < nbOfNodesInit; i++) {
            Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(i),
                    VoldTestHelper.MXBEANS_NUMBER_INIT));
        }
    }

    public static final void stopNodes() throws Exception {
        for (int k = 0; k < nbOfNodes; k++) {
            stopNode(k);
        }
    }

    @AfterClass
    public static final void cleanRemoteVolds() throws Exception {
        // Clean all Vold potentially started
        for (int k = 0; k < nbOfNodes; k++) {
            stopNode(k);
            finiVoldTestHelper(k);
            dummyMBeanServers[k] = null;
        }
    }

    private static void initVoldTestHelper(final int index) throws IOException {

        final DtxNode currPeer = peerList.get(index);
        final ArrayList<DtxNode> otherPeers = new ArrayList<DtxNode>(peerList);
        otherPeers.remove(currPeer);

        final String peers = constructRemotePeers(otherPeers);

        // VOLD test Helper
        final VoldTestHelper voldTestHelper = new VoldTestHelper(VOLD_OWNER, VoldTestHelper.CompressionType.no,
                HashAlgorithm.MD5, vvrIsStarted);

        voldTestHelpers[index] = voldTestHelper;

        voldTestHelper.createTemporary(currPeer.getNodeId().toString(), currPeer.getAddress().getHostName(),
                Integer.valueOf(currPeer.getAddress().getPort()), peers, ISCSI_ADDRESS,
                Integer.valueOf(getIscsiServerPort(index)), NBD_ADDRESS, Integer.valueOf(getNbdServerPort(index)));

    }

    private static void finiVoldTestHelper(final int index) {
        try {
            voldTestHelpers[index].destroy();
        }
        catch (final Throwable t) {
            LOGGER.warn("Error while destroying the vold test helper", t);
        }
        finally {
            voldTestHelpers[index] = null;
        }

    }

    private static String constructRemotePeers(final ArrayList<DtxNode> otherPeers) {
        final StringBuilder result = new StringBuilder();
        boolean filled = false;
        for (final DtxNode peer : otherPeers) {
            if (filled) {
                result.append(",");
            }
            filled = true;
            result.append(peer.getNodeId() + "@127.0.0.1:" + peer.getAddress().getPort());
        }
        return result.toString();
    }

    protected final static void stopNode(final int index) throws Exception {

        if (nodeStarted[index]) {

            if (msgClientObjectNames[index] != null) {
                try {
                    dummyMBeanServers[index].unregisterMBean(msgClientObjectNames[index]);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while unregistering the msg client", t);
                }
                finally {
                    msgClientObjectNames[index] = null;
                }
            }
            if (msgServerObjectNames[index] != null) {
                try {
                    dummyMBeanServers[index].unregisterMBean(msgServerObjectNames[index]);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while unregistering the msg server", t);
                }
                finally {
                    msgServerObjectNames[index] = null;
                }
            }

            if (voldClientServers[index] != null) {
                final VoldClientServer voldClientServer = voldClientServers[index];
                try {
                    voldClientServer.getVoldSyncServer().stop();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while stopping the vold sync server", t);
                }
                try {
                    voldClientServer.getMsgClientStartpoint().stop();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while stopping the msg client", t);
                }
                finally {
                    voldClientServers[index] = null;
                }
            }

            if (vvrManagers[index] != null) {
                try {
                    vvrManagers[index].fini();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while finishing the vvr manager", t);
                }
                finally {
                    vvrManagers[index] = null;
                }
            }

            if (dtxManagerObjectNames[index] != null) {
                try {
                    dummyMBeanServers[index].unregisterMBean(dtxManagerObjectNames[index]);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while unregistering the dtx manager", t);
                }
                finally {
                    dtxManagerObjectNames[index] = null;
                }
            }

            if (dtxLocalNodeObjectNames[index] != null) {
                try {
                    dummyMBeanServers[index].unregisterMBean(dtxLocalNodeObjectNames[index]);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while unregistering the local node", t);
                }
                finally {
                    dtxLocalNodeObjectNames[index] = null;
                }
            }

            if (dtxManagers[index] != null) {
                try {
                    dtxManagers[index].fini();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while finishing the DtxManager", t);
                }
                finally {
                    dtxManagers[index] = null;
                }
            }

            if (iscsiServers[index] != null) {
                try {
                    iscsiServers[index].stop();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while stopping the iscsi server", t);
                }
                finally {
                    iscsiServers[index] = null;
                }
            }

            if (nbdServers[index] != null) {
                try {
                    nbdServers[index].stop();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while stopping the nbd server", t);
                }
                finally {
                    nbdServers[index] = null;
                }
            }
            nodeStarted[index] = false;
        }
    }

    protected final static void startNode(final int index) throws Exception {

        if (!nodeStarted[index]) {
            final DtxNode currPeer = peerList.get(index);

            // ISCSI server
            final IscsiServer iscsiServer = VvrManagerTestUtils.createIscsiServer(dummyMBeanServers[index],
                    getVoldTestHelper(index));
            iscsiServers[index] = iscsiServer;

            // NBD server
            final NbdServer nbdServer = VvrManagerTestUtils.createNbdServer(dummyMBeanServers[index],
                    getVoldTestHelper(index));
            nbdServers[index] = nbdServer;

            // VVR Manager
            final VvrManager vvrManager = VvrManagerTestUtils.createVvrManager(dummyMBeanServers[index],
                    getVoldTestHelper(index), currPeer.getNodeId().toString(), iscsiServer, nbdServer);
            Assert.assertNotNull(vvrManager);
            vvrManagers[index] = vvrManager;

            // Java net
            final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(getVoldTestHelper(index)
                    .getVoldCfgFile(), VoldConfigurationContext.getInstance(), IscsiServerConfigurationContext
                    .getInstance(), NbdServerConfigurationContext.getInstance(), DtxConfigurationContext.getInstance());
            final VoldClientServer voldClientServer = MultiVoldUtils.initAndGetRemote(currPeer.getNodeId(), vvrManager,
                    metaConfiguration);
            vvrManager.setSyncClient(voldClientServer.getMsgClientStartpoint());
            voldClientServers[index] = voldClientServer;
            msgServerObjectNames[index] = voldClientServer.getVoldSyncServer().registerMXBean(dummyMBeanServers[index]);
            msgClientObjectNames[index] = voldClientServer.getMsgClientStartpoint().registerMXBean(
                    dummyMBeanServers[index]);

            final DtxNode currDtxPeer = dtxPeerList.get(index);

            final ArrayList<DtxNode> otherDtxPeers = new ArrayList<DtxNode>(dtxPeerList);
            otherDtxPeers.remove(currDtxPeer);

            // DTX Manager
            final DtxManager dtxManager = VvrManagerTestUtils.createDtxManager(dummyMBeanServers[index],
                    getVoldTestHelper(index), currDtxPeer, otherDtxPeers);
            dtxManagers[index] = dtxManager;

            // Register MBean
            dtxManagerObjectNames[index] = VvrManagerTestUtils.registerDtxManagerMXBean(dummyMBeanServers[index],
                    getVoldTestHelper(index), dtxManager);
            dtxLocalNodeObjectNames[index] = VvrManagerTestUtils.registerDtxLocalNodeMXBean(dummyMBeanServers[index],
                    getVoldTestHelper(index), dtxManager.new DtxLocalNode());

            VvrManagerHelper.initDtxManagement(vvrManager, dtxManager);

            nodeStarted[index] = true;
        }

    }

    protected final void restartVvrManagers() throws Exception {

        for (int j = 0; j < nbOfNodes; j++) {
            vvrManagers[j].fini();
        }
        Thread.sleep(1000);
        for (int j = 0; j < nbOfNodes; j++) {
            VvrManagerHelper.initDtxManagement(vvrManagers[j], dtxManagers[j]);
        }
    }

    protected final void restartNodes() throws Exception {

        for (int j = 0; j < nbOfNodes; j++) {
            stopNode(j);
        }
        Thread.sleep(1000);
        for (int j = 0; j < nbOfNodes; j++) {
            startNode(j);
        }
        // Wait for all the nodes are started correctly
        for (int i = 0; i < nbOfNodes; i++) {
            Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(i),
                    VoldTestHelper.MXBEANS_NUMBER_INIT));
        }
    }

    protected void setDeviceRO(final DummyMBeanServer server, final VoldTestHelper voldTestHelper, final UUID vvrUuid,
            final DeviceMXBean device) throws Exception {
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), server);
        Assert.assertTrue(device.isActive());
    }

    protected void setDeviceRW(final DummyMBeanServer server, final VoldTestHelper voldTestHelper, final UUID vvrUuid,
            final DeviceMXBean device) throws Exception {
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), server);
        Assert.assertTrue(device.isActive());
    }

    protected void setDeviceDeActivated(final DummyMBeanServer server, final VoldTestHelper voldTestHelper,
            final UUID vvrUuid, final DeviceMXBean device) throws Exception {
        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), server);
        Assert.assertFalse(device.isActive());
    }

    protected final DeviceMXBean getDeviceMXBeanOnOtherServer(final UUID vvrUuid, final DeviceMXBean device,
            final DummyMBeanServer server) throws MalformedObjectNameException {
        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(device.getUuid()));
        return (DeviceMXBean) server.getMXBean(on2);
    }

    protected final DeviceMXBean waitDeviceMXBeanOnOtherServer(final UUID vvrUuid, final DeviceMXBean device,
            final DummyMBeanServer server) throws MalformedObjectNameException {
        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(device.getUuid()));
        return (DeviceMXBean) server.waitMXBean(on2);
    }
}
