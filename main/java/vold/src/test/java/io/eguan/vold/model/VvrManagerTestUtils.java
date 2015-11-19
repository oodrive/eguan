package io.eguan.vold.model;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.dtx.DtxConstants;
import io.eguan.dtx.DtxLocalNodeMXBean;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxManagerMXBean;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.DtxNodeState;
import io.eguan.dtx.DtxPeerAdm;
import io.eguan.dtx.DtxRequestQueueAdm;
import io.eguan.dtx.DtxResourceManagerAdm;
import io.eguan.dtx.DtxResourceManagerState;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskStatus;
import io.eguan.dtx.DtxManager.DtxLocalNode;
import io.eguan.dtx.DtxPeerAdm.DtxPeerStatus;
import io.eguan.dtx.DtxResourceManagerAdm.DtxJournalStatus;
import io.eguan.dtx.config.DtxConfigurationContext;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiServerConfigurationContext;
import io.eguan.nbdsrv.NbdServer;
import io.eguan.nbdsrv.NbdServerConfigurationContext;
import io.eguan.nrs.NrsConfigurationContext;
import io.eguan.utils.mapper.FileMapperConfigurationContext;
import io.eguan.vold.EnableIscsiConfigKey;
import io.eguan.vold.EnableNbdConfigKey;
import io.eguan.vold.NodeConfigKey;
import io.eguan.vold.OwnerConfigKey;
import io.eguan.vold.VoldConfigurationContext;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrManager;
import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrManagerTargetType;
import io.eguan.vold.model.VvrManagerTask;
import io.eguan.vold.model.VvrManagerTaskOperation;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.model.VvrTask;
import io.eguan.vvr.configuration.CommonConfigurationContext;
import io.eguan.vvr.configuration.IbsConfigurationContext;
import io.eguan.vvr.configuration.PersistenceConfigurationContext;
import io.eguan.vvr.persistence.repository.VvrTargetType;
import io.eguan.vvr.persistence.repository.VvrTaskOperation;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Assert;

public abstract class VvrManagerTestUtils {
    /**
     * Create a {@link VvrManager} instance using {@link DummyMBeanServer} as the MBeanServer.
     * 
     * @param voldTestHelper
     * 
     * @return A {@link VvrManager} object.
     */
    public final static VvrManager createVvrManager(final VoldTestHelper voldTestHelper) throws NullPointerException,
            IllegalArgumentException, IOException, ConfigValidationException {

        final InetAddress bindAddr = InetAddress.getLoopbackAddress();
        return createVvrManager(DummyMBeanServer.getMBeanServer1(), voldTestHelper, null, new IscsiServer(bindAddr),
                new NbdServer(bindAddr));
    }

    /**
     * Create a {@link VvrManager} instance using {@link DummyMBeanServer} as the MBeanServer.
     * 
     * @param voldTestHelper
     * 
     * @return A {@link VvrManager} object.
     */
    public final static VvrManager createVvrManager(final VoldTestHelper voldTestHelper,
            final DummyMBeanServer dummyMBeanServer) throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {

        final InetAddress bindAddr = InetAddress.getLoopbackAddress();
        return createVvrManager(dummyMBeanServer, voldTestHelper, null, new IscsiServer(bindAddr), new NbdServer(
                bindAddr));
    }

    /**
     * Create a {@link VvrManager}
     * 
     * @param voldTestHelper
     * @param dummyMBeanServer
     * @param nodeId
     *            the node ID to set
     * @param iscsi
     *            server
     * 
     * @return A {@link VvrManager} object.
     */
    public final static VvrManager createVvrManager(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper, final String nodeId, final IscsiServer iscsiServer,
            final NbdServer nbdServer) throws NullPointerException, IllegalArgumentException, IOException,
            ConfigValidationException {
        // Create the MetaConfiguration from the VVR template file
        final File voldDir = voldTestHelper.getVoldFile();

        final MetaConfiguration vvrTemplate = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VVR_TEMPLATE), CommonConfigurationContext.getInstance(), FileMapperConfigurationContext
                .getInstance(), IbsConfigurationContext.getInstance(), NrsConfigurationContext.getInstance(),
                PersistenceConfigurationContext.getInstance());

        // Create the MetaConfiguration from the VOLD file
        final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VOLD_CONFIG_FILE), VoldConfigurationContext.getInstance(),
                IscsiServerConfigurationContext.getInstance(), NbdServerConfigurationContext.getInstance(),
                DtxConfigurationContext.getInstance());

        final UUID nodeUuid = (nodeId == null) ? NodeConfigKey.getInstance().getTypedValue(metaConfiguration) : UUID
                .fromString(nodeId);
        final VvrManager vvrManager = new VvrManager(dummyMBeanServer, OwnerConfigKey.getInstance().getTypedValue(
                metaConfiguration), nodeUuid, voldTestHelper.getVvrsFile(), vvrTemplate, iscsiServer, nbdServer);

        return vvrManager;
    }

    public final static DtxManager createDtxManagerStandAlone(final VoldTestHelper voldTestHelper)
            throws NullPointerException, IllegalArgumentException, IOException, ConfigValidationException {
        return createDtxManagerStandAlone(DummyMBeanServer.getMBeanServer1(), voldTestHelper);
    }

    public final static DtxManager createDtxManagerStandAlone(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper) throws NullPointerException, IllegalArgumentException, IOException,
            ConfigValidationException {
        final File voldDir = voldTestHelper.getVoldFile();

        // Create the MetaConfiguration from the VOLD file
        final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VOLD_CONFIG_FILE), VoldConfigurationContext.getInstance(),
                IscsiServerConfigurationContext.getInstance(), NbdServerConfigurationContext.getInstance(),
                DtxConfigurationContext.getInstance());
        final UUID nodeUuid = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);

        final List<DtxNode> dtxNodes = Collections.emptyList();

        // Local node: stand alone and dynamic port (no incoming connection)
        final InetSocketAddress localPeer = new InetSocketAddress("localhost", 0);
        final DtxNode localDtxNode = new DtxNode(nodeUuid, localPeer);

        final String clusterName = OwnerConfigKey.getInstance().getTypedValue(metaConfiguration).toString();
        final String clusterPassword = clusterName; // TODO temporary password

        final DtxManagerConfig dtxManagerConfig = new DtxManagerConfig(metaConfiguration, FileSystems.getDefault()
                .getPath(voldDir.getAbsolutePath()), clusterName, clusterPassword, localDtxNode,
                dtxNodes.toArray(new DtxNode[dtxNodes.size()]));
        final DtxManager dtxManager = new DtxManager(dtxManagerConfig);
        dtxManager.init();
        dtxManager.start();

        return dtxManager;
    }

    public final static DtxManager createDtxManager(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper, final DtxNode localDtxNode, final ArrayList<DtxNode> otherPeers)
            throws NullPointerException, IllegalArgumentException, IOException, ConfigValidationException {
        // Create the config of dtx manager
        // Create the MetaConfiguration from the VVR template file
        final File voldDir = voldTestHelper.getVoldFile();

        // Create the MetaConfiguration from the VOLD file
        final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VOLD_CONFIG_FILE), VoldConfigurationContext.getInstance(),
                IscsiServerConfigurationContext.getInstance(), NbdServerConfigurationContext.getInstance(),
                DtxConfigurationContext.getInstance());

        final DtxManagerConfig dtxManagerConfig = new DtxManagerConfig(metaConfiguration, FileSystems.getDefault()
                .getPath(voldTestHelper.getVoldPath()), "clusterName", "clusterPassword", localDtxNode,
                otherPeers.toArray(new DtxNode[otherPeers.size()]));

        final DtxManager dtxManager = new DtxManager(dtxManagerConfig);
        dtxManager.init();
        dtxManager.start();

        return dtxManager;
    }

    public final static ObjectName registerDtxManagerMXBean(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper, final DtxManager dtxManager) {
        final ObjectName dtxManagerObjectName = voldTestHelper.newDtxManagerObjectName();
        dummyMBeanServer.register(dtxManager, dtxManagerObjectName);
        return dtxManagerObjectName;
    }

    public final static ObjectName registerDtxLocalNodeMXBean(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper, final DtxLocalNode dtxLocalNode) {
        final ObjectName dtxLocalNodeObjectName = voldTestHelper.newDtxLocalNodeObjectName();
        dummyMBeanServer.register(dtxLocalNode, dtxLocalNodeObjectName);
        return dtxLocalNodeObjectName;
    }

    public final static IscsiServer createIscsiServer(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper) throws Exception {

        // Get the config of dtx manager
        // Create the MetaConfiguration from the VVR template file
        final File voldDir = voldTestHelper.getVoldFile();

        // Create the MetaConfiguration from the VOLD file
        final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VOLD_CONFIG_FILE), VoldConfigurationContext.getInstance(),
                IscsiServerConfigurationContext.getInstance(), NbdServerConfigurationContext.getInstance(),
                DtxConfigurationContext.getInstance());

        // Create server
        final IscsiServer iscsiServer = new IscsiServer(metaConfiguration);
        final boolean enableIscsiServer = EnableIscsiConfigKey.getInstance().getTypedValue(metaConfiguration)
                .booleanValue();
        if (enableIscsiServer) {
            iscsiServer.start();
        }

        // Export MXBean?
        // final String iscsiServerObjNameStr = iscsiServer.getClass().getPackage().getName() + ":type=Server";
        // ObjectName iscsiServerObjName = new ObjectName(iscsiServerObjNameStr);
        // dummyMBeanServer.registerMBean(iscsiServer, iscsiServerObjName);
        return iscsiServer;
    }

    public final static ObjectName getIscsiServerObjectName(final IscsiServer iscsiServer)
            throws MalformedObjectNameException {
        return new ObjectName(iscsiServer.getClass().getPackage().getName() + ":type=Server");
    }

    public final static NbdServer createNbdServer(final DummyMBeanServer dummyMBeanServer,
            final VoldTestHelper voldTestHelper) throws Exception {

        // Get the config of dtx manager
        // Create the MetaConfiguration from the VVR template file
        final File voldDir = voldTestHelper.getVoldFile();

        // Create the MetaConfiguration from the VOLD file
        final MetaConfiguration metaConfiguration = MetaConfiguration.newConfiguration(new File(voldDir,
                VoldTestHelper.VOLD_CONFIG_FILE), VoldConfigurationContext.getInstance(),
                IscsiServerConfigurationContext.getInstance(), NbdServerConfigurationContext.getInstance(),
                DtxConfigurationContext.getInstance());

        // Create server
        final NbdServer nbdServer = new NbdServer(metaConfiguration);
        final boolean enableNbdServer = EnableNbdConfigKey.getInstance().getTypedValue(metaConfiguration)
                .booleanValue();
        if (enableNbdServer) {
            nbdServer.start();
        }

        // Export MXBean?
        // final String nbdServerObjNameStr = nbdServer.getClass().getPackage().getName() + ":type=Server";
        // ObjectName nbdServerObjName = new ObjectName(nbdServerObjNameStr);
        // dummyMBeanServer.registerMBean(nbdServer, nbdServerObjName);
        return nbdServer;
    }

    public final static ObjectName getNbdServerObjectName(final NbdServer nbdServer)
            throws MalformedObjectNameException {
        return new ObjectName(nbdServer.getClass().getPackage().getName() + ":type=Server");
    }

    public final static ObjectName getVvrManagerObjectName(final UUID ownerUuid) throws MalformedObjectNameException {
        return VvrObjectNameFactory.newVvrManagerObjectName(ownerUuid);
    }

    public final static ObjectName getDtxManagerObjectName(final UUID ownerUuid) throws MalformedObjectNameException {
        return VvrObjectNameFactory.newDtxManagerObjectName(ownerUuid);
    }

    public final static DtxManagerMXBean getDtxManagerMXBean(final DummyMBeanServer server, final UUID ownerUuid)
            throws MalformedObjectNameException {

        final ObjectName dtxManagerObjectName = getDtxManagerObjectName(ownerUuid);
        return (DtxManagerMXBean) server.getMXBean(dtxManagerObjectName);
    }

    public final static ObjectName getDtxLocalNodeObjectName(final UUID ownerUuid) throws MalformedObjectNameException {
        return VvrObjectNameFactory.newDtxLocalNodeObjectName(ownerUuid);
    }

    public final static DtxLocalNodeMXBean getDtxLocalNodeMXBean(final DummyMBeanServer server, final UUID ownerUuid)
            throws MalformedObjectNameException {

        final ObjectName dtxLocalNodeObjectName = getDtxLocalNodeObjectName(ownerUuid);
        return (DtxLocalNodeMXBean) server.getMXBean(dtxLocalNodeObjectName);
    }

    public final static VvrManagerMXBean getVvrManagerMXBean(final DummyMBeanServer server, final UUID ownerUuid)
            throws MalformedObjectNameException {

        final ObjectName vvrManagerObjectName = getVvrManagerObjectName(ownerUuid);
        return (VvrManagerMXBean) server.getMXBean(vvrManagerObjectName);
    }

    public final static ObjectName getVvrObjectName(final UUID ownerUuid, final UUID vvrUuid)
            throws MalformedObjectNameException {
        return VvrObjectNameFactory.newVvrObjectName(ownerUuid, vvrUuid);
    }

    public final static VvrMXBean getVvrMXBean(final DummyMBeanServer server, final UUID ownerUuid, final UUID vvrUuid)
            throws MalformedObjectNameException {

        final ObjectName vvrObjectName = getVvrObjectName(ownerUuid, vvrUuid);

        return (VvrMXBean) server.getMXBean(vvrObjectName);
    }

    public final static ObjectName getDeviceObjectName(final UUID ownerUuid, final UUID vvrUuid, final UUID deviceUuid)
            throws MalformedObjectNameException {
        return VvrObjectNameFactory.newDeviceObjectName(ownerUuid, vvrUuid, deviceUuid);
    }

    public final static DeviceMXBean getDeviceMXBean(final DummyMBeanServer server, final UUID ownerUuid,
            final UUID vvrUuid, final UUID deviceUuid) throws MalformedObjectNameException {

        final ObjectName deviceObjectName = getDeviceObjectName(ownerUuid, vvrUuid, deviceUuid);
        return (DeviceMXBean) server.getMXBean(deviceObjectName);
    }

    public final static ObjectName getSnapshotObjectName(final UUID ownerUuid, final UUID vvrUuid,
            final UUID snapshotUuid) throws MalformedObjectNameException {
        return VvrObjectNameFactory.newSnapshotObjectName(ownerUuid, vvrUuid, snapshotUuid);
    }

    public final static SnapshotMXBean getSnapshotMXBean(final DummyMBeanServer server, final UUID ownerUuid,
            final UUID vvrUuid, final UUID snapshotUuid) throws MalformedObjectNameException {

        final ObjectName snapshotObjectName = getSnapshotObjectName(ownerUuid, vvrUuid, snapshotUuid);
        return (SnapshotMXBean) server.getMXBean(snapshotObjectName);
    }

    public final static SnapshotMXBean getSnapshotRoot(final MBeanServer server, final VoldTestHelper voldTestHelper,
            final UUID vvrUuid) {
        final Set<SnapshotMXBean> snapshots = voldTestHelper.getSnapshots(server, vvrUuid);
        final SnapshotMXBean snapshotRoot = snapshots.iterator().next();
        Assert.assertNotNull(snapshotRoot);
        return snapshotRoot;
    }

    public final static DeviceMXBean createDevice(final DummyMBeanServer server, final VoldTestHelper voldTestHelper,
            final SnapshotMXBean snapshot, final UUID vvrUuid, final String deviceName, final long size) {
        final String deviceTaskUuid = snapshot.createDevice(deviceName, size);
        final DeviceMXBean device = voldTestHelper.getDevice(server, vvrUuid, deviceTaskUuid);
        Assert.assertNotNull(device);
        return device;
    }

    public final static SnapshotMXBean takeSnapshot(final DummyMBeanServer server, final VoldTestHelper voldTestHelper,
            final DeviceMXBean device, final String snapshotName, final UUID ownerUuid, final UUID vvrUuid) {
        final String snapshotTaskUuid = device.takeSnapshot(snapshotName);
        final SnapshotMXBean snapshot = voldTestHelper.getSnapshot(server, vvrUuid, snapshotTaskUuid);
        Assert.assertNotNull(snapshot);
        return snapshot;
    }

    /**
     * Wait for the apparition of MXBeans.
     * 
     * @param server
     *            a dummyMBeanServer
     * @param nbMXBean
     *            the wanted number of mx beans
     * 
     * @return true if there are at least nbMXBean registered MXBeans, otherwise false.
     */
    public final static boolean waitMXBeanNumber(final DummyMBeanServer server, final int nbMXBean)
            throws InterruptedException {
        int i = 0;
        int currentNumber = 0;
        while ((currentNumber = server.getNbMXBeans()) < nbMXBean && i < 100) {
            Thread.sleep(200);
            i++;
        }
        if (currentNumber >= nbMXBean) {
            return true;
        }
        else {
            return false;
        }
    }

    public final static void checkVvrCreationTask(final String vvrTaskUuid, final String vvrUuid,
            final VvrManagerTask task) {
        Assert.assertEquals(vvrTaskUuid, task.getTaskId());

        Assert.assertEquals(DtxTaskStatus.COMMITTED, task.getStatus());

        Assert.assertEquals(vvrUuid, task.getInfo().getTargetId());

        Assert.assertEquals(VvrManagerTaskOperation.CREATE, task.getInfo().getOperation());

        Assert.assertEquals(VvrManagerTargetType.VVR, task.getInfo().getTargetType());
    }

    public final static void checkDeviceCreationTask(final String deviceTaskUuid, final String deviceUuid,
            final VvrTask task) {
        Assert.assertEquals(deviceTaskUuid, task.getTaskId());

        Assert.assertEquals(DtxTaskStatus.COMMITTED, task.getStatus());

        Assert.assertEquals(deviceUuid, task.getInfo().getTargetId());

        Assert.assertEquals(VvrTaskOperation.CREATE, task.getInfo().getOperation());

        Assert.assertEquals(VvrTargetType.DEVICE, task.getInfo().getTargetType());
    }

    public final static void checkResourceManagersAfterVvrCreation(final DtxManagerMXBean dtxManager,
            final UUID vvrUuid, final UUID owner) {
        final DtxResourceManagerAdm[] resourceManagers = dtxManager.getResourceManagers();
        assertEquals(2, resourceManagers.length);

        boolean foundVvr = false;
        boolean foundVvrManager = false;
        for (final DtxResourceManagerAdm resMgr : resourceManagers) {
            assertEquals(DtxResourceManagerState.UP_TO_DATE, resMgr.getStatus());
            if (resMgr.getUuid().equals(vvrUuid.toString())) {
                foundVvr = true;
                assertTrue(DtxConstants.DEFAULT_LAST_TX_VALUE == resMgr.getLastTransaction());
            }
            else if (resMgr.getUuid().equals(owner.toString())) {
                foundVvrManager = true;
                assertFalse(DtxConstants.DEFAULT_LAST_TX_VALUE == resMgr.getLastTransaction());
            }
            assertFalse("".equals(resMgr.getJournalPath()));
            assertTrue(DtxJournalStatus.STARTED.equals(resMgr.getJournalStatus()));
        }
        assertTrue(foundVvr);
        assertTrue(foundVvrManager);
    }

    public final static void checkResourceManagersAfterDeviceCreation(final DtxManagerMXBean dtxManager,
            final UUID vvrUuid, final UUID owner) {
        final DtxResourceManagerAdm[] resourceManagers = dtxManager.getResourceManagers();
        assertEquals(2, resourceManagers.length);

        boolean foundVvr = false;
        boolean foundVvrManager = false;
        for (final DtxResourceManagerAdm resMgr : resourceManagers) {
            assertEquals(DtxResourceManagerState.UP_TO_DATE, resMgr.getStatus());
            if (resMgr.getUuid().equals(vvrUuid.toString())) {
                foundVvr = true;
                assertFalse(DtxConstants.DEFAULT_LAST_TX_VALUE == resMgr.getLastTransaction());
            }
            else if (resMgr.getUuid().equals(owner.toString())) {
                foundVvrManager = true;
                assertFalse(DtxConstants.DEFAULT_LAST_TX_VALUE == resMgr.getLastTransaction());
            }
            assertFalse("".equals(resMgr.getJournalPath()));
            assertTrue(DtxJournalStatus.STARTED.equals(resMgr.getJournalStatus()));
        }
        assertTrue(foundVvr);
        assertTrue(foundVvrManager);
    }

    public static final void checkRequestQueueEmpty(final DtxManagerMXBean dtxManager) {
        // No pending request
        final DtxRequestQueueAdm requestQueue = dtxManager.getRequestQueue();
        assertEquals(0, requestQueue.getNbOfPendingRequests());
        assertEquals("", requestQueue.getNextTaskID());
        assertEquals("", requestQueue.getNextResourceManagerID());
    }

    public static final void checkDtxTasksListAfterVvrCreation(final DtxManagerMXBean dtxManager, final UUID vvrUuid,
            final UUID owner, final int before) {
        // Check tasks list
        final DtxTaskAdm[] vvrManagerDtxTasks = dtxManager.getResourceManagerTasks(owner.toString());
        assertEquals(before + 1, vvrManagerDtxTasks.length);
        final DtxTaskAdm[] vvrDtxTasks = dtxManager.getResourceManagerTasks(vvrUuid.toString());
        assertEquals(0, vvrDtxTasks.length);
    }

    public static final void checkDtxTasksListAfterDeviceCreation(final DtxManagerMXBean dtxManager,
            final UUID vvrUuid, final String deviceTaskUuidStr, final int before) {
        // Check vvr Tasks
        final DtxTaskAdm[] vvrDtxTasks = dtxManager.getResourceManagerTasks(vvrUuid.toString());
        assertEquals(before + 3, vvrDtxTasks.length);
        boolean foundTask = false;
        for (final DtxTaskAdm task : vvrDtxTasks) {
            if (deviceTaskUuidStr.equals(task.getTaskId())) {
                assertEquals(DtxTaskStatus.COMMITTED, vvrDtxTasks[0].getStatus());
                foundTask = true;
            }
        }
        assertTrue(foundTask);
    }

    public static final void checkDtxLocalNode(final DtxLocalNodeMXBean dtxLocalNode,
            final InetSocketAddress localPeer, final int nbPeers) {
        // Node state is STARTED
        assertEquals(DtxNodeState.STARTED, dtxLocalNode.getStatus());

        assertEquals(localPeer.getAddress().getHostAddress(), dtxLocalNode.getIpAddress());
        assertEquals(localPeer.getPort(), dtxLocalNode.getPort());
        // No peers
        assertEquals(nbPeers, dtxLocalNode.getPeers().length);
        assertEquals(dtxLocalNode.getCurrentAtomicLong(), dtxLocalNode.getNextAtomicLong());
    }

    public static final void checkDtxLocalNodePeerStatus(final DtxLocalNodeMXBean dtxLocalNode, final UUID peerUuid,
            final boolean isStarted) {
        final DtxPeerAdm[] peers = dtxLocalNode.getPeers();
        boolean found = false;
        for (final DtxPeerAdm peer : peers) {
            if (peer.getUuid().equals(peerUuid.toString())) {
                found = true;
                if (isStarted) {
                    assertEquals(DtxPeerStatus.ONLINE, peer.getStatus());
                }
                else {
                    assertEquals(DtxPeerStatus.OFFLINE, peer.getStatus());
                }
            }
        }
        assertTrue(found);
    }

}
