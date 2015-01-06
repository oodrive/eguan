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

import java.util.Set;
import java.util.UUID;

import javax.management.MalformedObjectNameException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oodrive.nuage.dtx.DtxLocalNodeMXBean;
import com.oodrive.nuage.dtx.DtxManagerMXBean;
import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.model.DummyMBeanServer;
import com.oodrive.nuage.vold.model.SnapshotMXBean;
import com.oodrive.nuage.vold.model.VoldTestHelper;
import com.oodrive.nuage.vold.model.VvrMXBean;
import com.oodrive.nuage.vold.model.VvrManagerMXBean;
import com.oodrive.nuage.vold.model.VvrManagerTestUtils;

public class TestMultiVoldStopAndRestoreL extends TestMultiVoldAbstract {

    private static int NB_NODES = 3;
    private static int NB_NODES_INIT = 2;

    private final String[] snapUuid = new String[3];
    private final String[] devUuid = new String[3];
    private final DeviceMXBean[] devices = new DeviceMXBean[3];
    private final SnapshotMXBean[] snapshots = new SnapshotMXBean[3];

    private final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
    private final long size1 = (long) (2048.0 * 1024.0 * 1024.0);
    private final long size2 = (long) (4096.0 * 1024.0 * 1024.0);

    @BeforeClass
    public static void init() throws Exception {
        // three nodess, only two started at the beginning, vvr not started automatically
        setUpVolds(NB_NODES, NB_NODES_INIT, false);
    }

    private void createVvr() throws Exception {
        // Create a vvr on node 1
        final VvrMXBean vvrMXBean = getVoldTestHelper(0).createVvr(getDummyMBeanServer(0), "name", "description");
        vvrUuid = UUID.fromString(vvrMXBean.getUuid());
    }

    private void startThirdNode(final int nbMXBeansToWait) throws Exception {
        startNode(2);
        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(2), nbMXBeansToWait));
        Assert.assertEquals(nbMXBeansToWait, getDummyMBeanServer(2).getNbMXBeans());
    }

    private void startAndCheckVvr(final int indexNodeStart, final int indexNodeEnd, final int nbMXBeansToWait)
            throws Exception {

        for (int i = indexNodeStart; i <= indexNodeEnd; i++) {

            // start the vvr
            VvrManagerTestUtils.getVvrMXBean(getDummyMBeanServer(i), VOLD_OWNER, vvrUuid).start();

            // wait mx bean number
            Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(i), nbMXBeansToWait));

            // check exactly the mxBean number
            Assert.assertEquals(nbMXBeansToWait, getDummyMBeanServer(i).getNbMXBeans());

            // Check vvr settings
            checkVvr(i, true);

        }

        // Get root snapshot
        final Set<SnapshotMXBean> snapshots = getVoldTestHelper(0).getSnapshots(getDummyMBeanServer(0), vvrUuid);
        Assert.assertEquals(1, snapshots.size());
        final SnapshotMXBean rootSnapshot = snapshots.iterator().next();
        rootUuid = UUID.fromString(rootSnapshot.getUuid());
    }

    private void checkVvr(final int node, final boolean isStarted) throws MalformedObjectNameException {

        final VvrMXBean vvr = VvrManagerTestUtils.getVvrMXBean(getDummyMBeanServer(node), VOLD_OWNER, vvrUuid);

        // Check vvr existence on this node
        Assert.assertNotNull(vvr);

        // Check if it is started or not
        if (isStarted) {
            Assert.assertTrue(vvr.isStarted());
        }
        else {
            Assert.assertFalse(vvr.isStarted());
        }
    }

    @Test
    public void testCreateDevicesOnThreeNodes() throws Exception {

        // Start Node 3 at the beginning
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT);

        // Create a vvr and check it
        createVvr();
        startAndCheckVvr(0, 2, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);

        final SnapshotMXBean rootSnapshot = VvrManagerTestUtils.getSnapshotMXBean(server1, VOLD_OWNER, vvrUuid,
                rootUuid);

        // Create some elements on the first node
        {
            devices[0] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev0",
                    size0);
            devUuid[0] = devices[0].getUuid();
            snapshots[0] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[0], "snap0", VOLD_OWNER,
                    vvrUuid);
            snapUuid[0] = snapshots[0].getUuid();
            devices[1] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev1",
                    size1);
            devUuid[1] = devices[1].getUuid();
            snapshots[1] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[1], "snap1", VOLD_OWNER,
                    vvrUuid);
            snapUuid[1] = snapshots[1].getUuid();

            snapshots[1].delete();

            snapshots[2] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[0], "snap2", VOLD_OWNER,
                    vvrUuid);
            snapUuid[2] = snapshots[2].getUuid();
            devices[0].delete();

            devices[2] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshots[2], vvrUuid, "dev2",
                    size2);
            devUuid[2] = devices[2].getUuid();
        }
        // Check that all the nodes contains the created devices and snapshots
        for (int i = 0; i < 3; i++) {
            checkSnapshotsAndDevices(getDummyMBeanServer(i), getVoldTestHelper(i));
        }

        // Restart all the nodes
        restartNodes();

        // Check again that all the devices and snapshots are presents on all the nodes
        for (int i = 0; i < NB_NODES; i++) {
            Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(getDummyMBeanServer(i),
                    VoldTestHelper.MXBEANS_NUMBER_INIT + 6));
        }

        // Check vvr is always started
        for (int i = 0; i < NB_NODES; i++) {
            checkVvr(i, true);
        }

        // Check that all the nodes contains the created devices and snapshots
        for (int i = 0; i < NB_NODES; i++) {
            checkSnapshotsAndDevices(getDummyMBeanServer(i), getVoldTestHelper(i));
        }
    }

    @Test
    public void testCreateVvrOnThreeNodesStopOne() throws Exception {

        // Start Node 3 at the beginning
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT);

        // Create a vvr on node 1
        createVvr();

        // Check MX Beans number on all the nodes
        for (int i = 0; i < NB_NODES; i++) {
            Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 1, getDummyMBeanServer(i).getNbMXBeans());
        }

        // Check if vvr exists and not started on each node
        for (int i = 0; i < NB_NODES; i++) {
            checkVvr(i, false);
        }

        // Stop Node 3
        stopNode(2);

        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);

        // start the vvr on the two other node
        startAndCheckVvr(0, 1, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        // Create 1 device on node 1
        final SnapshotMXBean rootSnapshot = VvrManagerTestUtils.getSnapshotMXBean(server1, VOLD_OWNER, vvrUuid,
                rootUuid);

        devices[0] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev0", size0);
        devUuid[0] = devices[0].getUuid();

        // check devices
        for (int i = 0; i < NB_NODES - 1; i++) {
            Assert.assertNotNull(VvrManagerTestUtils.getDeviceMXBean(getDummyMBeanServer(i), VOLD_OWNER, vvrUuid,
                    UUID.fromString(devUuid[0])));
        }

        // Restart Node 3
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 1);

        // Check VVr existence but not started
        checkVvr(2, false);

        // Start it on the 3rd node
        startAndCheckVvr(2, 2, VoldTestHelper.MXBEANS_NUMBER_INIT + 3);

        // Check root Snapshot
        Assert.assertNotNull(VvrManagerTestUtils.getSnapshotMXBean(getDummyMBeanServer(2), VOLD_OWNER, vvrUuid,
                rootUuid));

        // check device
        Assert.assertNotNull(VvrManagerTestUtils.getDeviceMXBean(getDummyMBeanServer(2), VOLD_OWNER, vvrUuid,
                UUID.fromString(devUuid[0])));

        // Stop Node 2
        stopNode(2);

        // Restart Node 2
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 3);

        // Check vvr is always started
        checkVvr(2, true);
    }

    @Test
    public void testCreateElementsStartOneNodeAfter() throws Exception {
        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);

        // Create a vvr and start it on the two node
        createVvr();
        startAndCheckVvr(0, 1, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        // Create a device and activate it
        final SnapshotMXBean rootSnapshot = VvrManagerTestUtils.getSnapshotMXBean(server1, VOLD_OWNER, vvrUuid,
                rootUuid);

        devices[0] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev0", size0);
        devUuid[0] = devices[0].getUuid();

        voldTestHelper1.waitTaskEnd(vvrUuid, devices[0].activateRW(), server1);
        Assert.assertTrue(devices[0].isActive());

        // Start the 3rd node
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 1);

        // Check VVr existence but not started
        checkVvr(2, false);
    }

    @Test
    public void testCreateStopRestartOneNode() throws Exception {

        // start third node at the beginning
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT);

        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);

        createVvr();
        startAndCheckVvr(0, 2, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        final SnapshotMXBean rootSnapshot = VvrManagerTestUtils.getSnapshotMXBean(server1, VOLD_OWNER, vvrUuid,
                rootUuid);

        // Create 1 element
        {
            devices[0] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev0",
                    size0);
            devUuid[0] = devices[0].getUuid();
        }
        // Stop Node 3
        stopNode(2);

        // Create other elements
        snapshots[0] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[0], "snap0", VOLD_OWNER,
                vvrUuid);
        snapUuid[0] = snapshots[0].getUuid();

        devices[1] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, "dev1", size1);
        devUuid[1] = devices[1].getUuid();

        snapshots[1] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[1], "snap1", VOLD_OWNER,
                vvrUuid);
        snapUuid[1] = snapshots[1].getUuid();

        snapshots[1].delete();

        snapshots[2] = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, devices[0], "snap2", VOLD_OWNER,
                vvrUuid);
        snapUuid[2] = snapshots[2].getUuid();
        devices[0].delete();

        devices[2] = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshots[2], vvrUuid, "dev2", size2);
        devUuid[2] = devices[2].getUuid();

        // Restart the Node 3
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 6);

        // Check its elements
        checkSnapshotsAndDevices(getDummyMBeanServer(2), getVoldTestHelper(2));
    }

    @Test
    public void testCreateDeviceCheckTasksOnThreeNode() throws Exception {

        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);

        // Start Node 3 at the beginning
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT);

        // Create a vvr and check it
        final String vvrTaskIdStr = getVvrManager(0).createVvrNoWait("name1", "description1");
        final String vvrUuidStr = voldTestHelper1.waitVvrManagerTaskEnd(vvrTaskIdStr, server1);
        vvrUuid = UUID.fromString(vvrUuidStr);

        // check the task on all the node
        for (int i = 0; i < 3; i++) {
            VvrManagerTestUtils
                    .checkVvrCreationTask(vvrTaskIdStr, vvrUuidStr, ((VvrManagerMXBean) getDummyMBeanServer(i)
                            .getMXBean(getVoldTestHelper(i).newVvrManagerObjectName())).getVvrManagerTask(vvrTaskIdStr));
        }

        // start the vvr
        startAndCheckVvr(0, 2, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        // Stop the third node
        stopNode(2);

        // Create a device
        final String deviceTaskUuidStr = VvrManagerTestUtils.getSnapshotRoot(server1, voldTestHelper1, vvrUuid)
                .createDevice("name", size0);
        // Wait task is committed
        final String deviceUuidStr = voldTestHelper1.waitTaskEnd(vvrUuid, deviceTaskUuidStr, server1);

        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 3);

        // Check if vvr creation task is ever here
        VvrManagerTestUtils.checkVvrCreationTask(vvrTaskIdStr, vvrUuidStr, ((VvrManagerMXBean) getDummyMBeanServer(2)
                .getMXBean(getVoldTestHelper(2).newVvrManagerObjectName())).getVvrManagerTask(vvrTaskIdStr));

        // check the device creation task on all the node
        for (int i = 0; i < NB_NODES; i++) {
            VvrManagerTestUtils.checkDeviceCreationTask(deviceTaskUuidStr, deviceUuidStr, ((VvrMXBean) server1
                    .getMXBean(getVoldTestHelper(i).newVvrObjectName(vvrUuid))).getVvrTask(deviceTaskUuidStr));
        }
    }

    @Test
    public void testCreateDeviceCheckDtxMXBeansOnThreeNode() throws Exception {

        final DummyMBeanServer server1 = getDummyMBeanServer(0);
        final VoldTestHelper voldTestHelper1 = getVoldTestHelper(0);
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);

        final DtxManagerMXBean[] dtxManagerMXBean = new DtxManagerMXBean[3];
        final DtxLocalNodeMXBean[] dtxLocalNodeMXBean = new DtxLocalNodeMXBean[3];

        for (int i = 0; i < NB_NODES_INIT; i++) {
            dtxManagerMXBean[i] = VvrManagerTestUtils.getDtxManagerMXBean(getDummyMBeanServer(i), VOLD_OWNER);
            dtxLocalNodeMXBean[i] = VvrManagerTestUtils.getDtxLocalNodeMXBean(getDummyMBeanServer(i), VOLD_OWNER);

            // Check request Queue
            VvrManagerTestUtils.checkRequestQueueEmpty(dtxManagerMXBean[i]);

            // Check dtx node, 2 peers
            VvrManagerTestUtils.checkDtxLocalNode(dtxLocalNodeMXBean[i], dtxPeerList.get(i).getAddress(), 2);

            // 3rd node is offline
            VvrManagerTestUtils.checkDtxLocalNodePeerStatus(dtxLocalNodeMXBean[i], dtxPeerList.get(2).getNodeId(),
                    false);

            // the other is online
            VvrManagerTestUtils.checkDtxLocalNodePeerStatus(dtxLocalNodeMXBean[i], dtxPeerList.get((i + 1) % 2)
                    .getNodeId(), true);

        }
        // Start Node 3 at the beginning
        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT);

        // check the same
        dtxManagerMXBean[2] = VvrManagerTestUtils.getDtxManagerMXBean(getDummyMBeanServer(2), VOLD_OWNER);
        dtxLocalNodeMXBean[2] = VvrManagerTestUtils.getDtxLocalNodeMXBean(getDummyMBeanServer(2), VOLD_OWNER);

        VvrManagerTestUtils.checkRequestQueueEmpty(dtxManagerMXBean[2]);
        VvrManagerTestUtils.checkDtxLocalNode(dtxLocalNodeMXBean[2], dtxPeerList.get(2).getAddress(), 2);

        // the peers are all online
        for (int i = 0; i < 2; i++) {
            VvrManagerTestUtils
                    .checkDtxLocalNodePeerStatus(dtxLocalNodeMXBean[2], dtxPeerList.get(i).getNodeId(), true);
        }

        final int[] before = new int[NB_NODES];
        for (int i = 0; i < NB_NODES; i++) {
            before[i] = dtxManagerMXBean[i].getResourceManagerTasks(VOLD_OWNER.toString()).length;
        }
        // Create a vvr and check it
        final String vvrTaskIdStr = getVvrManager(0).createVvrNoWait("name1", "description1");
        final String vvrUuidStr = voldTestHelper1.waitVvrManagerTaskEnd(vvrTaskIdStr, server1);
        vvrUuid = UUID.fromString(vvrUuidStr);

        for (int i = 0; i < NB_NODES; i++) {
            // Check Resource Manager
            VvrManagerTestUtils.checkResourceManagersAfterVvrCreation(dtxManagerMXBean[i], vvrUuid, VOLD_OWNER);
            VvrManagerTestUtils.checkDtxTasksListAfterVvrCreation(dtxManagerMXBean[i], vvrUuid, VOLD_OWNER, before[i]);
        }

        startAndCheckVvr(0, 2, VoldTestHelper.MXBEANS_NUMBER_INIT + 2);

        for (int i = 0; i < NB_NODES; i++) {
            before[i] = dtxManagerMXBean[i].getResourceManagerTasks(vvrUuidStr).length;
        }
        // Stop the third node
        stopNode(2);

        // Create a device
        final String deviceTaskUuidStr = VvrManagerTestUtils.getSnapshotRoot(server1, voldTestHelper1, vvrUuid)
                .createDevice("name", size0);
        voldTestHelper1.waitTaskEnd(vvrUuid, deviceTaskUuidStr, server1);

        for (int i = 0; i < 2; i++) {
            VvrManagerTestUtils.checkRequestQueueEmpty(dtxManagerMXBean[i]);
            // Check Resource Manager
            VvrManagerTestUtils.checkResourceManagersAfterDeviceCreation(dtxManagerMXBean[i], vvrUuid, VOLD_OWNER);
            VvrManagerTestUtils.checkDtxTasksListAfterDeviceCreation(dtxManagerMXBean[i], vvrUuid, deviceTaskUuidStr,
                    before[i]);
        }

        startThirdNode(VoldTestHelper.MXBEANS_NUMBER_INIT + 3);

        dtxManagerMXBean[2] = VvrManagerTestUtils.getDtxManagerMXBean(getDummyMBeanServer(2), VOLD_OWNER);
        dtxLocalNodeMXBean[2] = VvrManagerTestUtils.getDtxLocalNodeMXBean(getDummyMBeanServer(2), VOLD_OWNER);

        VvrManagerTestUtils.checkRequestQueueEmpty(dtxManagerMXBean[2]);

        // Check resource Manager on the third node
        VvrManagerTestUtils.checkResourceManagersAfterDeviceCreation(dtxManagerMXBean[2], vvrUuid, VOLD_OWNER);
        VvrManagerTestUtils.checkDtxTasksListAfterDeviceCreation(dtxManagerMXBean[2], vvrUuid, deviceTaskUuidStr,
                before[2]);
    }

    private void checkSnapshotsAndDevices(final DummyMBeanServer server, final VoldTestHelper voldTestHelper)
            throws MalformedObjectNameException {
        final SnapshotMXBean rootSnapshot = VvrManagerTestUtils
                .getSnapshotMXBean(server, VOLD_OWNER, vvrUuid, rootUuid);
        // Basic check
        final boolean[] devicesFound = new boolean[2];
        final boolean[] snapFound = new boolean[2];

        {
            final String[] rootChildren = rootSnapshot.getChildrenSnapshots();
            for (final String uuid : rootChildren) {
                if (uuid.equals(snapUuid[0])) {
                    snapFound[0] = true;
                }
                else if (uuid.equals(snapUuid[1])) {
                    snapFound[1] = true;
                }
                else {
                    throw new AssertionError("Unknown UUID: " + uuid);
                }
            }
        }
        {
            final String[] rootChildren = rootSnapshot.getChildrenDevices();
            for (final String uuid : rootChildren) {
                if (uuid.equals(devUuid[0])) {
                    devicesFound[0] = true;
                }
                else if (uuid.equals(devUuid[1])) {
                    devicesFound[1] = true;
                }
                else {
                    throw new AssertionError("Unknown UUID: " + uuid);
                }
            }
        }

        Assert.assertFalse(devicesFound[0]);
        Assert.assertTrue(devicesFound[1]);
        Assert.assertTrue(snapFound[0]);
        Assert.assertFalse(snapFound[1]);

        final SnapshotMXBean snapshot2 = VvrManagerTestUtils.getSnapshotMXBean(server, VOLD_OWNER, vvrUuid,
                UUID.fromString(snapUuid[2]));
        Assert.assertEquals("snap2", snapshot2.getName());
        Assert.assertEquals(size0, snapshot2.getSize());

        {
            final String[] snap2Children = snapshot2.getChildrenSnapshots();
            Assert.assertEquals(0, snap2Children.length);
        }
        {
            final String[] snap2Children = snapshot2.getChildrenDevices();
            Assert.assertEquals(1, snap2Children.length);
            Assert.assertEquals(devUuid[2], snap2Children[0]);
        }
    }
}
