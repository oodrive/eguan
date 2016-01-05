package io.eguan.vold.model;

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

import io.eguan.dtx.DtxLocalNodeMXBean;
import io.eguan.dtx.DtxManagerMXBean;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrMXBean;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Test;

/**
 * Create a few elements and check VVR after VOLD restart.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class TestVoldStopAndRestore extends AbstractVoldTest {

    public TestVoldStopAndRestore() throws Exception {
        super();
    }

    /** Address set in stand-alone mode */
    private static final String STANDALONE_HOST_ADDR = "localhost";

    /**
     * Check if when the JMX notification for the snapshot registration is sending, its device children are present.
     * 
     * @throws InstanceNotFoundException
     * @throws InterruptedException
     * @throws ListenerNotFoundException
     */
    @Test
    public void testCreateSnapshotVerifyHierarchy() throws InstanceNotFoundException, InterruptedException,
            ListenerNotFoundException {
        final CountDownLatch latch = new CountDownLatch(1);

        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);

        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);

        final String devUuidTaskStr = rootSnapshot.createDevice("dev0", size0);
        final DeviceMXBean device = helper.getDevice(vvrUuid, devUuidTaskStr);
        final String devUuid = device.getUuid();

        // Add a listener
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final NotificationListener notificationListener = new NotificationListener() {

            @Override
            public final void handleNotification(final Notification notification, final Object handback) {
                final MBeanServerNotification mbs = (MBeanServerNotification) notification;

                if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {

                    final ObjectName objectname = mbs.getMBeanName();
                    if (objectname.getKeyProperty("type").equals("Snapshot")) {

                        final SnapshotMXBean snapshotMXBean = JMX.newMXBeanProxy(
                                ManagementFactory.getPlatformMBeanServer(), objectname, SnapshotMXBean.class, false);
                        final String[] devices = snapshotMXBean.getChildrenDevices();

                        if ((devices.length != 0) && (devices[0].equals(devUuid))) {
                            latch.countDown();
                        }
                    }
                }
            }
        };

        server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener, null, null);
        try {
            device.takeSnapshot();
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
        finally {
            server.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, notificationListener);
        }
    }

    @Test
    public void testCreateStopRestore() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final long size1 = (long) (2048.0 * 1024.0 * 1024.0);
        final long size2 = (long) (4096.0 * 1024.0 * 1024.0);

        final String[] snapUuid = new String[3];
        final String[] devUuid = new String[3];

        // Create elements
        {
            final String[] snapUuidTaskStr = new String[3];
            final String[] devUuidTaskStr = new String[3];

            final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
            final DeviceMXBean[] devices = new DeviceMXBean[3];
            final SnapshotMXBean[] snapshots = new SnapshotMXBean[3];

            devUuidTaskStr[0] = rootSnapshot.createDevice("dev0", size0);
            devices[0] = helper.getDevice(vvrUuid, devUuidTaskStr[0]);
            devUuid[0] = devices[0].getUuid();

            devUuidTaskStr[1] = rootSnapshot.createDevice("dev1", size1);
            devices[1] = helper.getDevice(vvrUuid, devUuidTaskStr[1]);
            devUuid[1] = devices[1].getUuid();

            {
                final String uuid = UUID.randomUUID().toString();
                snapUuidTaskStr[0] = devices[0].takeSnapshotUuid("snap0", uuid);
                snapshots[0] = helper.getSnapshot(vvrUuid, snapUuidTaskStr[0]);
                snapUuid[0] = snapshots[0].getUuid();
                Assert.assertEquals("snap0", snapshots[0].getName());
                Assert.assertEquals(snapUuid[0], uuid);
            }

            {
                final String uuid = UUID.randomUUID().toString();
                snapUuidTaskStr[1] = devices[1].takeSnapshotUuid(uuid);
                snapshots[1] = helper.getSnapshot(vvrUuid, snapUuidTaskStr[1]);
                snapUuid[1] = snapshots[1].getUuid();
                Assert.assertEquals(snapUuid[1], uuid);
                snapshots[1].delete();
            }

            snapUuidTaskStr[2] = devices[0].takeSnapshot("snap2");
            snapshots[2] = helper.getSnapshot(vvrUuid, snapUuidTaskStr[2]);
            snapUuid[2] = snapshots[2].getUuid();

            devices[0].delete();

            devUuidTaskStr[2] = snapshots[2].createDevice("dev2", size2);
            devices[2] = helper.getDevice(vvrUuid, devUuidTaskStr[2]);
            devUuid[2] = devices[2].getUuid();
        }

        // Restart VOLD
        restartVold();

        Assert.assertTrue(helper.waitMXBeanRegistration(helper.newVvrObjectName(vvrUuid)));

        {
            final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);

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

            final SnapshotMXBean snapshot2 = helper.getSnapshot(vvrUuid, UUID.fromString(snapUuid[2]));
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

    @Test
    public void testResizeNoWait() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final long size1 = (long) (2048.0 * 1024.0 * 1024.0);
        final long size2 = (long) (4096.0 * 1024.0 * 1024.0);

        final UUID[] devUuid = new UUID[3];
        final String rootUuidStr = rootUuid.toString();
        final String snapUuidStr;
        // Create elements
        {
            final String[] devTaskUuid = new String[3];
            final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
            final DeviceMXBean[] devices = new DeviceMXBean[3];

            devTaskUuid[0] = rootSnapshot.createDevice("dev0", size0);
            devices[0] = helper.getDevice(vvrUuid, devTaskUuid[0]);
            devUuid[0] = UUID.fromString(devices[0].getUuid());

            devTaskUuid[1] = rootSnapshot.createDevice("dev1", size0);
            devices[1] = helper.getDevice(vvrUuid, devTaskUuid[1]);
            devUuid[1] = UUID.fromString(devices[1].getUuid());

            final String snapshotUuidTask = devices[0].takeSnapshot("snap2");
            final SnapshotMXBean snapshot = helper.getSnapshot(vvrUuid, snapshotUuidTask);
            snapUuidStr = snapshot.getUuid();
            devTaskUuid[2] = snapshot.createDevice("dev2", size2);
            devices[2] = helper.getDevice(vvrUuid, devTaskUuid[2]);
            devUuid[2] = UUID.fromString(devices[2].getUuid());

            // Resize
            devices[0].setSizeNoWait(size1);
            devices[0].setSizeNoWait(size2);
            devices[1].setSizeNoWait(size2);
            devices[1].setSizeNoWait(size1);
            devices[2].setSize(size0);

            // All the tasks should be completed
            Assert.assertEquals(size2, devices[0].getSize());
            Assert.assertEquals(size1, devices[1].getSize());
            Assert.assertEquals(size0, devices[2].getSize());

            Assert.assertEquals(rootSnapshot.getUuid(), rootUuidStr);
            Assert.assertEquals(snapUuidStr, devices[0].getParent());
            Assert.assertEquals(rootUuidStr, devices[1].getParent());
            Assert.assertEquals(snapUuidStr, devices[2].getParent());
        }

        final ObjectName[] deviceName = new ObjectName[3];

        // Save all devices object name
        for (int i = 0; i < 3; i++) {
            deviceName[i] = helper.getDeviceObjectName(vvrUuid, devUuid[i]);
        }
        // Restart VOLD
        restartVold();

        // Wait Device MX Bean registration
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(helper.waitMXBeanRegistration(deviceName[i]));
        }
        // Check elements
        {
            final DeviceMXBean[] devices = new DeviceMXBean[3];
            devices[0] = helper.getDevice(vvrUuid, devUuid[0]);
            devices[1] = helper.getDevice(vvrUuid, devUuid[1]);
            devices[2] = helper.getDevice(vvrUuid, devUuid[2]);

            Assert.assertEquals(size2, devices[0].getSize());
            Assert.assertEquals(size1, devices[1].getSize());
            Assert.assertEquals(size0, devices[2].getSize());

            Assert.assertEquals(snapUuidStr, devices[0].getParent());
            Assert.assertEquals(rootUuidStr, devices[1].getParent());
            Assert.assertEquals(snapUuidStr, devices[2].getParent());
        }
    }

    @Test
    public void testActivateDeactivate() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final long size1 = (long) (2048.0 * 1024.0 * 1024.0);
        final long size2 = (long) (4096.0 * 1024.0 * 1024.0);

        final UUID[] devUuid = new UUID[3];
        // Create elements
        {
            final String[] devTaskUuid = new String[3];
            final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
            final DeviceMXBean[] devices = new DeviceMXBean[3];

            devTaskUuid[0] = rootSnapshot.createDevice("dev0", size0);
            devices[0] = helper.getDevice(vvrUuid, devTaskUuid[0]);
            devUuid[0] = UUID.fromString(devices[0].getUuid());

            devTaskUuid[1] = rootSnapshot.createDevice("dev1", size1);
            devices[1] = helper.getDevice(vvrUuid, devTaskUuid[1]);
            devUuid[1] = UUID.fromString(devices[1].getUuid());

            devices[0].takeSnapshot("snap0");
            devices[1].takeSnapshot();

            final String snapshot2UuidTask = devices[0].takeSnapshot("snap2");
            final SnapshotMXBean snapshot2 = helper.getSnapshot(vvrUuid, snapshot2UuidTask);

            devTaskUuid[2] = snapshot2.createDevice("dev2", size2);
            devices[2] = helper.getDevice(vvrUuid, devTaskUuid[2]);
            devUuid[2] = UUID.fromString(devices[2].getUuid());

            final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

            Assert.assertFalse(devices[0].isActive());
            Assert.assertFalse(devices[1].isActive());
            helper.waitTaskEnd(vvrUuid, devices[1].activateRO(), server);
            Assert.assertTrue(devices[1].isReadOnly());
            Assert.assertTrue(devices[1].isActive());

            Assert.assertFalse(devices[2].isActive());
            helper.waitTaskEnd(vvrUuid, devices[2].activateRW(), server);
            Assert.assertFalse(devices[2].isReadOnly());
            Assert.assertTrue(devices[2].isActive());
        }

        final ObjectName[] deviceName = new ObjectName[3];

        // Save all devices object name
        for (int i = 0; i < 3; i++) {
            deviceName[i] = helper.getDeviceObjectName(vvrUuid, devUuid[i]);
        }
        // Restart VOLD
        restartVold();

        // Wait Device MX Bean registration
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(helper.waitMXBeanRegistration(deviceName[i]));
        }
        // Check elements
        {
            final DeviceMXBean[] devices = new DeviceMXBean[3];
            devices[0] = helper.getDevice(vvrUuid, devUuid[0]);
            devices[1] = helper.getDevice(vvrUuid, devUuid[1]);
            devices[2] = helper.getDevice(vvrUuid, devUuid[2]);

            Assert.assertFalse(devices[0].isActive());

            Assert.assertTrue(devices[1].isReadOnly());
            Assert.assertTrue(devices[1].isActive());

            Assert.assertFalse(devices[2].isReadOnly());
            Assert.assertTrue(devices[2].isActive());
        }
    }

    @Test
    public void testRestartVoldReadTask() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        // Create a device
        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        final String deviceTaskUuid = rootSnapshot.createDevice("name", size0);
        // Wait task is committed
        final String deviceUuid = helper.waitTaskEnd(vvrUuid, deviceTaskUuid.toString(), server);

        final ObjectName vvrObjectName = helper.newVvrObjectName(vvrUuid);

        // check it !
        VvrManagerTestUtils.checkDeviceCreationTask(deviceTaskUuid, deviceUuid,
                JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false).getVvrTask(deviceTaskUuid));

        // Restart the Vold
        restartVold();

        // wait vvr mx bean creation
        Assert.assertTrue(helper.waitMXBeanRegistration(vvrObjectName));

        // check the task
        VvrManagerTestUtils.checkDeviceCreationTask(deviceTaskUuid, deviceUuid,
                JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false).getVvrTask(deviceTaskUuid));

    }

    @Test
    public void testJmxDtxManagerAndLocalNode() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        final DtxManagerMXBean dtxManager = JMX.newMXBeanProxy(server, helper.newDtxManagerObjectName(),
                DtxManagerMXBean.class, false);

        final DtxLocalNodeMXBean dtxLocalNode = JMX.newMXBeanProxy(server, helper.newDtxLocalNodeObjectName(),
                DtxLocalNodeMXBean.class, false);

        // Check Resource Manager
        VvrManagerTestUtils.checkResourceManagersAfterVvrCreation(dtxManager, vvrUuid, helper.VOLD_OWNER_UUID_TEST);

        // Check request Queue
        VvrManagerTestUtils.checkRequestQueueEmpty(dtxManager);

        // Check task list
        VvrManagerTestUtils.checkDtxTasksListAfterVvrCreation(dtxManager, vvrUuid, helper.VOLD_OWNER_UUID_TEST, 0);

        // Check dtx node
        // Local node: stand alone and dynamic port (no incoming connection)
        final InetSocketAddress localPeer = new InetSocketAddress(STANDALONE_HOST_ADDR, 0);
        VvrManagerTestUtils.checkDtxLocalNode(dtxLocalNode, localPeer, 0);

        // Create a device
        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        final String deviceTaskUuidStr = rootSnapshot.createDevice("name", size0);
        // Wait task is committed
        helper.waitTaskEnd(vvrUuid, deviceTaskUuidStr, server);

        VvrManagerTestUtils.checkResourceManagersAfterDeviceCreation(dtxManager, vvrUuid, helper.VOLD_OWNER_UUID_TEST);

        // No pending request
        VvrManagerTestUtils.checkRequestQueueEmpty(dtxManager);

        // Check tasks List
        VvrManagerTestUtils.checkDtxTasksListAfterDeviceCreation(dtxManager, vvrUuid, deviceTaskUuidStr, 0);

    }
}
