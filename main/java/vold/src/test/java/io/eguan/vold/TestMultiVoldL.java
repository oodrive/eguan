package io.eguan.vold;

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

import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.DummyMBeanServer;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VoldTestHelper;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrManagementException;
import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrManagerTestUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * All multi-vold unit tests are composed of two remote VOLDs.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public class TestMultiVoldL extends TestMultiVoldAbstract {

    private final static String DEVICE_NAME_TEST = "device name test";
    private VvrManagerMXBean vvrManager1;
    private DummyMBeanServer server1;
    private DummyMBeanServer server2;
    private VoldTestHelper voldTestHelper1;
    private VoldTestHelper voldTestHelper2;

    @BeforeClass
    public static void init() throws Exception {
        // Two nodes, vvr started automatically
        setUpVolds(2, 2, true);
    }

    @Before
    public void initialize() throws Exception {

        server1 = getDummyMBeanServer(0);
        Assert.assertNotNull(server1);

        server2 = getDummyMBeanServer(1);
        Assert.assertNotNull(server2);

        voldTestHelper1 = getVoldTestHelper(0);
        Assert.assertNotNull(voldTestHelper1);

        voldTestHelper2 = getVoldTestHelper(1);
        Assert.assertNotNull(voldTestHelper2);

        vvrManager1 = VvrManagerTestUtils.getVvrManagerMXBean(server1, VOLD_OWNER);
        Assert.assertNotNull(vvrManager1);

    }

    @Test
    public void testCreateAndSetVvr() throws InterruptedException, VvrManagementException, MalformedObjectNameException {

        // Create a VVR into the first VVR manager
        createVvrStarted();

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server1.getNbMXBeans());
        // Check if that VVR is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server2.getNbMXBeans());

        final ObjectName on1 = VvrManagerTestUtils.getVvrObjectName(VOLD_OWNER, vvrUuid);
        final VvrMXBean vvr1 = (VvrMXBean) server1.waitMXBean(on1);

        final ObjectName on2 = VvrManagerTestUtils.getVvrObjectName(VOLD_OWNER, vvrUuid);
        final VvrMXBean vvr2 = (VvrMXBean) server2.waitMXBean(on2);

        compareVvrs(vvr1, vvr2);

        vvr1.setDescription("new desc1");
        vvr2.setName("new name1");

        // Wait some time for the new name to be set
        int i = 0;
        while ("name".equals(vvr1.getName()) && i < 30) {
            Thread.sleep(1000);
            i++;
        }

        compareVvrs(vvr1, vvr2);
    }

    @Test
    public void testCreateAndSetDevice() throws InterruptedException, VvrManagementException,
            MalformedObjectNameException, FileNotFoundException, IOException {

        // Create a Device into the first VVR manager
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(d1.getUuid()));
        final DeviceMXBean d2 = (DeviceMXBean) server2.getMXBean(on2);

        compareDevices(d1, d2);

        d1.setDescription("new desc1");
        d1.setIqn("iqn.2000-06.com.oodrive:test1");
        d1.setIscsiAlias("new iscsi alias");
        d1.setName("new name");
        d1.setSize(409600);
        Thread.sleep(1000);

        compareDevices(d1, d2);
    }

    @Test
    public void testActivationRWDevice() throws VvrManagementException, InterruptedException,
            MalformedObjectNameException {

        // Create a Device into the first VVR manager
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(d1.getUuid()));
        final DeviceMXBean d2 = (DeviceMXBean) server2.getMXBean(on2);

        compareDevices(d1, d2);

        // Activate the first device in RO mode
        voldTestHelper1.waitTaskEnd(vvrUuid, d1.activateRO(), server1);
        Assert.assertTrue(d1.isActive());

        // It cannot be activated in RW mode into the second peer
        try {
            d2.activateRW();
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IllegalStateException e) {
            // OK
        }

        // It can be activated in RO mode into the second peer
        voldTestHelper2.waitTaskEnd(vvrUuid, d2.activateRO(), server2);
        Assert.assertTrue(d2.isActive());
    }

    @Test
    public void testActivationRODevice() throws VvrManagementException, InterruptedException,
            MalformedObjectNameException {

        // Create a Device into the first VVR manager
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        Thread.sleep(1000);
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(d1.getUuid()));
        final DeviceMXBean d2 = (DeviceMXBean) server2.getMXBean(on2);

        compareDevices(d1, d2);

        // Activate the first device in RW mode
        voldTestHelper1.waitTaskEnd(vvrUuid, d1.activateRW(), server1);
        Assert.assertTrue(d1.isActive());

        // It cannot be activated in RO mode into the second peer
        try {
            d2.activateRO();
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IllegalStateException e) {
            // OK
        }
        // It cannot be activated in RW mode into the second peer
        try {
            d2.activateRW();
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void testDeleteDevice() throws VvrManagementException, InterruptedException, MalformedObjectNameException {

        // Create a Device into the first VVR manager
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

        // Delete that device
        Assert.assertFalse(d1.isActive());
        voldTestHelper1.waitTaskEnd(vvrUuid, d1.delete(), server1);
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server2.getNbMXBeans());
    }

    @Test
    public void testDeleteDeviceActivated() throws VvrManagementException, InterruptedException,
            MalformedObjectNameException {

        // Create a Device into the first VVR manager
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

        // Activate the device into the second peer
        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(d1.getUuid()));
        final DeviceMXBean d2 = (DeviceMXBean) server2.getMXBean(on2);
        voldTestHelper2.waitTaskEnd(vvrUuid, d2.activateRO(), server2);
        Assert.assertTrue(d2.isActive());

        // Delete the device from the first peer, should raise an exception
        try {
            voldTestHelper1.waitTaskEnd(vvrUuid, d1.delete(), server1);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void testSetSizeDevice() throws InterruptedException, VvrManagementException, MalformedObjectNameException {

        // Create a Device
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean device = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);

        // Check the second peer
        final ObjectName on2 = VvrManagerTestUtils.getDeviceObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(device.getUuid()));
        final DeviceMXBean d2 = (DeviceMXBean) server2.getMXBean(on2);

        Assert.assertEquals(409600, device.getSize());
        Assert.assertEquals(409600, d2.getSize());

        device.setSize(4096);
        Assert.assertEquals(4096, device.getSize());
        Assert.assertEquals(4096, d2.getSize());

        device.setSize(40960);
        Assert.assertEquals(40960, device.getSize());
        Assert.assertEquals(40960, d2.getSize());

        voldTestHelper1.waitTaskEnd(vvrUuid, device.activateRO(), server1);
        Assert.assertTrue(device.isActive());

        boolean exceptionOnSetSize = false;
        try {
            device.setSize(4096);
        }
        catch (final IllegalStateException e) {
            exceptionOnSetSize = true;
        }
        Assert.assertTrue(exceptionOnSetSize);

        voldTestHelper1.waitTaskEnd(vvrUuid, device.deActivate(), server1);
        Assert.assertFalse(device.isActive());

        voldTestHelper1.waitTaskEnd(vvrUuid, device.activateRW(), server1);
        exceptionOnSetSize = false;
        try {
            device.setSize(4096);
        }
        catch (final IllegalStateException e) {
            exceptionOnSetSize = true;
        }
        Assert.assertFalse(exceptionOnSetSize);

        Assert.assertEquals(4096, device.getSize());
        Assert.assertEquals(4096, d2.getSize());
    }

    @Test
    public void testCreateSnapshot() throws InterruptedException, VvrManagementException, MalformedObjectNameException {

        // Create a Device
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean device = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);
        // Take a snapshot into the first VVR manager
        voldTestHelper1.waitTaskEnd(vvrUuid, device.takeSnapshot("name"), server1);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server1.getNbMXBeans());
        // Check if that snapshot is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server2.getNbMXBeans());

        voldTestHelper1.waitTaskEnd(vvrUuid, device.activateRO(), server1);
        Assert.assertTrue(device.isActive());

        boolean exceptionOnTakeSnap = false;
        try {
            voldTestHelper1.waitTaskEnd(vvrUuid, device.takeSnapshot("name2"), server1);
        }
        catch (final IllegalStateException e) {
            exceptionOnTakeSnap = true;
        }
        Assert.assertTrue(exceptionOnTakeSnap);

        voldTestHelper1.waitTaskEnd(vvrUuid, device.deActivate(), server1);
        Assert.assertFalse(device.isActive());

        voldTestHelper1.waitTaskEnd(vvrUuid, device.activateRW(), server1);
        Assert.assertTrue(device.isActive());

        exceptionOnTakeSnap = false;
        try {
            voldTestHelper1.waitTaskEnd(vvrUuid, device.takeSnapshot("name 3"), server1);
        }
        catch (final IllegalStateException e) {
            exceptionOnTakeSnap = true;
        }
        Assert.assertFalse(exceptionOnTakeSnap);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 5, server1.getNbMXBeans());
        // Check if that snapshot is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 5, server2.getNbMXBeans());
    }

    @Test
    public void testDeleteSnapshot() throws InterruptedException, VvrManagementException, MalformedObjectNameException {

        // Create a Device
        final SnapshotMXBean snapshotRoot = createVvrStarted();

        final DeviceMXBean device = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);
        // Take a snapshot into the first VVR manager
        final String uuidStrTaskSnap = device.takeSnapshot("name");
        final SnapshotMXBean snapshot1 = voldTestHelper1.getSnapshot(server1, vvrUuid, uuidStrTaskSnap);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server1.getNbMXBeans());
        // Check if that snapshot is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server2.getNbMXBeans());

        final ObjectName os1 = VvrManagerTestUtils.getSnapshotObjectName(VOLD_OWNER, vvrUuid,
                UUID.fromString(snapshot1.getUuid()));
        final SnapshotMXBean snapshot2 = (SnapshotMXBean) server2.getMXBean(os1);
        voldTestHelper2.waitTaskEnd(vvrUuid, snapshot2.delete(), server2);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that snapshot is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());
    }

    @Test
    public void testMultiVoldReadTask() throws Exception {

        // Create a VVR into the first VVR manager
        final String vvrTaskIdStr = vvrManager1.createVvrNoWait("name", "description");
        final String vvrUuidStr = voldTestHelper1.waitVvrManagerTaskEnd(vvrTaskIdStr, server1);
        vvrUuid = UUID.fromString(vvrUuidStr);

        // check the task on node 1
        VvrManagerTestUtils.checkVvrCreationTask(vvrTaskIdStr, vvrUuidStr, ((VvrManagerMXBean) server1
                .getMXBean(voldTestHelper1.newVvrManagerObjectName())).getVvrManagerTask(vvrTaskIdStr));

        // check the task on node 2
        VvrManagerTestUtils.checkVvrCreationTask(vvrTaskIdStr, vvrUuidStr, ((VvrManagerMXBean) server2
                .getMXBean(voldTestHelper2.newVvrManagerObjectName())).getVvrManagerTask(vvrTaskIdStr));

        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);

        final SnapshotMXBean snapshotRoot = VvrManagerTestUtils.getSnapshotRoot(server1, voldTestHelper1, vvrUuid);
        rootUuid = UUID.fromString(snapshotRoot.getUuid());

        // Create a device
        final String deviceTaskUuidStr = snapshotRoot.createDevice("name", size0);
        // Wait task is committed
        final String deviceUuidStr = voldTestHelper1.waitTaskEnd(vvrUuid, deviceTaskUuidStr, server1);

        // check it on node 1
        VvrManagerTestUtils.checkDeviceCreationTask(deviceTaskUuidStr, deviceUuidStr, ((VvrMXBean) server1
                .getMXBean(voldTestHelper1.newVvrObjectName(vvrUuid))).getVvrTask(deviceTaskUuidStr));

        // check it on node 2
        VvrManagerTestUtils.checkDeviceCreationTask(deviceTaskUuidStr, deviceUuidStr, ((VvrMXBean) server2
                .getMXBean(voldTestHelper2.newVvrObjectName(vvrUuid))).getVvrTask(deviceTaskUuidStr));

    }

    /**
     * Compare the attributes of two devices.
     * 
     * @param d1
     * @param d2
     */
    private final void compareDevices(final DeviceMXBean d1, final DeviceMXBean d2) {
        Assert.assertEquals(d1.getDescription(), d2.getDescription());
        Assert.assertEquals(d1.getIqn(), d2.getIqn());
        Assert.assertEquals(d1.getIscsiAlias(), d2.getIscsiAlias());
        Assert.assertEquals(d1.getName(), d2.getName());
        Assert.assertEquals(d1.getParent(), d2.getParent());
        Assert.assertEquals(d1.getSize(), d2.getSize());
        Assert.assertEquals(d1.getUuid(), d2.getUuid());
        Assert.assertEquals(d1.getIscsiBlockSize(), d2.getIscsiBlockSize());
    }

    /**
     * Compare the attributes of two VVRs.
     * 
     * @param vvr1
     * @param vvr2
     */
    private final void compareVvrs(final VvrMXBean vvr1, final VvrMXBean vvr2) {
        Assert.assertEquals(vvr1.getName(), vvr2.getName());
        Assert.assertEquals(vvr1.getDescription(), vvr2.getDescription());
        Assert.assertEquals(vvr1.getOwnerUuid(), vvr2.getOwnerUuid());
        Assert.assertEquals(vvr1.getUuid(), vvr2.getUuid());
    }

}
