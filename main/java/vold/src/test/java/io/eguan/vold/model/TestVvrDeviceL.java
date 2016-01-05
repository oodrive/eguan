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

import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;

import java.io.IOException;
import java.util.UUID;

import javax.management.JMException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVvrDeviceL extends TestVvrManagerAbstract {

    static final String DEVICE_NAME_TEST = "device test name";
    static final String DEVICE_DESCRIPTION_TEST = "device test description";
    static final String DEVICE_ISCSI_ALIAS_TEST = "iscsi alias";

    private VvrMXBean vvr;
    private SnapshotMXBean snapshotRoot;
    private DeviceMXBean device;

    public TestVvrDeviceL(final Boolean vvrStarted) {
        super(vvrStarted);
    }

    @Before
    public void initTestVvrDevice() throws Exception {
        vvr = createVvr("", "");
        if (!voldTestHelper.isVvrStarted()) {
            vvr.start();
        }

        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        snapshotRoot = VvrManagerTestUtils.getSnapshotRoot(dummyMbeanServer, voldTestHelper, vvrUuid);
        device = VvrManagerTestUtils.createDevice(dummyMbeanServer, voldTestHelper, snapshotRoot, vvrUuid,
                DEVICE_NAME_TEST, 409600);
    }

    @After
    public void finiTestVvrDevice() throws Exception {
        voldTestHelper.deleteVvr(dummyMbeanServer, UUID.fromString(vvr.getUuid()));
        device = null;
        snapshotRoot = null;
        vvr = null;
    }

    @Test
    public void testCreationOfDevice() {
        device.setName("name");
        device.setDescription("creation description");
        device.setIscsiAlias("iscsi alias");
        device.setIqn("iqn1");
        device.setSize(40960);
        device.setIscsiBlockSize(512);

        checkDevice(device, device.getUuid(), "name", "creation description", snapshotRoot.getUuid(), "iscsi alias",
                "iqn1", 40960, 512);
    }

    @Test
    public void testDeletionOfDevice() {
        device.setName("name");
        device.setDescription("creation description");
        device.setIscsiAlias("iscsi alias");
        device.setIqn("iqn1");
        device.setSize(40960);
        device.setIscsiBlockSize(512);

        checkDevice(device, device.getUuid(), "name", "creation description", snapshotRoot.getUuid(), "iscsi alias",
                "iqn1", 40960, 512);

        final String taskId = device.delete();
        voldTestHelper.waitTaskEnd(UUID.fromString(vvr.getUuid()), taskId, dummyMbeanServer);
        // add mx beans Vvr and Snapshot root
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, dummyMbeanServer.getNbMXBeans());
    }

    @Test
    public void testCreationOfSnapshot() {
        // name
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        final String snapshotTaskUuid = device.takeSnapshot("new snap");
        final SnapshotMXBean snap = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, snapshotTaskUuid);
        // 6 = dtxManager, vvrManager, Vvr, Snapshot root, Device and new snapshot
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, dummyMbeanServer.getNbMXBeans());
        Assert.assertEquals(snapshotRoot.getUuid(), snap.getParent());
        Assert.assertEquals("new snap", snap.getName());

        // name, description
        final String snap1Task = device.takeSnapshot("snap1", "description1");
        final SnapshotMXBean snap1 = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, snap1Task);
        Assert.assertEquals("snap1", snap1.getName());
        Assert.assertEquals("description1", snap1.getDescription());

        // name, uuid
        final String snapuuid2 = UUID.randomUUID().toString();
        final String snap2Task = device.takeSnapshotUuid("snap2", snapuuid2);
        final SnapshotMXBean snap2 = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, snap2Task);
        Assert.assertEquals("snap2", snap2.getName());
        Assert.assertEquals(snapuuid2, snap2.getUuid());

        // name, description, uuid
        final String snapuuid3 = UUID.randomUUID().toString();
        final String snap3Task = device.takeSnapshotUuid("snap3", "description3", snapuuid3);
        final SnapshotMXBean snap3 = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, snap3Task);
        Assert.assertEquals("snap3", snap3.getName());
        Assert.assertEquals("description3", snap3.getDescription());
        Assert.assertEquals(snapuuid3, snap3.getUuid());
    }

    @Test
    public void testCloneOfDevice() {
        // name
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        final String devTask1 = device.clone("clone1");
        final DeviceMXBean clone1 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, devTask1);
        Assert.assertEquals(snapshotRoot.getUuid(), clone1.getParent());
        Assert.assertEquals("clone1", clone1.getName());

        // name, description
        final String devTask2 = device.clone("clone2", "description2");
        final DeviceMXBean clone2 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, devTask2);
        Assert.assertEquals(snapshotRoot.getUuid(), clone2.getParent());
        Assert.assertEquals("clone2", clone2.getName());
        Assert.assertEquals("description2", clone2.getDescription());

        // name, uuid
        final String devuuid3 = UUID.randomUUID().toString();
        final String devTask3 = device.cloneUuid("clone3", devuuid3);
        final DeviceMXBean clone3 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, devTask3);
        Assert.assertEquals(snapshotRoot.getUuid(), clone3.getParent());
        Assert.assertEquals("clone3", clone3.getName());
        Assert.assertEquals(devuuid3, clone3.getUuid());

        // name, description, uuid
        final String devuuid4 = UUID.randomUUID().toString();
        final String devTask4 = device.cloneUuid("clone4", "description4", devuuid4);
        final DeviceMXBean clone4 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, devTask4);
        Assert.assertEquals(snapshotRoot.getUuid(), clone4.getParent());
        Assert.assertEquals("clone4", clone4.getName());
        Assert.assertEquals("description4", clone4.getDescription());
        Assert.assertEquals(devuuid4, clone4.getUuid());

        // clone of clone
        final String devTask5 = clone4.clone("clone5", "description5");
        final DeviceMXBean clone5 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, devTask5);
        Assert.assertEquals(snapshotRoot.getUuid(), clone4.getParent());
        Assert.assertEquals("clone5", clone5.getName());
        Assert.assertEquals("description5", clone5.getDescription());
    }

    @Test
    public void testActivationDevice() {

        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        boolean exceptionOnActivateRO = false, exceptionOnActivateRW = false;

        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);
        Assert.assertTrue(device.isActive() && device.isReadOnly());

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        Assert.assertFalse(device.isActive());

        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);
        Assert.assertTrue(device.isActive() && !device.isReadOnly());

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);

        // activate two times the device, RO
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);
        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnActivateRO = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }
        Assert.assertTrue(exceptionOnActivateRO);

        // activate two time the device, RW
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);
        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnActivateRW = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }
        Assert.assertTrue(exceptionOnActivateRW);

        // activate in RO and RW
        exceptionOnActivateRW = false;
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);
        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnActivateRW = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }
        Assert.assertTrue(exceptionOnActivateRW);

        // activate in RW and RO
        exceptionOnActivateRO = false;
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);
        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnActivateRO = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }
        Assert.assertTrue(exceptionOnActivateRO);
    }

    @Test
    public void testLoadDevice() throws JMException, IOException, InterruptedException {
        final UUID deviceUuid = UUID.fromString(device.getUuid());
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        device.setDescription("creation description");
        device.setIscsiAlias("iscsi alias");
        device.setIqn("iqn1");
        device.setIscsiBlockSize(512);
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, dummyMbeanServer.getNbMXBeans());

        vvr.stop();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 1, dummyMbeanServer.getNbMXBeans());

        vvrManager.fini();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT - 1, dummyMbeanServer.getNbMXBeans());

        VvrManagerHelper.initDtxManagement(vvrManager, dtxManager);
        Assert.assertTrue(VvrManagerTestUtils
                .waitMXBeanNumber(dummyMbeanServer, VoldTestHelper.MXBEANS_NUMBER_INIT + 1));

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 1, dummyMbeanServer.getNbMXBeans());

        vvr = (VvrMXBean) dummyMbeanServer.getMXBean(VvrManagerTestUtils.getVvrObjectName(
                voldTestHelper.VOLD_OWNER_UUID_TEST, vvrUuid));
        vvr.start();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, dummyMbeanServer.getNbMXBeans());

        device = (DeviceMXBean) dummyMbeanServer.getMXBean(VvrObjectNameFactory.newDeviceObjectName(deviceUuid,
                vvrUuid, deviceUuid));

        checkDevice(device, deviceUuid.toString(), DEVICE_NAME_TEST, "creation description", snapshotRoot.getUuid(),
                "iscsi alias", "iqn1", 409600, 512);
    }

    @Test
    public void testSetIqn() {

        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        boolean exceptionOnRO = false, exceptionOnRW = false;

        device.setIqn("iqn.2000-06.com.oodrive:test1");
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);

        try {
            device.setIqn("iqn.2000-06.com.oodrive:test2");
        }
        catch (final IllegalStateException e) {
            exceptionOnRO = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);

        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.setIqnNoWait("iqn.2000-06.com.oodrive:test2"),
                    dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnRW = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        Assert.assertTrue(exceptionOnRO && exceptionOnRW);
    }

    @Test
    public void testSetIscsiAlias() {
        boolean exceptionOnRO = false, exceptionOnRW = false;
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());

        device.setIscsiAlias("alias1");
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);

        try {
            device.setIscsiAlias("alias2");
        }
        catch (final IllegalStateException e) {
            exceptionOnRO = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);

        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.setIscsiAliasNoWait("alias2"), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnRW = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        Assert.assertTrue(exceptionOnRO && exceptionOnRW);
    }

    @Test
    public void testSetIscsiBlockSize() {
        boolean exceptionOnRO = false, exceptionOnRW = false;
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());

        device.setIscsiBlockSize(512);
        Assert.assertEquals(512, device.getIscsiBlockSize());

        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRO(), dummyMbeanServer);

        try {
            device.setIscsiBlockSize(1024);
        }
        catch (final IllegalStateException e) {
            exceptionOnRO = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        voldTestHelper.waitTaskEnd(vvrUuid, device.activateRW(), dummyMbeanServer);

        try {
            voldTestHelper.waitTaskEnd(vvrUuid, device.setIscsiBlockSizeNoWait(1024), dummyMbeanServer);
        }
        catch (final IllegalStateException e) {
            exceptionOnRW = true;
        }
        finally {
            voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        }

        Assert.assertTrue(exceptionOnRO && exceptionOnRW);

        voldTestHelper.waitTaskEnd(vvrUuid, device.deActivate(), dummyMbeanServer);
        device.setIscsiBlockSize(0);
        Assert.assertEquals(0, device.getIscsiBlockSize());
        device.setIscsiBlockSize(-154);
        Assert.assertEquals(0, device.getIscsiBlockSize());
    }

    private void checkDevice(final DeviceMXBean device, final String uuid, final String name, final String description,
            final String parent, final String iscsiAlias, final String iqn, final long size, final long blockSize) {

        Assert.assertEquals(name, device.getName());
        Assert.assertEquals(uuid, device.getUuid());
        Assert.assertEquals(description, device.getDescription());
        Assert.assertEquals(parent, device.getParent());
        Assert.assertEquals(iscsiAlias, device.getIscsiAlias());
        Assert.assertEquals(iqn, device.getIqn());
        Assert.assertEquals(size, device.getSize());
        Assert.assertEquals(blockSize, device.getIscsiBlockSize());
    }
}
