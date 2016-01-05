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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.JMException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestVvrSnapshot extends TestVvrManagerAbstract {

    static final String SNAPSHOT_NAME_TEST = "snapshot test name";
    static final String SNAPSHOT_DESCRIPTION_TEST = "snapshot test description";

    private static final String DEVICE_UUID = "d56e5114-5d91-11e3-a9e6-180373e17308";

    private VvrMXBean vvr;
    private SnapshotMXBean snapshotRoot;

    public TestVvrSnapshot(final Boolean vvrStarted) {
        super(vvrStarted);
    }

    @Before
    public void createVvrAndSnapshotRoot() throws Exception {
        vvr = createVvr("", "");
        if (!voldTestHelper.isVvrStarted()) {
            vvr.start();
        }

        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        final Set<SnapshotMXBean> snapshots = voldTestHelper.getSnapshots(dummyMbeanServer, vvrUuid);
        Assert.assertEquals(1, snapshots.size());
        snapshotRoot = snapshots.iterator().next();
    }

    @After
    public void finiTestVvr() throws Exception {
        voldTestHelper.deleteVvr(dummyMbeanServer, UUID.fromString(vvr.getUuid()));
        vvr = null;
        snapshotRoot = null;
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateDeviceFail() {

        snapshotRoot.setName(SNAPSHOT_NAME_TEST);
        snapshotRoot.setDescription(SNAPSHOT_DESCRIPTION_TEST);

        {
            snapshotRoot.createDevice("name device 1");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDeviceNullName1() {
        snapshotRoot.createDevice(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDeviceNullName2() {
        snapshotRoot.createDevice(null, 15 * 4096);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateDeviceNullUuid1() {
        snapshotRoot.createDeviceUuid("name", null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateDeviceNullUuid2() {
        snapshotRoot.createDeviceUuid("name", null, 15 * 4096);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDeviceUuidInvalid1() {
        snapshotRoot.createDeviceUuid("name", "not-a-uuid");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDeviceUuidInvalid2() {
        snapshotRoot.createDeviceUuid("name", "not-a-uuid", 15 * 4096);
    }

    @Test
    public void testCreateDeviceUuid() {
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());

        // name, uuid, size
        String taskId = snapshotRoot.createDeviceUuid("name1", DEVICE_UUID, 15 * 4096);
        final DeviceMXBean device1 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name1", device1.getName());
        Assert.assertEquals(DEVICE_UUID, device1.getUuid());
        Assert.assertEquals(15 * 4096, device1.getSize());

        // name, description, uuid, size
        String uuid = UUID.randomUUID().toString();
        taskId = snapshotRoot.createDeviceUuid("name2", "description2", uuid, 15 * 4096);
        final DeviceMXBean device2 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name2", device2.getName());
        Assert.assertEquals("description2", device2.getDescription());
        Assert.assertEquals(uuid, device2.getUuid());
        Assert.assertEquals(15 * 4096, device2.getSize());

        // Take a snapshot
        taskId = device2.takeSnapshot();
        final SnapshotMXBean snapshot = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, taskId);

        // name, uuid
        uuid = UUID.randomUUID().toString();
        taskId = snapshot.createDeviceUuid("name3", uuid);
        final DeviceMXBean device3 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name3", device3.getName());
        Assert.assertEquals(uuid, device3.getUuid());
        Assert.assertEquals(snapshot.getSize(), device3.getSize());

        // name, description, uuid
        uuid = UUID.randomUUID().toString();
        taskId = snapshot.createDeviceUuid("name4", "description4", uuid);
        final DeviceMXBean device4 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name4", device4.getName());
        Assert.assertEquals("description4", device4.getDescription());
        Assert.assertEquals(uuid, device4.getUuid());
        Assert.assertEquals(snapshot.getSize(), device4.getSize());

    }

    @Test
    public void testCreateDevice() {

        final UUID vvrUuid = UUID.fromString(vvr.getUuid());

        // name, size
        String taskId = snapshotRoot.createDevice("name1", 15 * 4096);
        final DeviceMXBean device1 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name1", device1.getName());
        Assert.assertEquals(15 * 4096, device1.getSize());

        // name, description, size
        taskId = snapshotRoot.createDevice("name2", "description2", 15 * 4096);
        final DeviceMXBean device2 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name2", device2.getName());
        Assert.assertEquals("description2", device2.getDescription());
        Assert.assertEquals(15 * 4096, device2.getSize());

        // take snapshot
        taskId = device2.takeSnapshot();
        final SnapshotMXBean snapshot = voldTestHelper.getSnapshot(dummyMbeanServer, vvrUuid, taskId);

        // name
        taskId = snapshot.createDevice("name3");
        final DeviceMXBean device3 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name3", device3.getName());
        Assert.assertEquals(snapshot.getSize(), device3.getSize());

        // name, description
        taskId = snapshot.createDevice("name4", "description4");
        final DeviceMXBean device4 = voldTestHelper.getDevice(dummyMbeanServer, vvrUuid, taskId);
        Assert.assertEquals("name4", device4.getName());
        Assert.assertEquals("description4", device4.getDescription());
        Assert.assertEquals(snapshot.getSize(), device4.getSize());

    }

    @Test
    public void testCreateDeviceCheckHierarchy() {

        final List<String> children = new ArrayList<String>(2);
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        snapshotRoot.setName(SNAPSHOT_NAME_TEST);
        snapshotRoot.setDescription(SNAPSHOT_DESCRIPTION_TEST);

        {
            final String taskId = snapshotRoot.createDevice("name device 1", 4096000);
            children.add(voldTestHelper.waitTaskEnd(vvrUuid, taskId, dummyMbeanServer));
            Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, dummyMbeanServer.getNbMXBeans());
        }

        {
            final String taskId = snapshotRoot.createDevice("name device 2", "description 2", 4096000);
            children.add(voldTestHelper.waitTaskEnd(vvrUuid, taskId, dummyMbeanServer));
            Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, dummyMbeanServer.getNbMXBeans());
        }

        // Transactions on devices are done: those on root snapshot should be done too
        checkSnapshot(snapshotRoot, SNAPSHOT_NAME_TEST, SNAPSHOT_DESCRIPTION_TEST, 0, children);
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteSnapshotRoot() throws JMException {
        final String taskId = snapshotRoot.delete();
        final UUID vvrUuid = UUID.fromString(vvr.getUuid());
        voldTestHelper.waitTaskEnd(vvrUuid, taskId, dummyMbeanServer);
    }

    private void checkSnapshot(final SnapshotMXBean snapshot, final String name, final String description,
            final long size, final List<String> children) {

        Assert.assertEquals(name, snapshot.getName());
        Assert.assertEquals(description, snapshot.getDescription());
        Assert.assertEquals(size, snapshot.getSize());

        final Set<String> setChildrenExpected = new HashSet<String>(children);
        final Set<String> setChildrenActuals = new HashSet<String>(Arrays.asList(snapshot.getChildrenSnapshots()));
        setChildrenActuals.addAll(Arrays.asList(snapshot.getChildrenDevices()));

        Assert.assertEquals(0, Sets.difference(setChildrenActuals, setChildrenExpected).size());
    }
}
