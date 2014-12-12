package com.oodrive.nuage.vold;

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

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import javax.management.MalformedObjectNameException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.model.DummyMBeanServer;
import com.oodrive.nuage.vold.model.SnapshotMXBean;
import com.oodrive.nuage.vold.model.VoldTestHelper;
import com.oodrive.nuage.vold.model.VvrManagerMXBean;
import com.oodrive.nuage.vold.model.VvrManagerTask;
import com.oodrive.nuage.vold.model.VvrManagerTestUtils;
import com.oodrive.nuage.vold.model.VvrTask;

public class TestMultiVoldHistoryL extends TestMultiVoldAbstract {

    private final long size1 = 4096;
    private final long size2 = 40960;
    private final long size3 = 409600;

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
    public void initialize() throws MalformedObjectNameException {

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
    public void testHistory() throws Exception {

        SnapshotMXBean rootSnapshot1 = createVvrStarted();

        // Create the history into the first peer
        // New device11
        final DeviceMXBean device11 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot1,
                vvrUuid, "D1", size1);
        // New snapshot11
        final SnapshotMXBean snapshot11 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device11, "snap1",
                VOLD_OWNER, vvrUuid);
        // New device12
        final DeviceMXBean device12 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, snapshot11, vvrUuid,
                "d2", size2);
        // New snapshot12 of device12
        final SnapshotMXBean snapshot12 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device12, "snap2",
                VOLD_OWNER, vvrUuid);
        // New snapshot13 of device12
        final SnapshotMXBean snapshot13 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device12, "snap3",
                VOLD_OWNER, vvrUuid);
        device12.setSize(size3);

        SnapshotMXBean rootSnapshot2 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(rootSnapshot1.getUuid()));

        // Check those operations
        checkTree(server1, rootSnapshot1, vvrUuid, UUID.fromString(device11.getUuid()),
                UUID.fromString(device12.getUuid()), UUID.fromString(snapshot11.getUuid()),
                UUID.fromString(snapshot12.getUuid()), UUID.fromString(snapshot13.getUuid()));
        checkTree(server2, rootSnapshot2, vvrUuid, UUID.fromString(device11.getUuid()),
                UUID.fromString(device12.getUuid()), UUID.fromString(snapshot11.getUuid()),
                UUID.fromString(snapshot12.getUuid()), UUID.fromString(snapshot13.getUuid()));

        // Check Tasks
        checkVvrManagerTasks(VvrManagerTestUtils.getVvrManagerMXBean(server1, VOLD_OWNER).getVvrManagerTasks(),
                VvrManagerTestUtils.getVvrManagerMXBean(server2, VOLD_OWNER).getVvrManagerTasks());

        checkVvrTasks(VvrManagerTestUtils.getVvrMXBean(server1, VOLD_OWNER, vvrUuid).getVvrTasks(), VvrManagerTestUtils
                .getVvrMXBean(server2, VOLD_OWNER, vvrUuid).getVvrTasks());

        final int nbMXBeans1 = server1.getNbMXBeans();
        final int nbMXBeans2 = server2.getNbMXBeans();

        // Restart VVR managers
        restartNodes();
        initialize(); // reload local variables

        assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server1, nbMXBeans1));
        assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server2, nbMXBeans2));

        // Refresh root snapshots
        rootSnapshot1 = VvrManagerTestUtils.getSnapshotMXBean(server1, VOLD_OWNER, vvrUuid,
                UUID.fromString(rootSnapshot1.getUuid()));
        rootSnapshot2 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(rootSnapshot2.getUuid()));

        // Check peers after restart
        checkTree(server1, rootSnapshot1, vvrUuid, UUID.fromString(device11.getUuid()),
                UUID.fromString(device12.getUuid()), UUID.fromString(snapshot11.getUuid()),
                UUID.fromString(snapshot12.getUuid()), UUID.fromString(snapshot13.getUuid()));

        checkTree(server2, rootSnapshot2, vvrUuid, UUID.fromString(device11.getUuid()),
                UUID.fromString(device12.getUuid()), UUID.fromString(snapshot11.getUuid()),
                UUID.fromString(snapshot12.getUuid()), UUID.fromString(snapshot13.getUuid()));

        // Check tasks after restart
        checkVvrManagerTasks(VvrManagerTestUtils.getVvrManagerMXBean(server1, VOLD_OWNER).getVvrManagerTasks(),
                VvrManagerTestUtils.getVvrManagerMXBean(server2, VOLD_OWNER).getVvrManagerTasks());

        checkVvrTasks(VvrManagerTestUtils.getVvrMXBean(server1, VOLD_OWNER, vvrUuid).getVvrTasks(), VvrManagerTestUtils
                .getVvrMXBean(server2, VOLD_OWNER, vvrUuid).getVvrTasks());

        // New device13 after restarting VVR manager
        final DeviceMXBean device13 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot1,
                vvrUuid, "d3", size2);
        Assert.assertEquals(rootSnapshot1.getUuid(), device13.getParent());
    }

    // Check the tree generated by the method above
    private final void checkTree(final DummyMBeanServer server, final SnapshotMXBean rootSnapshot, final UUID vvrUuid,
            final UUID device11, final UUID device12, final UUID snapshot11, final UUID snapshot12,
            final UUID snapshot13) throws MalformedObjectNameException {

        final DeviceMXBean device21 = VvrManagerTestUtils.getDeviceMXBean(server, VOLD_OWNER, vvrUuid, device11);
        final SnapshotMXBean snapshot21 = VvrManagerTestUtils
                .getSnapshotMXBean(server, VOLD_OWNER, vvrUuid, snapshot11);
        final DeviceMXBean device22 = VvrManagerTestUtils.getDeviceMXBean(server, VOLD_OWNER, vvrUuid, device12);
        final SnapshotMXBean snapshot22 = VvrManagerTestUtils
                .getSnapshotMXBean(server, VOLD_OWNER, vvrUuid, snapshot12);
        final SnapshotMXBean snapshot23 = VvrManagerTestUtils
                .getSnapshotMXBean(server, VOLD_OWNER, vvrUuid, snapshot13);

        Assert.assertEquals(size2, snapshot23.getSize());
        Assert.assertEquals(size3, device22.getSize());
        Assert.assertEquals(size1, device21.getSize());
        Assert.assertEquals(size1, snapshot21.getSize());
        Assert.assertEquals(size2, snapshot22.getSize());
        Assert.assertEquals(1, rootSnapshot.getChildrenSnapshots().length);
        Assert.assertEquals(1, snapshot21.getChildrenSnapshots().length);
        Assert.assertEquals(1, snapshot22.getChildrenSnapshots().length);
        Assert.assertEquals(0, snapshot23.getChildrenSnapshots().length);
        Assert.assertEquals(0, rootSnapshot.getChildrenDevices().length);
        Assert.assertEquals(1, snapshot21.getChildrenDevices().length);
        Assert.assertEquals(0, snapshot22.getChildrenDevices().length);
        Assert.assertEquals(1, snapshot23.getChildrenDevices().length);
        Assert.assertEquals(rootSnapshot.getUuid(), snapshot21.getParent());
        Assert.assertEquals(snapshot21.getUuid(), snapshot22.getParent());
        Assert.assertEquals(snapshot22.getUuid(), snapshot23.getParent());
        Assert.assertEquals(snapshot23.getUuid(), device22.getParent());
        Assert.assertEquals(snapshot21.getUuid(), device21.getParent());
    }

    private void checkVvrTasks(final VvrTask[] vvrTasks1, final VvrTask[] vvrTasks2) {
        boolean found = false;

        // Compare the task list
        for (final VvrTask task1 : vvrTasks1) {
            for (final VvrTask task2 : vvrTasks2) {
                if (task1.getTaskId().equals(task2.getTaskId())) {
                    Assert.assertEquals(task1.getStatus(), task2.getStatus());
                    Assert.assertEquals(task1.getInfo().getOperation(), task2.getInfo().getOperation());
                    Assert.assertEquals(task1.getInfo().getSource(), task2.getInfo().getSource());
                    Assert.assertEquals(task1.getInfo().getTargetId(), task2.getInfo().getTargetId());
                    Assert.assertEquals(task1.getInfo().getTargetType(), task2.getInfo().getTargetType());

                    found = true;
                    break;
                }
            }
            assertTrue(found);
            found = false;
        }
    }

    private void checkVvrManagerTasks(final VvrManagerTask[] vvrManagerTasks1, final VvrManagerTask[] vvrManagerTasks2) {
        boolean found = false;

        // Compare the task list
        for (final VvrManagerTask task1 : vvrManagerTasks1) {
            for (final VvrManagerTask task2 : vvrManagerTasks2) {
                if (task1.getTaskId().equals(task2.getTaskId())) {
                    Assert.assertEquals(task1.getStatus(), task2.getStatus());
                    Assert.assertEquals(task1.getInfo().getOperation(), task2.getInfo().getOperation());
                    Assert.assertEquals(task1.getInfo().getSource(), task2.getInfo().getSource());
                    Assert.assertEquals(task1.getInfo().getTargetId(), task2.getInfo().getTargetId());
                    Assert.assertEquals(task1.getInfo().getTargetType(), task2.getInfo().getTargetType());

                    found = true;
                    break;
                }
            }
            assertTrue(found);
            found = false;
        }
    }

    @Test
    public void testHistoryDeleteSnapshot() throws Exception {

        final SnapshotMXBean rootSnapshot1 = createVvrStarted();

        // New device11
        final DeviceMXBean device11 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot1,
                vvrUuid, "D1", size1);
        // New snapshot11
        final SnapshotMXBean snapshot11 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device11, "snap1",
                VOLD_OWNER, vvrUuid);
        // New snapshot12
        final SnapshotMXBean snapshot12 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device11, "snap2",
                VOLD_OWNER, vvrUuid);
        // New snapshot13
        final SnapshotMXBean snapshot13 = VvrManagerTestUtils.takeSnapshot(server1, voldTestHelper1, device11, "snap3",
                VOLD_OWNER, vvrUuid);

        SnapshotMXBean rootSnapshot2 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(rootSnapshot1.getUuid()));
        // Check those operations onto the second peer
        DeviceMXBean device21 = VvrManagerTestUtils.getDeviceMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(device11.getUuid()));
        final SnapshotMXBean snapshot21 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(snapshot11.getUuid()));
        SnapshotMXBean snapshot22 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(snapshot12.getUuid()));
        final SnapshotMXBean snapshot23 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(snapshot13.getUuid()));

        Assert.assertEquals(size1, device21.getSize());
        Assert.assertEquals(size1, snapshot21.getSize());
        Assert.assertEquals(size1, snapshot22.getSize());
        Assert.assertEquals(size1, snapshot23.getSize());
        Assert.assertEquals(rootSnapshot2.getUuid(), snapshot21.getParent());
        Assert.assertEquals(snapshot21.getUuid(), snapshot22.getParent());
        Assert.assertEquals(snapshot22.getUuid(), snapshot23.getParent());
        Assert.assertEquals(snapshot23.getUuid(), device21.getParent());

        // Delete snapshot11 from the first peer and check if its removed from the second peer
        voldTestHelper1.waitTaskEnd(vvrUuid, snapshot11.delete(), server1);
        Assert.assertEquals(rootSnapshot2.getUuid(), snapshot22.getParent());

        // Delete from the first peer and check if its removed from the second peer
        voldTestHelper1.waitTaskEnd(vvrUuid, snapshot13.delete(), server1);
        Assert.assertEquals(snapshot22.getUuid(), device21.getParent());

        final int nbMXBeans1 = server1.getNbMXBeans();
        final int nbMXBeans2 = server2.getNbMXBeans();

        // Restart VVR managers
        restartNodes();
        initialize(); // reload local variables

        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server1, nbMXBeans1));
        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server2, nbMXBeans2));

        rootSnapshot2 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(rootSnapshot2.getUuid()));

        snapshot22 = VvrManagerTestUtils.getSnapshotMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(snapshot22.getUuid()));
        device21 = VvrManagerTestUtils.getDeviceMXBean(server2, VOLD_OWNER, vvrUuid,
                UUID.fromString(device21.getUuid()));

        Assert.assertEquals(rootSnapshot2.getUuid(), snapshot22.getParent());
        Assert.assertEquals(snapshot22.getUuid(), device21.getParent());
    }
}
