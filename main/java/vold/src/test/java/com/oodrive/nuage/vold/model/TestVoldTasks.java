package com.oodrive.nuage.vold.model;

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

import java.lang.management.ManagementFactory;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

import com.oodrive.nuage.dtx.DtxTaskApiAbstract.TaskKeeperParameters;
import com.oodrive.nuage.dtx.DtxTaskStatus;
import com.oodrive.nuage.vvr.persistence.repository.VvrTargetType;
import com.oodrive.nuage.vvr.persistence.repository.VvrTaskOperation;

/**
 * Create a few elements and check tasks after VOLD restart.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author pwehrle
 * 
 */
public class TestVoldTasks extends AbstractVoldTest {

    private static final int MAX_SIZE = 2;
    private static final int MAX_DURATION = 15 * 1000;
    private static final int ABSOLUTE_SIZE = 15;
    private static final int ABSOLUTE_DURATION = 45 * 1000;

    private static final int PERIOD = 20 * 1000;
    private static final int DELAY = 1 * 1000;

    public TestVoldTasks() throws Exception {
        super(new TaskKeeperParameters(ABSOLUTE_DURATION, ABSOLUTE_SIZE, MAX_DURATION, MAX_SIZE, PERIOD, DELAY));
    }

    @Test
    public void testVoldReadTask() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);

        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName vvrObjectName = helper.newVvrObjectName(vvrUuid);

        final VvrMXBean vvr = JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false);
        VvrTask[] vvrTasks = vvr.getVvrTasks();
        assertEquals(0, vvrTasks.length);

        SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        final String firstDeviceTaskUuid = rootSnapshot.createDevice("name", size0);

        // Wait task is committed
        final String deviceUuidStr = helper.waitTaskEnd(vvrUuid, firstDeviceTaskUuid.toString(), server);

        // Create a lot of devices.
        for (int i = 0; i < MAX_SIZE * 2; i++) {
            rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
            final String deviceTaskUuid = rootSnapshot.createDevice("name", size0);

            // Wait task is committed
            helper.waitTaskEnd(vvrUuid, deviceTaskUuid.toString(), server);
        }
        // 3 tasks are created for each device creation.
        assertEquals((MAX_SIZE * 2 + 1) * 3, vvr.getVvrTasks().length);

        int i = 0;
        Thread.sleep(MAX_DURATION);
        // The tasks must be purged before MAX_DURATION + PERIOD
        while (vvr.getVvrTasks().length > MAX_SIZE && i < PERIOD / 1000) {
            Thread.sleep(1000);
            i++;
        }
        assertEquals(MAX_SIZE, vvr.getVvrTasks().length);

        // first task should have disappeared from the list
        vvrTasks = vvr.getVvrTasks();
        for (final VvrTask task : vvrTasks) {
            assertFalse(task.getTaskId().equals(firstDeviceTaskUuid));
        }
        // But getTask return the task (it is read in the journal)
        final VvrTask firstTask = vvr.getVvrTask(firstDeviceTaskUuid);
        assertEquals(firstDeviceTaskUuid, firstTask.getTaskId());
        assertEquals(DtxTaskStatus.COMMITTED, firstTask.getStatus());
        assertEquals(deviceUuidStr, firstTask.getInfo().getTargetId());
        assertEquals(VvrTargetType.DEVICE, firstTask.getInfo().getTargetType());
        assertEquals(VvrTaskOperation.CREATE, firstTask.getInfo().getOperation());
    }

    @Test
    public void testRestartVoldReadTask() throws Exception {
        final long size0 = (long) (1024.0 * 1024.0 * 1024.0);
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName vvrObjectName = helper.newVvrObjectName(vvrUuid);
        final VvrMXBean vvr = JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false);

        final VvrTask[] vvrTasks = vvr.getVvrTasks();
        assertEquals(0, vvrTasks.length);

        // Create a lot of devices.
        for (int i = 0; i < MAX_SIZE * 2; i++) {
            final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
            final String deviceTaskUuid = rootSnapshot.createDevice("name", size0);

            // Wait task is committed
            helper.waitTaskEnd(vvrUuid, deviceTaskUuid.toString(), server);

        }
        // 3 tasks are created for each device creation.
        assertEquals(MAX_SIZE * 2 * 3, vvr.getVvrTasks().length);

        // Do not wait the purge and restart.
        restartVold();

        // wait vvr mx bean creation
        assertTrue(helper.waitMXBeanRegistration(vvrObjectName));

        // The task must have been reloaded.
        assertEquals(MAX_SIZE * 2 * 3, vvr.getVvrTasks().length);

        // Now wait for the purge.
        Thread.sleep(MAX_DURATION);
        // The tasks must be purged before MAX_DURATION + PERIOD
        int i = 0;
        while (vvr.getVvrTasks().length > MAX_SIZE && i < PERIOD / 1000) {
            Thread.sleep(1000);
            i++;
        }
        assertEquals(MAX_SIZE, vvr.getVvrTasks().length);
    }

}
