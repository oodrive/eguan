package com.oodrive.nuage.webui.jmx;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.dtx.DtxTaskStatus;
import com.oodrive.nuage.vold.model.VvrMXBean;
import com.oodrive.nuage.vold.model.VvrObjectNameFactory;
import com.oodrive.nuage.vold.model.VvrTask;
import com.oodrive.nuage.webui.model.VvrModel;

final class JmxVvrModel implements VvrModel {

    private final VvrMXBean vvrMXBean;
    private final UUID ownerUuid;
    private final MBeanServerConnection jmxConnection;

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxVvrModel.class);

    JmxVvrModel(final MBeanServerConnection jmxConnection, final UUID ownerUuid, final UUID vvrUuid) {
        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(ownerUuid, vvrUuid);
        this.vvrMXBean = JMX.newMXBeanProxy(jmxConnection, vvrObjectName, VvrMXBean.class, false);
        this.ownerUuid = ownerUuid;
        this.jmxConnection = jmxConnection;
    }

    @Override
    public final UUID getItemUuid() {
        return UUID.fromString(vvrMXBean.getUuid());
    }

    @Override
    public final String getVvrDescription() {
        return vvrMXBean.getDescription();
    }

    @Override
    public final void setVvrDescription(final String desc) {
        vvrMXBean.setDescription(desc);
    }

    @Override
    public final String getVvrName() {
        return vvrMXBean.getName();
    }

    @Override
    public final void setVvrName(final String name) {
        vvrMXBean.setName(name);
    }

    @Override
    public final void startVvr() {
        vvrMXBean.start();
    }

    @Override
    public final void stopVvr() {
        vvrMXBean.stop();
    }

    @Override
    public final boolean isVvrStarted() {
        return vvrMXBean.isStarted();
    }

    @Override
    public final Set<UUID> getSnapshotsList() {
        final Set<ObjectName> snapshotInstances;
        final HashSet<UUID> snapshotUuid = new HashSet<>();
        try {
            snapshotInstances = jmxConnection.queryNames(
                    VvrObjectNameFactory.newSnapshotQueryListObjectName(ownerUuid, getItemUuid()), null);
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying Snapshots", e);
            throw new IllegalArgumentException();
        }
        if (snapshotInstances.isEmpty()) {
            LOGGER.debug("No Snapshots");
        }
        for (final ObjectName snapshotObjectName : snapshotInstances) {
            snapshotUuid.add(JmxHandler.getSnapshotUuid(snapshotObjectName));
        }
        return snapshotUuid;
    }

    @Override
    public final Set<UUID> getDevicesList() {
        final Set<ObjectName> deviceInstances;
        final HashSet<UUID> deviceUuid = new HashSet<>();
        try {
            deviceInstances = jmxConnection.queryNames(
                    VvrObjectNameFactory.newDeviceQueryListObjectName(ownerUuid, getItemUuid()), null);
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying Snapshots", e);
            throw new IllegalArgumentException();
        }
        if (deviceInstances.isEmpty()) {
            LOGGER.debug("No Devices");
        }
        for (final ObjectName vvrObjectName : deviceInstances) {
            deviceUuid.add(JmxHandler.getDeviceUuid(vvrObjectName));
        }
        return deviceUuid;
    }

    @Override
    public final UUID getRootSnapshot() {
        final ObjectName rootSnapName;
        try {
            final Set<ObjectName> foundNames = jmxConnection.queryNames(
                    VvrObjectNameFactory.newSnapshotQueryListObjectName(ownerUuid, getItemUuid()),
                    Query.eq(Query.attr("Uuid"), Query.attr("Parent")));
            if (foundNames.isEmpty()) {
                LOGGER.debug("No root snapshot");
                return null;
            }
            rootSnapName = foundNames.iterator().next();
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying Snapshots", e);
            throw new IllegalArgumentException();
        }
        return JmxHandler.getSnapshotUuid(rootSnapName);
    }

    final String waitTaskEnd(final String taskId) {
        while (true) {
            final VvrTask task = vvrMXBean.getVvrTask(taskId);
            if (task == null) {
                LOGGER.error("taskId=" + taskId + " not found");
                throw new IllegalArgumentException("Invalid task ID");
            }
            if (!taskId.equals(task.getTaskId())) {
                LOGGER.error("taskId=" + taskId + " not found");
                throw new IllegalArgumentException("Invalid task ID");
            }

            final DtxTaskStatus status = task.getStatus();
            if (status == DtxTaskStatus.COMMITTED) {
                return task.getInfo().getTargetId();
            }
            else if (status == DtxTaskStatus.ROLLED_BACK || status == DtxTaskStatus.UNKNOWN) {
                throw new IllegalStateException("Task status=" + status);
            }

            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
        }
    }

}
