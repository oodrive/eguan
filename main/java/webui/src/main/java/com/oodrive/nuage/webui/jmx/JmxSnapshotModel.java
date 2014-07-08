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

import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.oodrive.nuage.vold.model.SnapshotMXBean;
import com.oodrive.nuage.vold.model.VvrObjectNameFactory;
import com.oodrive.nuage.webui.model.SnapshotModel;
import com.oodrive.nuage.webui.model.VvrModel;

final class JmxSnapshotModel implements SnapshotModel {

    private final SnapshotMXBean snapshotMXBean;

    public JmxSnapshotModel(final MBeanServerConnection mBeanServerConnection, final UUID ownerUuid,
            final VvrModel vvr, final UUID snapshotUuid) {
        final ObjectName snapshotObjectName = VvrObjectNameFactory.newSnapshotObjectName(ownerUuid, vvr.getItemUuid(),
                snapshotUuid);
        this.snapshotMXBean = JMX
                .newMXBeanProxy(mBeanServerConnection, snapshotObjectName, SnapshotMXBean.class, false);
    }

    @Override
    public final UUID getItemUuid() {
        return UUID.fromString(snapshotMXBean.getUuid());
    }

    @Override
    public final String getSnapshotName() {
        return snapshotMXBean.getName();
    }

    @Override
    public final void setSnapshotName(final String name) {
        snapshotMXBean.setName(name);

    }

    @Override
    public final String getSnapshotDescription() {
        return snapshotMXBean.getDescription();
    }

    @Override
    public final void setSnapshotDescription(final String desc) {
        snapshotMXBean.setDescription(desc);

    }

    @Override
    public final long getSnapshotSize() {
        return snapshotMXBean.getSize();
    }

    @Override
    public final UUID[] getSnapshotChildrenDevices() {
        final String[] children = snapshotMXBean.getChildrenDevices();
        final UUID[] uuidChildren = new UUID[children.length];
        int i = 0;
        for (final String child : children) {
            uuidChildren[i++] = UUID.fromString(child);
        }
        return uuidChildren;
    }

    @Override
    public final UUID[] getSnapshotChildrenSnapshots() {
        final String[] children = snapshotMXBean.getChildrenSnapshots();
        final UUID[] uuidChildren = new UUID[children.length];
        int i = 0;
        for (final String child : children) {
            uuidChildren[i++] = UUID.fromString(child);
        }
        return uuidChildren;
    }

    @Override
    public final UUID getSnapshotParent() {
        return UUID.fromString(snapshotMXBean.getParent());
    }

    @Override
    public final void createDevice(final String deviceName, final long size) {
        snapshotMXBean.createDevice(deviceName, size);
    }

    @Override
    public final void deleteSnapshot() {
        snapshotMXBean.delete();
    }

}
