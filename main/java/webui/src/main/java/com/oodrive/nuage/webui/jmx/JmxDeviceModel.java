package com.oodrive.nuage.webui.jmx;

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

import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.model.VvrObjectNameFactory;
import com.oodrive.nuage.webui.model.DeviceModel;
import com.oodrive.nuage.webui.model.VvrModel;

/**
 * The class represents a device in the JMX model.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class JmxDeviceModel implements DeviceModel {

    private final DeviceMXBean deviceMXBean;
    private final VvrModel vvr;

    JmxDeviceModel(final MBeanServerConnection jmxConnection, final UUID ownerUuid, final VvrModel vvr,
            final UUID deviceUuid) {
        final ObjectName deviceObjectName = VvrObjectNameFactory.newDeviceObjectName(ownerUuid, vvr.getItemUuid(),
                deviceUuid);
        this.vvr = vvr;
        this.deviceMXBean = JMX.newMXBeanProxy(jmxConnection, deviceObjectName, DeviceMXBean.class, false);
    }

    @Override
    public final UUID getItemUuid() {
        return UUID.fromString(deviceMXBean.getUuid());
    }

    @Override
    public final String getDeviceName() {
        return deviceMXBean.getName();
    }

    @Override
    public final void setDeviceName(final String name) {
        deviceMXBean.setName(name);
    }

    @Override
    public final String getDeviceDescription() {
        return deviceMXBean.getDescription();
    }

    @Override
    public final void setDeviceDescription(final String description) {
        deviceMXBean.setDescription(description);
    }

    @Override
    public final String getDeviceIqn() {
        return deviceMXBean.getIqn();
    }

    @Override
    public final void setDeviceIqn(final String iqn) throws IllegalStateException {
        deviceMXBean.setIqn(iqn);
    }

    @Override
    public final String getDeviceIscsiAlias() {
        return deviceMXBean.getIscsiAlias();
    }

    @Override
    public final void setDeviceIscsiAlias(final String alias) throws IllegalStateException {
        deviceMXBean.setIscsiAlias(alias);
    }

    @Override
    public final int getDeviceIscsiBlockSize() {
        return deviceMXBean.getIscsiBlockSize();
    }

    @Override
    public final void setDeviceIscsiBlockSize(final int blockSize) throws IllegalStateException {
        deviceMXBean.setIscsiBlockSize(blockSize);
    }

    @Override
    public final long getDeviceSize() {
        return deviceMXBean.getSize();
    }

    @Override
    public final void setDeviceSize(final long size) {
        deviceMXBean.setSize(size);
    }

    @Override
    public final UUID getDeviceParent() {
        return UUID.fromString(deviceMXBean.getParent());
    }

    @Override
    public final boolean isDeviceActive() {
        return deviceMXBean.isActive();
    }

    @Override
    public final boolean isDeviceReadOnly() {
        return deviceMXBean.isReadOnly();
    }

    @Override
    public final void activateDeviceRO() {
        final String taskUuid = deviceMXBean.activateRO();
        // Wait RO activation
        if (vvr instanceof JmxVvrModel) {
            ((JmxVvrModel) vvr).waitTaskEnd(taskUuid);
        }
    }

    @Override
    public final void activateDeviceRW() {
        final String taskUuid = deviceMXBean.activateRW();
        // Wait RW activation
        if (vvr instanceof JmxVvrModel) {
            ((JmxVvrModel) vvr).waitTaskEnd(taskUuid);
        }
    }

    @Override
    public final void deActivateDevice() {
        final String taskUuid = deviceMXBean.deActivate();
        // Wait RW activation
        if (vvr instanceof JmxVvrModel) {
            ((JmxVvrModel) vvr).waitTaskEnd(taskUuid);
        }
    }

    @Override
    public final void takeDeviceSnapshot(final String name) throws IllegalStateException {
        if (name.isEmpty() || name == null) {
            deviceMXBean.takeSnapshot();
        }
        else
            deviceMXBean.takeSnapshot(name);
    }

    @Override
    public final void deleteDevice() {
        deviceMXBean.delete();
    }

}
