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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.oodrive.nuage.vvr.repository.core.api.FutureDevice;
import com.oodrive.nuage.vvr.repository.core.api.FutureVoid;
import com.oodrive.nuage.vvr.repository.core.api.Snapshot;

/**
 * The class {@link Snapshot} encapsulates a {@link VvrSnapshot} and is exported as a {@link SnapshotMXBean}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 */
final class VvrSnapshot implements SnapshotMXBean {

    private final Snapshot snapshotInstance;

    VvrSnapshot(final Snapshot snapshotInstance) {
        super();
        this.snapshotInstance = snapshotInstance;
    }

    @Override
    public final String getName() {
        return snapshotInstance.getName();
    }

    @Override
    public final void setName(final String name) {
        final FutureVoid futureTask = snapshotInstance.setName(name);
        if (futureTask == null) {
            return;
        }
        try {
            futureTask.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // propagate failure
            throw new IllegalStateException(e);
        }

    }

    @Override
    public final String getDescription() {
        return snapshotInstance.getDescription();
    }

    @Override
    public final void setDescription(final String description) {
        final FutureVoid futureTask = snapshotInstance.setDescription(description);
        if (futureTask == null) {
            return;
        }
        try {
            futureTask.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // propagate failure
            throw new IllegalStateException(e);
        }

    }

    /*
     * Should be getUuidString(), but, in this case, the JMX attribute would be 'uuidString'
     * 
     * @see com.oodrive.nuage.vold.model.SnapshotMXBean#getUuid()
     */
    @Override
    public final String getUuid() {
        return getUuidUuid().toString();
    }

    final UUID getUuidUuid() {
        return snapshotInstance.getUuid();
    }

    @Override
    public final long getSize() {
        return snapshotInstance.getSize();
    }

    @Override
    public String getParent() {
        return snapshotInstance.getParent().toString();
    }

    @Override
    public final String[] getChildrenSnapshots() {
        final Collection<UUID> children = snapshotInstance.getChildrenSnapshotsUuid();
        final String[] result = new String[children.size()];
        int i = 0;
        for (final Iterator<UUID> iterator = children.iterator(); iterator.hasNext();) {
            result[i++] = iterator.next().toString();
        }
        return result;
    }

    @Override
    public final String[] getChildrenDevices() {
        final Collection<UUID> children = snapshotInstance.getSnapshotDevicesUuid();
        final String[] result = new String[children.size()];
        int i = 0;
        for (final Iterator<UUID> iterator = children.iterator(); iterator.hasNext();) {
            result[i++] = iterator.next().toString();
        }
        return result;
    }

    @Override
    public final String createDevice(final String name) {
        final FutureDevice futureDevice = snapshotInstance.createDevice(name);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDevice(final String name, final String description) {
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, description);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDevice(final String name, final long size) {
        final int blockSize = snapshotInstance.getBlockSize();
        final long roundedSize = size - (size % blockSize);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, roundedSize);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDevice(final String name, final String description, final long size) {
        final int blockSize = snapshotInstance.getBlockSize();
        final long roundedSize = size - (size % blockSize);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, description, roundedSize);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDeviceUuid(final String name, final String uuid) {
        final UUID uuidObj = UUID.fromString(uuid);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, uuidObj);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDeviceUuid(final String name, final String description, final String uuid) {
        final UUID uuidObj = UUID.fromString(uuid);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, description, uuidObj);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDeviceUuid(final String name, final String uuid, final long size) {
        final UUID uuidObj = UUID.fromString(uuid);
        final int blockSize = snapshotInstance.getBlockSize();
        final long roundedSize = size - (size % blockSize);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, uuidObj, roundedSize);
        return devicePostTask(futureDevice);
    }

    @Override
    public final String createDeviceUuid(final String name, final String description, final String uuid, final long size) {
        final UUID uuidObj = UUID.fromString(uuid);
        final int blockSize = snapshotInstance.getBlockSize();
        final long roundedSize = size - (size % blockSize);
        final FutureDevice futureDevice = snapshotInstance.createDevice(name, description, uuidObj, roundedSize);
        return devicePostTask(futureDevice);
    }

    /**
     * Operations done on the FutureDevice
     * 
     * @param futureDevice
     * @return the UUID of the task that created the device
     */
    private final String devicePostTask(final FutureDevice futureDevice) {
        try {
            VvrDevice.createVvrDevice(futureDevice.get());
        }
        catch (final Exception e) {
            throw new IllegalStateException("Failed to create device", e);
        }
        return futureDevice.getTaskId().toString();
    }

    @Override
    public final String delete() {
        final FutureVoid future = snapshotInstance.delete();
        return future.getTaskId().toString();
    }

}
