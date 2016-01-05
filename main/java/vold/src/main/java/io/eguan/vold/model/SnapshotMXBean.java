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

import io.eguan.vvr.repository.core.api.Snapshot;

/**
 * MXbean definition for a {@link Snapshot}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 */
public interface SnapshotMXBean {

    /**
     * @return the name of the snapshot
     */
    String getName();

    /**
     * Set the new name of a snapshot.
     * 
     * @param name
     *            The new name of the snapshot
     */
    void setName(String name);

    /**
     * @return the description of the snapshot
     */
    String getDescription();

    /**
     * Set the description of the device.
     * 
     * @param description
     *            the description of the device
     */
    void setDescription(String description);

    /**
     * @return the UUID of the snapshot
     */
    String getUuid();

    /**
     * @return the size of the snapshot
     */
    long getSize();

    /**
     * Gets the parent snapshot.
     * 
     * @return the parent snapshot's uuid
     */
    public String getParent();

    /**
     * Gets the UUIDs of the children snapshots.
     * 
     * @return the UUIDs of children snapshots. Maybe empty, but not <code>null</code>.
     */
    public String[] getChildrenSnapshots();

    /**
     * Gets the UUIDs of the children devices.
     * 
     * @return the UUIDs of children devices. Maybe empty, but not <code>null</code>.
     */
    public String[] getChildrenDevices();

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @return UUID of task creating the new device
     */
    String createDevice(String name);

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @param description
     *            description of the device
     * @return UUID of task creating the new device
     */
    String createDevice(String name, String description);

    /**
     * Create a device from this snapshot
     * 
     * @param name
     *            name of the device
     * @param size
     *            size of the device
     * @return UUID of task creating the new device
     */
    String createDevice(String name, long size);

    /**
     * Create a device from this snapshot
     * 
     * @param name
     *            name of the device
     * @param description
     *            of the device
     * @param size
     *            size of the device
     * @return UUID of task creating the new device
     */
    String createDevice(String name, String description, long size);

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @param uuid
     *            uuid of the device
     * @return UUID of task creating the new device
     */
    String createDeviceUuid(String name, String uuid);

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @param description
     *            description of the device
     * @param uuid
     *            uuid of the device
     * @return UUID of task creating the new device
     */
    String createDeviceUuid(String name, String description, String uuid);

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @param uuid
     *            uuid of the device
     * @param size
     *            size of the device
     * @return UUID of task creating the new device
     */
    String createDeviceUuid(String name, String uuid, long size);

    /**
     * Create a device from this snapshot.
     * 
     * @param name
     *            name of the device
     * @param description
     *            description of the device
     * @param uuid
     *            uuid of the device
     * @param size
     *            size of the device
     * @return UUID of task creating the new device
     */
    String createDeviceUuid(String name, String description, String uuid, long size);

    /**
     * Delete a snapshot.
     * 
     * @return UUID of the task that deletes the snapshot
     */
    String delete();

}
