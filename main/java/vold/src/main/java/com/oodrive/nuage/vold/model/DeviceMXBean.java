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

/**
 * MXbean definition for a {@link Device}
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 */
public interface DeviceMXBean {
    /**
     * @return the name of the device
     */
    String getName();

    /**
     * Set the new name of a device
     * 
     * @param name
     *            The new name of the device
     */
    void setName(String name);

    /**
     * @return the description of the device
     */
    String getDescription();

    /**
     * Set the description of the device
     * 
     * @param description
     *            the description of the device
     */
    void setDescription(String description);

    /**
     * @return the UUID of the device
     */
    String getUuid();

    /**
     * @return The IQN (iscsi qualified name) of the device
     */
    String getIqn();

    /**
     * Set the IQN (iSCSI qualified name) of the device. This method waits for the end of the task that changes the IQN.
     * It is necessary to have a method <code>void setXyz()</code> to comply with the Bean contract.
     * 
     * @param iqn
     *            the IQN (iSCSI qualified name) of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    void setIqn(String iqn) throws IllegalStateException;

    /**
     * Set the IQN (iSCSI qualified name) of the device.
     * 
     * @param iqn
     *            the IQN (iSCSI qualified name) of the device
     * @return UUID of the task that sets the IQN of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    String setIqnNoWait(String iqn) throws IllegalStateException;

    /**
     * @return The Iscsi alias of the device
     */
    String getIscsiAlias();

    /**
     * Set the iSCSI alias of the device. This method waits for the end of the task that changes the alias. It is
     * necessary to have a method <code>void setXyz()</code> to comply with the Bean contract.
     * 
     * @param alias
     *            The iSCSI alias of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    void setIscsiAlias(String alias) throws IllegalStateException;

    /**
     * Set the iSCSI alias of the device.
     * 
     * @param alias
     *            The iSCSI alias of the device
     * @return UUID of the task that sets the iSCSI alias of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    String setIscsiAliasNoWait(String alias) throws IllegalStateException;

    /**
     * @return The Iscsi block size of the device
     */
    int getIscsiBlockSize();

    /**
     * Set the iSCSI block size of the device. This method waits for the end of the task that changes the block size. It
     * is necessary to have a method <code>void setXyz()</code> to comply with the Bean contract.
     * 
     * @param blockSize
     *            The iSCSI block size of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    void setIscsiBlockSize(int blockSize) throws IllegalStateException;

    /**
     * Set the iSCSI block size of the device.
     * 
     * @param blockSize
     *            The iSCSI block size of the device
     * @return UUID of the task that sets the iSCSI block size of the device
     * @throws IllegalStateException
     *             if the device is active
     */
    String setIscsiBlockSizeNoWait(int blockSize) throws IllegalStateException;

    /**
     * @return the size of the device
     */
    long getSize();

    /**
     * Set a new size of the device.
     * 
     * @param size
     *            the new size
     */
    void setSize(long size);

    /**
     * Set a new size of the device. This method waits for the end of the task that changes the size. It is necessary to
     * have a method <code>void setXyz()</code> to comply with the Bean contract.
     * 
     * @param size
     *            the new size
     */
    String setSizeNoWait(long size);

    /**
     * Gets the parent snapshot.
     * 
     * @return the parent snapshot's uuid
     */
    public String getParent();

    /**
     * Tells if the device is activated.
     * 
     * @return true if the device is activated
     */
    boolean isActive();

    /**
     * Tells if the device is activated read-only.
     * 
     * @return true if the device is activated read-only
     */
    boolean isReadOnly();

    /**
     * Activate a device, RO mode
     * 
     * @return UUID of the task that activates the device
     */
    String activateRO();

    /**
     * Activate a device, RW mode
     * 
     * @return UUID of the task that activates the device
     */
    String activateRW();

    /**
     * Deactivate a device
     * 
     * @return UUID of the task that deactivates the device
     */
    String deActivate();

    /**
     * Take a snapshot from this device.
     * 
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshot() throws IllegalStateException;

    /**
     * Take a snapshot from this device.
     * 
     * @param name
     *            name of the snapshot
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshot(String name) throws IllegalStateException;

    /**
     * Take a snapshot from this device.
     * 
     * @param name
     *            name of the snapshot
     * @param description
     *            of the snapshot
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshot(String name, String description) throws IllegalStateException;

    /**
     * Take a snapshot from this device.
     * 
     * @param uuid
     *            UUID of the snapshot to create.
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshotUuid(String uuid) throws IllegalStateException;

    /**
     * Take a snapshot from this device.
     * 
     * @param name
     *            name of the snapshot
     * @param uuid
     *            UUID of the snapshot to create.
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshotUuid(String name, String uuid) throws IllegalStateException;

    /**
     * Take a snapshot from this device.
     * 
     * @param name
     *            name of the snapshot
     * @param description
     *            description of the snapshot
     * @param uuid
     *            UUID of the snapshot to create.
     * @return UUID of the task that takes the snapshot
     * @throws IllegalStateException
     *             if the device is activated read-only or if the device is activated read-write on another node.
     */
    String takeSnapshotUuid(String name, String description, String uuid) throws IllegalStateException;

    /**
     * Delete current device
     * 
     * @return UUID of the task that deletes the device
     */
    String delete();

    /**
     * Clone current device
     * 
     * @param name
     *            name of the new device
     * 
     * @return UUID of the task that clones the device
     */
    String clone(String name) throws IllegalStateException;

    /**
     * Clone current device
     * 
     * @param name
     *            name of the new device
     * @param description
     *            description of the new device
     * 
     * @return UUID of the task that clones the device
     */
    String clone(String name, String description) throws IllegalStateException;

    /**
     * Clone current device
     * 
     * @param name
     *            name of the new device
     * @param uuid
     *            unique identifier of the new device
     * 
     * @return UUID of the task that clones the device
     */
    String cloneUuid(String name, String uuid) throws IllegalStateException;

    /**
     * Clone current device
     * 
     * @param name
     *            name of the new device
     * @param description
     *            description of the new device
     * @param uuid
     *            unique identifier of the new device
     * 
     * @return UUID of the task that clones the device
     */
    String cloneUuid(String name, String description, String uuid) throws IllegalStateException;

}
