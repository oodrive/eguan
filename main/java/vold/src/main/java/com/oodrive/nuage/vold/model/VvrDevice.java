package com.oodrive.nuage.vold.model;

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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;

import com.oodrive.nuage.iscsisrv.IscsiDevice;
import com.oodrive.nuage.iscsisrv.IscsiServer;
import com.oodrive.nuage.iscsisrv.IscsiTarget;
import com.oodrive.nuage.nbdsrv.NbdDevice;
import com.oodrive.nuage.nbdsrv.NbdExport;
import com.oodrive.nuage.nbdsrv.NbdServer;
import com.oodrive.nuage.vvr.repository.core.api.Device;
import com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle;
import com.oodrive.nuage.vvr.repository.core.api.FutureDevice;
import com.oodrive.nuage.vvr.repository.core.api.FutureSnapshot;
import com.oodrive.nuage.vvr.repository.core.api.FutureVoid;

/**
 * The class {@link Device} encapsulates a {@link VvrDevice} and is exported as a {@link DeviceMXBean}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 */
final class VvrDevice implements DeviceMXBean {

    private static final Logger LOGGER = Constants.LOGGER;

    private static final class ProtocolDeviceImpl implements IscsiDevice, NbdDevice {
        /** Read & write handle, require for reading and writing to/from a VVR device object */
        private final ReadWriteHandle rwHandle;
        /** <code>true</code> when the device is opened read-only */
        private final boolean ro;
        /** user-defined ISCSI block size */
        private final int blockSize;

        protected ProtocolDeviceImpl(final ReadWriteHandle rwHandle, final boolean ro, final int blockSize) {
            super();
            this.rwHandle = rwHandle;
            this.ro = ro;
            this.blockSize = blockSize;
        }

        @Override
        public final boolean isReadOnly() {
            return ro;
        }

        @Override
        public final long getSize() {
            return rwHandle.getSize();
        }

        @Override
        public final int getBlockSize() {
            if (blockSize > 0) {
                return blockSize;
            }
            else {
                return rwHandle.getBlockSize();
            }
        }

        @Override
        public final void read(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            final int pos = bytes.position();
            rwHandle.read(bytes, pos, length, storageIndex);
        }

        @Override
        public final void write(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            final int pos = bytes.position();
            rwHandle.write(bytes, pos, length, storageIndex);
        }

        @Override
        public final void trim(final long length, final long storageIndex) {
            rwHandle.trim(length, storageIndex);
        }

        @Override
        public final void close() throws IOException {
            rwHandle.close();
        }

    }

    /** Property name for saving the IQN of device in its own custom properties */
    private final static String IQN_PROP_NAME = "iscsi_iqn";
    /** Property name for saving the iSCSI alias name of device in its own custom properties */
    private final static String IQN_ALIAS_PROP_NAME = "iscsi_alias";
    /** Property name for saving the block size of device in its own custom properties */
    private final static String BLOCK_SIZE_PROP_NAME = "iscsi_block_size";

    /** Property name for saving the state 'active' of device in its own custom properties */
    private final static String ACTIVE_PROP_NAME = "active";
    /** Value set when the device is activated read-only */
    private final static String ACTIVE_PROP_VALUE_RO = "ro";
    /** Value set when the device is activated read-write */
    private final static String ACTIVE_PROP_VALUE_RW = "rw";

    /** Associated VVR device */
    private final Device deviceInstance;
    /** Associated iSCSI server */
    private final IscsiServer iscsiServer;
    /** Associated NBD server */
    private final NbdServer nbdServer;
    /** Current node, needed for management of activation */
    private final UUID node;
    /** Property for local activation handling, needed for management of activation */
    private final String ACTIVE_PROP_NAME_NODE;

    /** Lock guarding activation of the device */
    private final ReadWriteLock activationLock = new ReentrantReadWriteLock();

    /** Current local opened target handle */
    @GuardedBy(value = "activationLock")
    private ProtocolDeviceImpl protocolDeviceImpl;
    /** Name of the NBD export. The device may be renamed after export. */
    @GuardedBy(value = "activationLock")
    private String nbdExportName;

    private VvrDevice(final Device deviceInstance, final IscsiServer iscsiServer, final NbdServer nbdServer,
            final UUID node) {
        super();
        this.deviceInstance = deviceInstance;
        this.iscsiServer = iscsiServer;
        this.nbdServer = nbdServer;
        this.node = node;
        this.ACTIVE_PROP_NAME_NODE = ACTIVE_PROP_NAME + this.node;
    }

    /**
     * Create a new device. Initialize VOLD-defined values.
     * 
     * @param deviceInstance
     */
    final static void createVvrDevice(final Device deviceInstance) {
        // Init device
        final VvrDevice device = new VvrDevice(deviceInstance, null, null, null);

        // Compute and store default IQN and alias
        device.setIqn(getDefaultIqn(device));
        device.setIscsiAlias(getDefaultAlias(device));
    }

    /**
     * Load an existing device.
     * 
     * @param deviceInstance
     * @return new device instance
     */
    final static VvrDevice loadVvrDevice(final Device deviceInstance, @Nonnull final IscsiServer iscsiServer,
            @Nonnull final NbdServer nbdServer, @Nonnull final UUID node) {
        // Init device
        final VvrDevice result = new VvrDevice(deviceInstance, Objects.requireNonNull(iscsiServer),
                Objects.requireNonNull(nbdServer), Objects.requireNonNull(node));

        // Activate device?
        result.handleStateOnLoad();

        return result;
    }

    /**
     * Compute the default iSCSI iqn for the given device.
     * 
     * @param vvrDevice
     * @return default iqn.
     */
    private static final String getDefaultIqn(final VvrDevice vvrDevice) {
        String name = vvrDevice.getName();
        if (name == null || name.equals("")) {
            name = vvrDevice.getUuid();
        }
        return Constants.IQN_PREFIX + name;
    }

    /**
     * Compute the default iSCSI alias for the given device.
     * 
     * @param vvrDevice
     * @return default iSCSI alias.
     */
    private static final String getDefaultAlias(final VvrDevice vvrDevice) {
        return getDefaultIqn(vvrDevice);
    }

    /**
     * Take care of activating a device if the according state was persisted.
     */
    private final void handleStateOnLoad() {
        activationLock.writeLock().lock();
        try {
            // Could be ro, rw or null
            final String activeState = deviceInstance.getUserProperty(ACTIVE_PROP_NAME_NODE);
            if (activeState != null) {
                if (activeState.equals(ACTIVE_PROP_VALUE_RO)) {
                    doActivate(false);
                }
                else if (activeState.equals(ACTIVE_PROP_VALUE_RW)) {
                    doActivate(true);
                }
            }
        }
        finally {
            activationLock.writeLock().unlock();
        }
    }

    @Override
    public final String getName() {
        return deviceInstance.getName();
    }

    @Override
    public final void setName(final String name) {
        final FutureVoid futureTask = deviceInstance.setName(name);
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
        return deviceInstance.getDescription();
    }

    @Override
    public final void setDescription(final String description) {
        final FutureVoid futureTask = deviceInstance.setDescription(description);
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
     * @see com.oodrive.nuage.vold.model.DeviceMXBean#getUuid()
     */
    @Override
    public final String getUuid() {
        return getUuidUuid().toString();
    }

    final UUID getUuidUuid() {
        return deviceInstance.getUuid();
    }

    @Override
    public final String getIqn() {
        return deviceInstance.getUserProperty(IQN_PROP_NAME);
    }

    @Override
    public final void setIqn(final String iqn) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIqnNoWait(iqn);
        try {
            futureVoid.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // Failed to edit the device
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String setIqnNoWait(final String iqn) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIqnNoWait(iqn);
        return futureVoid.getTaskId().toString();
    }

    private final FutureVoid doSetIqnNoWait(final String iqn) throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            if (isActive()) {
                throw new IllegalStateException("IQN change while device is active is not allowed");
            }
            return setDeviceProperties(IQN_PROP_NAME, iqn);
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String getIscsiAlias() {
        return deviceInstance.getUserProperty(IQN_ALIAS_PROP_NAME);
    }

    @Override
    public final void setIscsiAlias(final String alias) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIscsiAliasNoWait(alias);
        try {
            futureVoid.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // Failed to edit the device
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String setIscsiAliasNoWait(final String alias) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIscsiAliasNoWait(alias);
        return futureVoid.getTaskId().toString();
    }

    private final FutureVoid doSetIscsiAliasNoWait(final String alias) throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            if (isActive()) {
                throw new IllegalStateException("iSCSI alias change while device is active is not allowed");
            }
            return setDeviceProperties(IQN_ALIAS_PROP_NAME, alias);
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final int getIscsiBlockSize() {
        if (deviceInstance.getUserProperty(BLOCK_SIZE_PROP_NAME) != null) {
            return (Integer.parseInt(deviceInstance.getUserProperty(BLOCK_SIZE_PROP_NAME)));
        }
        else {
            return 0;
        }
    }

    @Override
    public final void setIscsiBlockSize(final int blockSize) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIscsiBlockSizeNoWait(blockSize);
        try {
            futureVoid.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // Failed to edit the device
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String setIscsiBlockSizeNoWait(final int blockSize) throws IllegalStateException {
        final FutureVoid futureVoid = doSetIscsiBlockSizeNoWait(blockSize);
        return futureVoid.getTaskId().toString();
    }

    private final FutureVoid doSetIscsiBlockSizeNoWait(final int blockSize) throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            if (isActive()) {
                throw new IllegalStateException("iSCSI block size change while device is active is not allowed");
            }
            if (blockSize <= 0) {
                return removeDeviceProperties(BLOCK_SIZE_PROP_NAME);
            }
            else {
                return setDeviceProperties(BLOCK_SIZE_PROP_NAME, String.valueOf(blockSize));
            }
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final long getSize() {
        return deviceInstance.getSize();
    }

    @Override
    public final void setSize(final long size) {
        final FutureVoid futureVoid = doSetSize(size);
        try {
            futureVoid.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // Failed to edit the device
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String setSizeNoWait(final long size) {
        final FutureVoid futureVoid = doSetSize(size);
        return futureVoid.getTaskId().toString();
    }

    private final FutureVoid doSetSize(final long size) {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final int blockSize = deviceInstance.getBlockSize();
            final long roundedSize = size - (size % blockSize);
            return deviceInstance.setSize(roundedSize);
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public String getParent() {
        return deviceInstance.getParent().toString();
    }

    @Override
    public final boolean isActive() {
        activationLock.readLock().lock();
        try {
            return deviceInstance.getUserProperty(ACTIVE_PROP_NAME) != null;
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    final boolean isActiveLocally() {
        activationLock.readLock().lock();
        try {
            return deviceInstance.getUserProperty(ACTIVE_PROP_NAME_NODE) != null;
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final boolean isReadOnly() {
        activationLock.readLock().lock();
        try {
            return ACTIVE_PROP_VALUE_RO.equals(deviceInstance.getUserProperty(ACTIVE_PROP_NAME));
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String activateRO() {
        final FutureVoid futureVoid;
        activationLock.writeLock().lock();
        try {
            // Activated RW somewhere?
            if (ACTIVE_PROP_VALUE_RW.equals(deviceInstance.getUserProperty(ACTIVE_PROP_NAME))) {
                throw new IllegalStateException("activated RW");
            }

            // Activate device
            doActivate(false);

            // Persist state
            futureVoid = setDeviceProperties(ACTIVE_PROP_NAME, ACTIVE_PROP_VALUE_RO, ACTIVE_PROP_NAME_NODE,
                    ACTIVE_PROP_VALUE_RO);
        }
        finally {
            activationLock.writeLock().unlock();
        }
        return futureVoid.getTaskId().toString();
    }

    @Override
    public final String activateRW() {
        final FutureVoid futureVoid;
        activationLock.writeLock().lock();
        try {
            // Already activated?
            if (isActive()) {
                throw new IllegalStateException("activated");
            }
            // Activate device
            doActivate(true);

            // Persist state
            futureVoid = setDeviceProperties(ACTIVE_PROP_NAME, ACTIVE_PROP_VALUE_RW, ACTIVE_PROP_NAME_NODE,
                    ACTIVE_PROP_VALUE_RW);
        }
        finally {
            activationLock.writeLock().unlock();
        }
        return futureVoid.getTaskId().toString();
    }

    /**
     * Open the device and create the iSCSI target.
     * 
     * @param rw
     *            <code>true</code> to open read-write
     */
    private final void doActivate(final boolean rw) {
        // TODO create a local task to register target in background after the end of the activation
        try {
            deviceInstance.activate().get();
        }
        catch (InterruptedException | ExecutionException e) {
            // Failed to activate the device
            throw new IllegalStateException(e);
        }
        final ReadWriteHandle rwHandle = deviceInstance.open(rw);

        // Implementation of protocol interface
        protocolDeviceImpl = new ProtocolDeviceImpl(rwHandle, !rw, getIscsiBlockSize());

        // iSCSI target
        final IscsiTarget iScsiTarget = IscsiTarget.newIscsiTarget(getIqn(), getIscsiAlias(), protocolDeviceImpl);
        iscsiServer.addTarget(iScsiTarget);

        // NBD export
        nbdExportName = getName();
        final NbdExport nbdExport = new NbdExport(nbdExportName, protocolDeviceImpl);
        nbdServer.addTarget(nbdExport);
    }

    @Override
    public final String deActivate() {
        final FutureVoid futureVoid;
        activationLock.writeLock().lock();
        try {
            // Deactivate
            doDeactivate();

            // Persist state
            // Must not remove property ACTIVE_PROP_NAME if the device is activated elsewhere
            boolean activeLocalOnly = true;
            final Map<String, String> deviceProperties = deviceInstance.getUserProperties();
            final Iterator<String> keys = deviceProperties.keySet().iterator();
            while (keys.hasNext() && activeLocalOnly) {
                final String key = keys.next();
                if (key.startsWith(ACTIVE_PROP_NAME)) {
                    activeLocalOnly = ACTIVE_PROP_NAME_NODE.equals(key) || ACTIVE_PROP_NAME.equals(key);
                }
            }

            if (activeLocalOnly) {
                futureVoid = removeDeviceProperties(ACTIVE_PROP_NAME, ACTIVE_PROP_NAME_NODE);
            }
            else {
                futureVoid = removeDeviceProperties(ACTIVE_PROP_NAME_NODE);
            }
        }
        finally {
            activationLock.writeLock().unlock();
        }
        return futureVoid.getTaskId().toString();
    }

    /**
     * Deactivate device. Do not persist state. Does nothing if the device is not activated.
     */
    final void doDeactivate() {
        activationLock.writeLock().lock();
        try {
            if (protocolDeviceImpl != null) {
                // Remove from NBD server
                try {
                    nbdServer.removeTarget(nbdExportName);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while removing " + toString() + " from NBD server", t);
                }
                nbdExportName = null;

                // Remove from iSCSI server
                try {
                    iscsiServer.removeTarget(getIqn());
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while removing " + toString() + " from iSCSI server", t);
                }

                try {
                    protocolDeviceImpl.close();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while closing " + toString(), t);
                }

                try {
                    deviceInstance.deactivate();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while deactivating " + toString(), t);
                }

                // Reset field (state deactivated)
                protocolDeviceImpl = null;
            }
        }
        finally {
            activationLock.writeLock().unlock();
        }
    }

    /**
     * Set the given properties in the device.
     * 
     * @param keyValues
     *            key/value pairs
     */
    private final FutureVoid setDeviceProperties(final String... keyValues) {
        return deviceInstance.setUserProperties(keyValues);
    }

    /**
     * Remove the given keys from the device properties.
     * 
     * @param keys
     *            property keys
     */
    private final FutureVoid removeDeviceProperties(final String... keys) {
        return deviceInstance.unsetUserProperties(keys);
    }

    /**
     * Check if the item can be changed on the current node. Need activation lock.
     */
    private final void checkModifyItem() {
        // No need to take a snapshot if the device is activated RO
        if (ACTIVE_PROP_VALUE_RO.equals(deviceInstance.getUserProperty(ACTIVE_PROP_NAME))) {
            throw new IllegalStateException("Activated read-only");
        }
        // Activated read-write?
        if (ACTIVE_PROP_VALUE_RW.equals(deviceInstance.getUserProperty(ACTIVE_PROP_NAME))) {
            if (!ACTIVE_PROP_VALUE_RW.equals(deviceInstance.getUserProperty(ACTIVE_PROP_NAME_NODE))) {
                throw new IllegalStateException("Activated read-write");
            }
        }
    }

    @Override
    public final String takeSnapshot() throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot();
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String takeSnapshot(final String name) throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot(name);
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String takeSnapshot(final String name, final String description) throws IllegalStateException {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot(name, description);
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String takeSnapshotUuid(final String uuid) throws IllegalStateException {
        final UUID uuidObj = UUID.fromString(uuid);
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot(uuidObj);
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String takeSnapshotUuid(final String name, final String uuid) throws IllegalStateException {
        final UUID uuidObj = UUID.fromString(uuid);
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot(name, uuidObj);
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String takeSnapshotUuid(final String name, final String description, final String uuid)
            throws IllegalStateException {
        final UUID uuidObj = UUID.fromString(uuid);
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureSnapshot futureSnapshot = deviceInstance.createSnapshot(name, description, uuidObj);
            return futureSnapshot.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String delete() {
        if (isActive()) {
            throw new IllegalStateException("Active");
        }
        final FutureVoid future = deviceInstance.delete();
        return future.getTaskId().toString();
    }

    @Override
    public final String clone(final String name) {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureDevice future = deviceInstance.clone(name);
            try {
                VvrDevice.createVvrDevice(future.get());
            }
            catch (final Exception e) {
                throw new IllegalStateException("Failed to clone device", e);
            }
            return future.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String clone(final String name, final String description) {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureDevice future = deviceInstance.clone(name, description);
            try {
                VvrDevice.createVvrDevice(future.get());
            }
            catch (final Exception e) {
                throw new IllegalStateException("Failed to clone device", e);
            }
            return future.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String cloneUuid(final String name, final String uuid) {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureDevice future = deviceInstance.clone(name, UUID.fromString(uuid));
            try {
                VvrDevice.createVvrDevice(future.get());
            }
            catch (final Exception e) {
                throw new IllegalStateException("Failed to clone device", e);
            }
            return future.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String cloneUuid(final String name, final String description, final String uuid) {
        activationLock.readLock().lock();
        try {
            checkModifyItem();

            final FutureDevice future = deviceInstance.clone(name, description, UUID.fromString(uuid));
            try {
                VvrDevice.createVvrDevice(future.get());
            }
            catch (final Exception e) {
                throw new IllegalStateException("Failed to clone device", e);
            }
            return future.getTaskId().toString();
        }
        finally {
            activationLock.readLock().unlock();
        }
    }

    @Override
    public final String toString() {
        return "VvrDevice[uuid=" + getUuid() + ",name=" + getName() + ",IQN=" + getIqn() + "]";
    }

}
