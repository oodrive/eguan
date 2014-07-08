package com.oodrive.nuage.vvr.persistence.repository;

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
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import javax.naming.OperationNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.nrs.NrsFileFlag;
import com.oodrive.nuage.nrs.NrsFileHeader;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.utils.SimpleIdentifierProvider;
import com.oodrive.nuage.utils.UuidT;
import com.oodrive.nuage.vvr.remote.VvrRemoteUtils;
import com.oodrive.nuage.vvr.repository.core.api.Device;
import com.oodrive.nuage.vvr.repository.core.api.FutureDevice;
import com.oodrive.nuage.vvr.repository.core.api.FutureVoid;
import com.oodrive.nuage.vvr.repository.core.api.Snapshot;

/**
 * Snapshot implementation.
 * <p>
 * This class adds the logic specific to snapshots to the more generic {@link AbstractSimpleStorageVolume}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * 
 */
public final class NrsSnapshot extends NrsVvrItem implements Snapshot {

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsSnapshot.class);

    static final String DELETED_KEY = "deleted";
    static final String NRSFILE_UUID_KEY = "fileuuid";

    /**
     * Flag for indicating snapshots that are considered deleted.
     */
    private boolean deleted = false;

    /**
     * Flag for the root snapshot.
     */
    private final boolean root;

    /**
     * Builder constructor called by {@link NrsSnapshot.Builder}s.
     * 
     * @param builder
     *            the configured builder
     */
    private NrsSnapshot(final NrsSnapshot.Builder builder) {
        super(builder);
        LOGGER.trace("building new {} instance with uuid {}", NrsSnapshot.class.getSimpleName(), this.getUuid());
        this.deleted = builder.deleted();
        this.root = getNrsFilePath().getDescriptor().isRoot();
    }

    /**
     * Builder for the root snapshot of a {@link NrsRepository}.
     * 
     * @param builder
     */
    private NrsSnapshot(final NrsSnapshot.BuilderRoot builder) {
        super(builder);
        LOGGER.trace("building root with uuid {}", this.getUuid());
        this.root = getNrsFilePath().getDescriptor().isRoot();
        assert root == true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.vvr.persistence.repository.NrsVvrItem#getPersistenceProperties()
     */
    @Override
    protected final Properties getPersistenceProperties() {
        final Properties properties = super.getPersistenceProperties();
        properties.setProperty(DELETED_KEY, Boolean.valueOf(deleted).toString());
        properties.setProperty(NRSFILE_UUID_KEY, getNrsFileId().toString());
        return properties;
    }

    @Override
    public final FutureDevice createDevice(final String name) {
        return createDevice(name, UUID.randomUUID());
    }

    @Override
    public final FutureDevice createDevice(final String name, final UUID uuid) {
        return createDevice(name, null, uuid);
    }

    @Override
    public final FutureDevice createDevice(final String name, final long size) {
        return createDevice(name, null, UUID.randomUUID(), size);
    }

    @Override
    public final FutureDevice createDevice(final String name, final UUID uuid, final long size) {
        return createDevice(name, null, uuid, size);
    }

    @Override
    public final FutureDevice createDevice(final String name, final String description) {
        return createDevice(name, description, getSize());
    }

    @Override
    public final FutureDevice createDevice(final String name, final String description, final UUID uuid) {
        return createDevice(name, description, uuid, getSize());
    }

    @Override
    public final FutureDevice createDevice(final String name, final String description, final long size) {
        return createDevice(name, description, UUID.randomUUID(), size);
    }

    @Override
    public final FutureDevice createDevice(final String name, final String description, final UUID uuid, final long size) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is null");
        }

        // Create payload
        final NrsRepository repository = getVvr();
        final UUID deviceUuid = Objects.requireNonNull(uuid);
        final UuidT<NrsFile> futureSnapshotUuid = SimpleIdentifierProvider.newId();
        final NrsFileHeader<NrsFile> deviceNrsFileHeader = repository.doCreateNrsFileHeader(getNrsFileId(), size,
                deviceUuid, futureSnapshotUuid);

        // Create and launch transaction
        final VvrRemote.RemoteOperation.Builder opBuilder = VvrRemote.RemoteOperation.newBuilder();
        NrsRemoteUtils.addNrsDevice(opBuilder, deviceNrsFileHeader, getUuid(), name, description);
        final UUID taskId = repository.submitTransaction(opBuilder, Type.DEVICE, OpCode.CREATE);

        // Get the device future
        return new NrsFutureDevice(deviceUuid, repository, taskId);
    }

    @Override
    public final byte[] export() throws OperationNotSupportedException {
        throw new OperationNotSupportedException("not yet implemented");
    }

    @Override
    public final FutureVoid delete() {
        final UUID snapshotUuid = getUuid();
        final VvrRemote.RemoteOperation.Builder opBuilder = VvrRemote.RemoteOperation.newBuilder();
        opBuilder.setUuid(VvrRemoteUtils.newUuid(snapshotUuid));
        final NrsRepository repository = getVvr();
        final UUID taskId = repository.submitTransaction(opBuilder, Type.SNAPSHOT, OpCode.DELETE);
        return new NrsFutureVoid(repository, taskId, snapshotUuid);
    }

    final void deleteSnapshot() {
        final NrsRepository repository = getVvr();
        repository.unregisterSnapshot(this.getUuid());
        this.deleted = true;
        try {
            this.persist();
        }
        catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final Date getSnapshotTime() {
        return new Date(getTimestamp());
    }

    @Override
    public final Collection<UUID> getChildrenSnapshotsUuid() {
        return getVvr().getSnapshotChildrenUuid(this.getUuid());
    }

    @Override
    public final Collection<UUID> getSnapshotDevicesUuid() {
        return getVvr().getSnapshotDevicesUuid(this.getUuid());
    }

    /**
     * Gets the deleted state of this snapshot.
     * 
     * @return the value of the deleted flag
     */
    protected final boolean isDeleted() {
        return this.deleted;
    }

    /**
     * Tells if the {@link Snapshot} is the root snapshot.
     * 
     * @return <code>true</code> for the root snapshot.
     */
    final boolean isRoot() {
        return root;
    }

    /**
     * Member builder for snapshot instances.
     * 
     * 
     */
    public static final class Builder extends NrsVvrItem.Builder implements Snapshot.Builder {
        /**
         * Source device of which the snapshot is to be taken.
         */
        private NrsDevice sourceDevice;

        @Override
        public final Snapshot.Builder device(final Device device) {
            this.sourceDevice = (NrsDevice) device;
            return this;
        }

        private NrsFileHeader<NrsFile> header;

        public final Snapshot.Builder header(final NrsFileHeader<NrsFile> header) {
            this.header = header;
            return this;
        }

        private boolean deleted;

        public final Snapshot.Builder deleted(final boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public final boolean deleted() {
            return this.deleted;
        }

        @Override
        protected final UUID deviceID() {
            return sourceDevice == null ? header.getDeviceId() : sourceDevice.getUuid();
        }

        @Override
        protected final UUID nodeID() {
            return sourceDevice == null ? header.getNodeId() : sourceDevice.getNodeId();
        }

        @Override
        protected final UuidT<NrsFile> futureID() {
            if (sourceDevice == null)
                return header.getFileId();
            else
                return sourceDevice.getNrsFileId();
        }

        @Override
        public final Snapshot build() {
            size(header.getSize());
            return new NrsSnapshot(this);
        }

        /**
         * Create a new snapshot.
         * 
         * @return the created snapshot.
         */
        public final Snapshot create() {
            // Check UUID conflict
            final NrsRepository repository = getVvr();
            {
                final UUID uuid = uuid();
                if (repository.getSnapshot(uuid) != null) {
                    throw new IllegalStateException("Failed to create snapshot, duplicate uuid=" + uuid);
                }
                if (repository.getDevice(uuid) != null) {
                    throw new IllegalStateException("Failed to create snapshot, duplicate uuid=" + uuid + " (device)");
                }
            }

            // Set NrsFile
            sourceFile(sourceDevice.getNrsFilePath());

            // Set owner ID as device ID (in deviceID())
            final NrsSnapshot result = new NrsSnapshot(this);
            try {
                result.create();
            }
            catch (final IOException e) {
                throw new IllegalStateException("Failed to create snapshot", e);
            }
            repository.preRegisterSnapshot(result);

            // New NrsFile for the source device: create from NRS template or create a new one, keeping the same size
            sourceDevice.createNewNrsFile(getNrsFileHeaderTemplate());

            repository.postRegisterSnapshot(result);
            return result;
        }

    }

    /**
     * Member builder for create the root snapshot of a new repository.
     * 
     * 
     */
    public static final class BuilderRoot extends NrsVvrItem.Builder implements Snapshot.Builder {

        @Override
        @Deprecated
        public final Snapshot.Builder device(final Device device) {
            // Must not be called: no device for the root snapshot
            throw new AssertionError();
        }

        @Override
        protected final UUID deviceID() {
            // Dummy device ID: set ownerID
            return getVvr().getOwnerId();
        }

        @Override
        protected final UUID nodeID() {
            // Current node for the root
            return getVvr().getNodeId();
        }

        @Override
        protected final UuidT<NrsFile> futureID() {
            // Same uuid has target
            return SimpleIdentifierProvider.fromUUID(uuid());
        }

        @Override
        @Deprecated
        public final Snapshot build() {
            throw new AssertionError();
        }

        /**
         * Create the root snapshot.
         * 
         * @return the created snapshot.
         */
        public final Snapshot create() {
            name(""); // Root has no name (yet)
            description(""); // Root has no description (yet)

            // Set id=parentId
            parentFile(futureID());
            size(0);

            // Create and set NrsFile
            final NrsFile nrsFile = createNrsFile(NrsFileFlag.ROOT);
            sourceFile(nrsFile);
            // Can not write the file of the root snapshot
            getVvr().getNrsFileJanitor().setNrsFileNoWritable(nrsFile);

            // Set owner ID as device ID (in method deviceID())
            final NrsSnapshot result = new NrsSnapshot(this);
            try {
                result.create();
            }
            catch (final IOException e) {
                // TODO remove NrsFile
                throw new IllegalStateException("Failed to create snapshot", e);
            }
            return result;
        }
    }
}
