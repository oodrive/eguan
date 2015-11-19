package io.eguan.vvr.persistence.repository;

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

import io.eguan.nrs.NrsFile;
import io.eguan.nrs.NrsFileFlag;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.Type;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.utils.SimpleIdentifierProvider;
import io.eguan.utils.UuidT;
import io.eguan.vvr.remote.VvrRemoteUtils;
import io.eguan.vvr.repository.core.api.FutureDevice;
import io.eguan.vvr.repository.core.api.FutureVoid;
import io.eguan.vvr.repository.core.api.Snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.naming.OperationNotSupportedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

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
    private NrsSnapshot(final NrsSnapshot.BuilderLoad builder) {
        super(builder);
        LOGGER.trace("Load new {} instance with uuid {}", NrsSnapshot.class.getSimpleName(), this.getUuid());
        this.deleted = builder.deleted();
        this.root = getNrsFilePath().getDescriptor().isRoot();
    }

    /**
     * Builder constructor called by {@link NrsSnapshot.Builder}s.
     * 
     * @param builder
     *            the configured builder
     */
    private NrsSnapshot(final NrsSnapshot.BuilderCreate builder) {
        super(builder);
        LOGGER.trace("Create new {} instance with uuid {}", NrsSnapshot.class.getSimpleName(), this.getUuid());
        this.deleted = false;
        this.root = getNrsFilePath().getDescriptor().isRoot();
    }

    /**
     * Builder for the root snapshot of a {@link NrsRepository}.
     * 
     * @param builder
     */
    private NrsSnapshot(final NrsSnapshot.BuilderRoot builder) {
        super(builder);
        LOGGER.trace("Create root with uuid {}", this.getUuid());
        this.root = getNrsFilePath().getDescriptor().isRoot();
        assert root == true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.vvr.persistence.repository.NrsVvrItem#getPersistenceProperties()
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
        final NrsFileHeader<NrsFile> deviceNrsFileHeader = repository.doCreateFutureNrsFileHeader(getNrsFileId(), size,
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
     * Load a snapshot from the persistence.
     */
    public static final class BuilderLoad extends NrsVvrItem.Builder {
        private NrsFileHeader<NrsFile> header;

        public final Snapshot.Builder header(final @Nonnull NrsFileHeader<NrsFile> header) {
            this.header = Objects.requireNonNull(header);
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
            return header.getDeviceId();
        }

        @Override
        protected final UUID nodeID() {
            return header.getNodeId();
        }

        @Override
        protected final UuidT<NrsFile> futureID() {
            return header.getFileId();
        }

        public final NrsSnapshot build() {
            size(header.getSize());
            flags(header.getFlags());
            return new NrsSnapshot(this);
        }
    }

    /**
     * Take a new snapshot of a device.
     */
    public static final class BuilderCreate extends NrsVvrItem.Builder {
        private NrsDevice sourceDevice;

        /**
         * Source device of which the snapshot will be taken.
         * <p>
         * Mandatory.
         */
        public final Snapshot.Builder device(final @Nonnull NrsDevice device) {
            this.sourceDevice = Objects.requireNonNull(device);

            // Inherit size and flags from the device
            size(device.getSize());
            flags(device.getNrsFilePath().getDescriptor().getFlags());
            return this;
        }

        private NrsFileHeader<NrsFile> deviceFutureNrsFileHeader;

        /**
         * Sets the header of the future {@link NrsFile} of the device.
         * 
         * @param nrsFileHeader
         * @return this
         */
        protected final NrsVvrItem.Builder deviceFutureNrsFileHeader(final @Nonnull NrsFileHeader<NrsFile> nrsFileHeader) {
            this.deviceFutureNrsFileHeader = Objects.requireNonNull(nrsFileHeader);
            return this;
        }

        @Override
        protected final UUID deviceID() {
            return sourceDevice.getUuid();
        }

        @Override
        protected final UUID nodeID() {
            return sourceDevice.getNodeId();
        }

        @Override
        protected final UuidT<NrsFile> futureID() {
            return sourceDevice.getNrsFileId();
        }

        /**
         * Create a new snapshot.
         * 
         * @return the created snapshot.
         */
        public final NrsSnapshot create() {
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

            // Creates the snapshot
            final NrsSnapshot result = new NrsSnapshot(this);
            try {
                result.create();
            }
            catch (final IOException e) {
                throw new IllegalStateException("Failed to create snapshot", e);
            }
            repository.preRegisterSnapshot(result);

            // New NrsFile for the source device: create from NRS template
            sourceDevice.createNewNrsFile(deviceFutureNrsFileHeader);

            repository.postRegisterSnapshot(result);
            return result;
        }

    }

    /**
     * Member builder for create the root snapshot of a new repository.
     * 
     * 
     */
    public static final class BuilderRoot extends NrsVvrItem.Builder {

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
            // Same uuid has the root snapshot
            return SimpleIdentifierProvider.fromUUID(uuid());
        }

        /**
         * Create the root snapshot.
         * 
         * @return the created snapshot.
         */
        public final NrsSnapshot create() {
            name(""); // Root has no name (yet)
            description(""); // Root has no description (yet)

            // Set id=parentId and force size to 0
            parentFile(futureID());
            size(0);
            final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
            flags.add(NrsFileFlag.ROOT);
            flags(flags);

            // Create and set NrsFile
            final NrsFile nrsFile = createNrsFile(null);
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
                throw new IllegalStateException("Failed to create root snapshot", e);
            }
            return result;
        }
    }
}
