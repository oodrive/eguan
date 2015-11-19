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

import static io.eguan.utils.ByteBuffers.ALLOCATE_DIRECT;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.DtxTaskInfo;
import io.eguan.hash.HashAlgorithm;
import io.eguan.ibs.Ibs;
import io.eguan.ibs.IbsErrorCode;
import io.eguan.ibs.IbsFactory;
import io.eguan.ibs.IbsIOException;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgServerRemoteStatus;
import io.eguan.net.MsgServerTimeoutException;
import io.eguan.nrs.NrsConfigurationContext;
import io.eguan.nrs.NrsException;
import io.eguan.nrs.NrsFile;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.nrs.NrsFileJanitor;
import io.eguan.nrs.NrsMsgEnhancer;
import io.eguan.nrs.NrsStorageConfigKey;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.nrs.NrsRemote.NrsFileHeaderMsg;
import io.eguan.proto.nrs.NrsRemote.NrsFileMapping;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate;
import io.eguan.proto.nrs.NrsRemote.NrsVersion;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.proto.vvr.VvrRemote.Item;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.SimpleIdentifierProvider;
import io.eguan.utils.UuidT;
import io.eguan.utils.mapper.FileMapperConfigurationContext;
import io.eguan.vvr.configuration.CommonConfigurationContext;
import io.eguan.vvr.configuration.IbsConfigurationContext;
import io.eguan.vvr.configuration.PersistenceConfigurationContext;
import io.eguan.vvr.configuration.keys.BlockSizeConfigKey;
import io.eguan.vvr.configuration.keys.DescriptionConfigkey;
import io.eguan.vvr.configuration.keys.DeviceFileDirectoryConfigKey;
import io.eguan.vvr.configuration.keys.HashAlgorithmConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpPathConfigKey;
import io.eguan.vvr.configuration.keys.NameConfigKey;
import io.eguan.vvr.configuration.keys.SnapshotFileDirectoryConfigKey;
import io.eguan.vvr.configuration.keys.StartedConfigKey;
import io.eguan.vvr.remote.VvrDtxRmContext;
import io.eguan.vvr.remote.VvrRemoteUtils;
import io.eguan.vvr.repository.core.api.AbstractRepositoryImpl;
import io.eguan.vvr.repository.core.api.Device;
import io.eguan.vvr.repository.core.api.Snapshot;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository.ItemChangedEvent.VvrItemAttributeType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * {@link NrsFile} based implementation of the repository interface.
 * <p>
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * @author pwehrle
 * 
 */
public final class NrsRepository extends AbstractRepositoryImpl {

    static final Logger LOGGER = LoggerFactory.getLogger(NrsRepository.class);

    /**
     * Name of the file containing the repository configuration.
     */
    private static final String NRS_CONFIG = "vvr.cfg";

    /**
     * Name of the file containing the previous repository configuration.
     */
    private static final String NRS_CONFIG_PREV = "vvr.cfg.bak";

    /** Block size in bytes. */
    private final int blockSize;

    /**
     * Association between the {@link NrsFile} and a {@link NrsSnapshot}. The key is the UuidT of the NrsFile, the value
     * is the UUID of the snapshot.
     */
    private BiMap<UuidT<NrsFile>, UUID> nrsToSnapshot;

    /**
     * Internal map of all snapshots.
     */
    private ConcurrentHashMap<UUID, NrsSnapshot> snapshots;

    /**
     * Internal map of all parents (NrsFile). The value is the UUID of the parent of the key.
     */
    private ConcurrentHashMap<UuidT<NrsFile>, UuidT<NrsFile>> parents;

    /**
     * Internal map of all devices.
     */
    private ConcurrentHashMap<UUID, NrsDevice> devices;

    /**
     * The item at the root of the hierarchy.
     */
    private NrsSnapshot rootItem = null;

    /**
     * The {@link Ibs} instance backing this repository.
     */
    private Ibs ibsInstance;

    /**
     * The flag indicating the operation mode.
     */
    private boolean started;

    /**
     * The configured {@link HashAlgorithm}.
     */
    private HashAlgorithm hashAlgorithm;

    /**
     * The length in bytes of the {@link #hashAlgorithm}'s results.
     */
    private int hashLength;

    /**
     * The temporary file to keep for IBS configuration.
     */
    private File targetIbsConfigFile;

    /**
     * The initialization state of this instance.
     */
    private boolean initialized;

    /**
     * The {@link NrsFileJanitor} used to construct {@link NrsVvrItem}s.
     */
    private final NrsFileJanitor nrsFileJanitor;
    private final NrsMsgEnhancer nrsMsgEnhancer = new NrsMsgEnhancer() {

        @Override
        public final void enhance(final GeneratedMessageLite.Builder<?, ?> genericBuilder) {
            final RemoteOperation.Builder remoteBuilder = (RemoteOperation.Builder) genericBuilder;
            enhanceMessage(remoteBuilder);
        }
    };

    /**
     * The directory for saving {@link NrsSnapshot}s.
     */
    private File snapshotDir;

    /**
     * The directory for saving {@link NrsDevice}s.
     */
    private File deviceDir;

    /**
     * Constructor to be invoked by builders.
     * 
     * @param builder
     *            the {@link NrsRepository.Builder} instance to build this instance from
     */
    private NrsRepository(final NrsRepository.Builder builder) {
        super(builder);
        LOGGER.trace("building new {} instance with uuid {}", NrsRepository.class.getSimpleName(), getUuid());

        final MetaConfiguration config = builder.getConfiguration();
        this.blockSize = BlockSizeConfigKey.getInstance().getTypedValue(config);

        // Initializes the NRS persistence configuration
        this.nrsFileJanitor = new NrsFileJanitor(config);
    }

    @Override
    public final void init() {
        if (initialized) {
            throw new IllegalStateException();
        }

        // initializes the IBS configuration
        this.initIbsConfiguration(false);

        // initializes the hash configuration fields
        this.initHashConfiguration();

        // initializes tree of items
        try {
            this.initNrsTree();
        }
        catch (final NrsException e) {
            throw new IllegalStateException(e);
        }

        // Inits the IBS instance. Need to start Ibs for remote messages handling
        this.ibsInstance = IbsFactory.openIbs(targetIbsConfigFile);
        this.ibsInstance.start();

        this.initialized = true;
    }

    @Override
    public final boolean isInitialized() {
        return this.initialized;
    }

    @Override
    public final void fini() {
        // TODO: add more checking and manage state better
        if (!this.initialized) {
            LOGGER.warn("Repository not initialized");
            return;
        }

        if (this.isStarted()) {
            this.stop(false);
        }

        try {
            this.ibsInstance.stop();
        }
        catch (final Throwable t) {
            LOGGER.warn("Error while stopping IBS", t);
        }

        // Clean configurations initialized during init()
        finiNrsTree();
        finiHashConfiguration();
        finiIbsConfiguration();
        finiSnapDevConfig();
        this.initialized = false;
    }

    @Override
    public final void start(final boolean saveState) {
        if (!this.isInitialized()) {
            throw new IllegalStateException("Repository not initialized");
        }
        if (started) {
            throw new IllegalStateException("Repository already started");
        }

        this.started = true;

        // Save configuration
        if (saveState) {
            saveStartedState(Boolean.TRUE);
        }
    }

    @Override
    public final boolean isStarted() {
        return this.started;
    }

    @Override
    public final void stop(final boolean saveState) {
        if (started) {
            if (!this.isInitialized()) {
                throw new IllegalStateException("repository not initialized");
            }

            this.started = false;

            // Save configuration
            if (saveState) {
                saveStartedState(Boolean.FALSE);
            }
        }
    }

    @Override
    public final void updateConfiguration(final Map<AbstractConfigKey, Object> newKeyValueMap) {
        // Update configuration
        super.updateConfiguration(newKeyValueMap);

        // Store new configuration
        final MetaConfiguration configuration = getConfiguration();
        final File repositoryPath = NrsStorageConfigKey.getInstance().getTypedValue(configuration);
        storeConfiguration(configuration, repositoryPath);
    }

    @Override
    public final NrsSnapshot getSnapshot(final UUID snapshotId) {
        return this.snapshots.get(snapshotId);
    }

    final NrsSnapshot getSnapshotFromFile(final UuidT<NrsFile> fileId) {
        final UUID snapshotId = nrsToSnapshot.get(fileId);
        return snapshotId == null ? null : getSnapshot(snapshotId);
    }

    @Override
    public final Set<UUID> getSnapshots() {
        return Collections.unmodifiableSet(this.snapshots.keySet());
    }

    @Override
    public final NrsDevice getDevice(final UUID deviceId) {
        return this.devices.get(Objects.requireNonNull(deviceId));
    }

    @Override
    public final Set<UUID> getDevices() {
        return Collections.unmodifiableSet(this.devices.keySet());
    }

    @Override
    public final Snapshot getRootSnapshot() {
        return rootItem;
    }

    // TODO not thread safe, not atomic
    @Override
    public final void delete() {
        // Started?
        if (isStarted()) {
            throw new IllegalStateException("Started");
        }

        // Flag as 'to delete'
        setDeleted();
    }

    public final Collection<UUID> getSnapshotChildrenUuid(final UUID snapshotId) {
        final Iterator<NrsSnapshot> ite = snapshots.values().iterator();
        final Collection<UUID> result = new ArrayList<>();
        while (ite.hasNext()) {
            final NrsSnapshot nrsSnapshot = ite.next();
            if (nrsSnapshot.isRoot()) {
                // Root is not a child
                continue;
            }
            if (snapshotId.equals(nrsSnapshot.getParent())) {
                result.add(nrsSnapshot.getUuid());
            }
        }
        return Collections.unmodifiableCollection(result);
    }

    public final Collection<UUID> getSnapshotDevicesUuid(final UUID snapshotId) {
        final Iterator<NrsDevice> ite = devices.values().iterator();
        final Collection<UUID> result = new ArrayList<>();
        while (ite.hasNext()) {
            final Device nrsDevice = ite.next();
            if (snapshotId.equals(nrsDevice.getParent())) {
                result.add(nrsDevice.getUuid());
            }
        }
        return Collections.unmodifiableCollection(result);
    }

    /**
     * Gets the registered parent for the given UUID.
     * 
     * @param parent
     * @return the snapshot found
     */
    final NrsSnapshot getParentSnapshot(final UuidT<NrsFile> parentNrsFile) {
        UuidT<NrsFile> loopUuidNrsFile = parentNrsFile;
        while (true) {
            final UUID snapshotUuid = nrsToSnapshot.get(loopUuidNrsFile);
            if (snapshotUuid != null) {
                final NrsSnapshot snapshot = snapshots.get(snapshotUuid);
                if (snapshot == null) {
                    throw new IllegalStateException("Snapshot not found, id=" + snapshotUuid);
                }
                return snapshot;
            }

            final UuidT<NrsFile> parentUuid = parents.get(loopUuidNrsFile);
            if (parentUuid == null) {
                throw new IllegalStateException("Parent not found, id=" + loopUuidNrsFile);
            }
            if (parentUuid.equals(loopUuidNrsFile)) {
                // Reached root
                return rootItem;
            }

            // Next loop
            loopUuidNrsFile = parentUuid;
        }
    }

    /**
     * Gets the configured {@link HashProvider.HashAlgorithm}.
     * 
     * @return the {@link HashProvider.HashAlgorithm} to apply to data blocks
     */
    final HashAlgorithm getHashAlgorithm() {
        // FIXME: this should only be necessary if initialization is not complete => hunt down the race condition and
        // exterminate it!
        if (this.hashAlgorithm == null) {
            this.initHashConfiguration();
        }
        return this.hashAlgorithm;
    }

    /**
     * Gets the {@link NrsFileJanitor} used to construct {@link NrsVvrItem}s.
     * 
     * @return the {@link NrsFileJanitor} initialized by {@link #initNrsConfiguration()}.
     */
    final NrsFileJanitor getNrsFileJanitor() {
        return nrsFileJanitor;
    }

    /**
     * Prepare the registration of a new snapshot.
     * 
     * Backing items are automatically registered.
     * 
     * @param newSnapshot
     *            the snapshot to register
     */
    final void preRegisterSnapshot(@Nonnull final NrsSnapshot newSnapshot) {
        // NPE if newSnapshot is null
        if (newSnapshot.getUuid() == null) {
            throw new IllegalArgumentException("snapshot to register has null id");
        }

        // Add NrsFile <> snapshot association
        final UuidT<NrsFile> nrsFileId = newSnapshot.getNrsFileId();
        final UuidT<NrsFile> parentFileId = newSnapshot.getParentFile();
        final UUID snapshotId = newSnapshot.getUuid();
        nrsToSnapshot.put(nrsFileId, snapshotId);

        // Stores Nrs hierarchy and snapshots
        this.parents.put(nrsFileId, parentFileId);

        if (this.snapshots.containsKey(snapshotId)) {
            return;
        }
        this.snapshots.put(snapshotId, newSnapshot);
    }

    /**
     * Last operation(s) after the update of the device.
     * 
     * @param newSnapshot
     */
    final void postRegisterSnapshot(@Nonnull final NrsSnapshot newSnapshot) {
        // Notify addition of the new snapshot
        final ItemCreatedEvent event = new ItemCreatedEvent(this, Objects.requireNonNull(newSnapshot));
        eventBus.post(event);
    }

    /**
     * Unregisters the snapshot with the given ID.
     * 
     * Backing items are not automatically unregistered.
     * 
     * @param snapshotId
     *            the ID of the snapshot to unregister
     */
    final void unregisterSnapshot(final UUID snapshotId) {
        if (snapshotId == null) {
            return;
        }
        if (getRootSnapshot().getUuid().equals(snapshotId)) {
            throw new IllegalStateException("cannot unregister root snapshot");
        }
        this.snapshots.remove(snapshotId);
        this.nrsToSnapshot.inverse().remove(snapshotId);

        // Must reset parent cache for the children (snapshots and devices). Reset all (getParent() would
        // init parent cache when it is not set...)
        for (final NrsSnapshot snapshot : snapshots.values()) {
            snapshot.resetParent();
        }
        for (final NrsDevice device : devices.values()) {
            device.resetParent();
        }

        // Notify delete
        final ItemDeletedEvent event = new ItemDeletedEvent(this, snapshotId, Snapshot.class);
        eventBus.post(event);
    }

    /**
     * Registers a new device.
     * 
     * Backing items are automatically registered.
     * 
     * @param newDevice
     *            the device to register
     */
    final void registerDevice(@Nonnull final NrsDevice newDevice) {
        if (newDevice == null) {
            throw new NullPointerException("device to register is null");
        }
        if (newDevice.getUuid() == null) {
            throw new IllegalArgumentException("device to register has null id");
        }

        if (this.devices.containsKey(newDevice.getUuid()) && this.devices.containsValue(newDevice)) {
            return;
        }

        this.devices.put(newDevice.getUuid(), newDevice);
        this.parents.put(newDevice.getNrsFileId(), newDevice.getParentFile());

        // Notify addition
        final ItemCreatedEvent event = new ItemCreatedEvent(this, newDevice);
        eventBus.post(event);
    }

    /**
     * Unregisters the device with the given ID.
     * 
     * Backing items are not automatically unregistered.
     * 
     * @param deviceId
     *            the ID of the device to unregister
     */
    final void unregisterDevice(final UUID deviceId) {
        if (deviceId == null) {
            return;
        }
        this.devices.remove(deviceId);

        // Notify delete
        final ItemDeletedEvent event = new ItemDeletedEvent(this, deviceId, Device.class);
        eventBus.post(event);
    }

    final void registerNrsFile(final UuidT<NrsFile> prevNrsFileUuid, final NrsFile nrsFile) {
        final NrsFileHeader<NrsFile> header = nrsFile.getDescriptor();

        assert prevNrsFileUuid.equals(header.getParentId());
        this.parents.put(header.getFileId(), header.getParentId());
    }

    /**
     * Gets the {@link Ibs} instance backing this repository.
     * 
     * @return the current {@link Ibs}
     */
    final Ibs getIbsInstance() {
        return this.ibsInstance;
    }

    /**
     * Gets the {@link #hashLength} value.
     * 
     * @return the {@link #hashLength} in bytes as computed by {@link #initHashConfiguration()}
     */
    final int getHashLength() {
        // FIXME: this should only be necessary if initialization is not complete => hunt down the race condition and
        // exterminate it!
        if (this.hashLength == 0) {
            this.initHashConfiguration();
        }
        return this.hashLength;
    }

    /**
     * The repository block size.
     * 
     * @return the block size
     */
    final int getBlockSize() {
        return blockSize;
    }

    final File getSnapshotDir() {
        return snapshotDir;
    }

    final File getDeviceDir() {
        return deviceDir;
    }

    /**
     * Expose method locally to send remote messages on Nrs objects.
     */
    @Override
    protected final Collection<MsgServerRemoteStatus> sendMessage(final RemoteOperation.Builder opBuilder,
            final Type type, final OpCode opCode, final boolean async, final UUID peer)
            throws MsgServerTimeoutException, InterruptedException, ConnectException {
        return super.sendMessage(opBuilder, type, opCode, async, peer);
    }

    /**
     * Expose method locally to submit transactions.
     * 
     * @see io.eguan.vvr.repository.core.api.AbstractRepositoryImpl#submitTransaction(io.eguan.proto.vvr.VvrRemote.Operation.Builder,
     *      io.eguan.proto.vvr.VvrRemote.Operation.Type,
     *      io.eguan.proto.vvr.VvrRemote.Operation.OpCode)
     */
    @Override
    protected final UUID submitTransaction(final RemoteOperation.Builder opBuilder, final Type type, final OpCode opCode) {
        return super.submitTransaction(opBuilder, type, opCode);
    }

    @Override
    public final MessageLite handleMsg(final RemoteOperation op) {
        final Type type = op.getType();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Msg op=" + op.getOp() + ", type=" + type + " uuid=" + VvrRemoteUtils.fromUuid(op.getUuid()));
        }
        if (type == Type.VVR) {
            handleMsgVvr(op);
            return null;
        }
        else if (type == Type.DEVICE) {
            handleMsgDevice(op);
            return null;
        }
        else if (type == Type.SNAPSHOT) {
            handleMsgSnapshot(op);
            return null;
        }
        else if (type == Type.NRS) {
            return handleMsgNrs(op);
        }
        else if (type == Type.IBS) {
            return handleMsgIbs(op);
        }
        else {
            throw new IllegalArgumentException("Unexpected message type=" + type);
        }
    }

    private final void handleMsgVvr(final RemoteOperation op) {
        final OpCode opCode = op.getOp();
        if (opCode == OpCode.SET) {
            if (op.hasItem()) {
                final Item item = op.getItem();
                final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();

                // TODO: name / description have already changed on the initiator node: cannot roll back!!
                final String oldName = getName();
                final String oldDescription = getDescription();
                // Name
                if (item.hasName()) {
                    final String nameNew = item.getName();
                    setNameLocal(nameNew);
                    newKeyValueMap.put(NameConfigKey.getInstance(), nameNew);
                }

                // Description
                if (item.hasDescription()) {
                    final String descriptionNew = item.getDescription();
                    setDescriptionLocal(descriptionNew);
                    newKeyValueMap.put(DescriptionConfigkey.getInstance(), descriptionNew);
                }
                if (!newKeyValueMap.isEmpty()) {
                    // Local update
                    updateConfiguration(newKeyValueMap);
                }

                // Notify change
                if (item.hasName()) {
                    eventBus.post(new ItemChangedEvent(this, getUuid(), VvrItemAttributeType.NAME, oldName, item
                            .getName()));
                }
                if (item.hasDescription()) {
                    eventBus.post(new ItemChangedEvent(this, getUuid(), VvrItemAttributeType.DESCRIPTION,
                            oldDescription, item.getDescription()));
                }
            }
        }
    }

    private final void handleMsgDevice(final RemoteOperation op) {
        final OpCode opCode = op.getOp();
        final UUID uuid = VvrRemoteUtils.fromUuid(op.getUuid());
        if (opCode == OpCode.CREATE) {
            // Need parent, NrsFile header and item (name is not empty)
            if (!op.hasSnapshot() || !(op.getNrsFileHeaderCount() == 1) || !op.hasItem() || !op.getItem().hasName()) {
                throw new IllegalArgumentException("Create device " + uuid + ": missing attribute(s)");
            }
            final NrsSnapshot parent = getSnapshot(op.getSnapshot());
            final Item item = op.getItem();
            final String name = item.getName();
            final String description = item.hasDescription() ? item.getDescription() : null;
            final NrsFileHeaderMsg nrsFileHeaderMsg = op.getNrsFileHeader(0);
            final NrsFileHeader<NrsFile> nrsFileHeader = NrsRemoteUtils.fromNrsFileHeaderMsg(nrsFileJanitor,
                    nrsFileHeaderMsg, null);
            final long size = nrsFileHeaderMsg.getSize();
            if (size < nrsFileHeaderMsg.getBlockSize()) {
                throw new IllegalArgumentException("Create device " + uuid + ":  size < "
                        + nrsFileHeaderMsg.getBlockSize());
            }
            doCreateDevice(parent, name, description, size, uuid, nrsFileHeader);
            return;
        }

        if (opCode == OpCode.CLONE) {
            final UUID origDeviceUuid = VvrRemoteUtils.fromUuid(op.getSnapshot());
            final UUID newDeviceUuid = VvrRemoteUtils.fromUuid(op.getUuid());

            // Need device NrsFile header for both devices and item (name is not empty)
            if (!op.hasSnapshot() || !(op.getNrsFileHeaderCount() == 2) || !op.hasItem() || !op.getItem().hasName()) {
                throw new IllegalArgumentException("Clone device from " + origDeviceUuid + ": missing attribute(s)");
            }
            // Note: the NrsFile of the device may have changed since the submission of the transaction: need to reset
            // the parentUuid of the header
            final NrsDevice device = getDevice(origDeviceUuid);
            final Item item = op.getItem();
            final String name = item.getName();
            final String description = item.hasDescription() ? item.getDescription() : null;
            final UuidT<NrsFile> nrsFileUuid = device.getNrsFileId();

            // Create NrsFile header for the cloned device
            {
                final NrsFileHeaderMsg newNrsFileHeaderMsg = op.getNrsFileHeader(0);
                final NrsFileHeader<NrsFile> newNrsFileHeader = NrsRemoteUtils.fromNrsFileHeaderMsg(nrsFileJanitor,
                        newNrsFileHeaderMsg, nrsFileUuid);
                device.doCloneDevice(newDeviceUuid, name, description, newNrsFileHeader);
            }

            // Create new NrsFile for the originate device
            {
                final NrsFileHeaderMsg origNrsFileHeaderMsg = op.getNrsFileHeader(1);
                final NrsFileHeader<NrsFile> origNrsFileHeader = NrsRemoteUtils.fromNrsFileHeaderMsg(nrsFileJanitor,
                        origNrsFileHeaderMsg, nrsFileUuid);
                device.newNrsFileLocal(origNrsFileHeader);
            }
            return;
        }

        // Find the device
        final NrsDevice device = devices.get(uuid);
        if (device == null) {
            throw new IllegalArgumentException("Device " + uuid + " not found");
        }
        if (opCode == OpCode.SET) {
            updateItem(device, op);

            // New header?
            if (op.getNrsFileHeaderCount() == 1) {
                // The NrsFile of the device may have changed since the submission of the transaction
                final NrsFileHeaderMsg nrsFileHeaderMsg = op.getNrsFileHeader(0);
                final NrsFileHeader<NrsFile> nrsFileHeader = NrsRemoteUtils.fromNrsFileHeaderMsg(getNrsFileJanitor(),
                        nrsFileHeaderMsg, device.getNrsFileId());
                device.newNrsFileLocal(nrsFileHeader);
            }
        }
        else if (opCode == OpCode.DELETE) {
            device.deleteDevice();
        }
    }

    private final void handleMsgSnapshot(final RemoteOperation op) {
        // Find the snapshot
        final OpCode opCode = op.getOp();
        if (opCode == OpCode.CREATE) {
            // Need device NrsFile header and item (name is not empty)
            final UUID uuidDevice = VvrRemoteUtils.fromUuid(op.getUuid());
            if (!op.hasSnapshot() || !(op.getNrsFileHeaderCount() == 1) || !op.hasItem() || !op.getItem().hasName()) {
                throw new IllegalArgumentException("Create snapshot from " + uuidDevice + ": missing attribute(s)");
            }
            // Note: the NrsFile of the device may have changed since the submission of the transaction: need to reset
            // the parentUuid of the header
            final NrsDevice device = getDevice(uuidDevice);
            final UUID uuid = VvrRemoteUtils.fromUuid(op.getSnapshot());
            final Item item = op.getItem();
            final String name = item.getName();
            final String description = item.hasDescription() ? item.getDescription() : null;
            final NrsFileHeaderMsg nrsFileHeaderMsg = op.getNrsFileHeader(0);
            final NrsFileHeader<NrsFile> nrsFileHeader = NrsRemoteUtils.fromNrsFileHeaderMsg(nrsFileJanitor,
                    nrsFileHeaderMsg, device.getNrsFileId());
            device.doCreateSnapshot(uuid, name, description, nrsFileHeader);
            return;
        }

        // Get snapshot
        final NrsSnapshot snapshot = getSnapshot(op.getUuid());
        if (opCode == OpCode.SET) {
            updateItem(snapshot, op);
        }
        else if (opCode == OpCode.DELETE) {
            snapshot.deleteSnapshot();
        }

    }

    private final MessageLite handleMsgNrs(final RemoteOperation op) {

        // Get NrsFile
        final OpCode opCode = op.getOp();

        try {
            if (opCode == OpCode.SET) {
                final UuidT<NrsFile> uuid = VvrRemoteUtils.fromUuidT(op.getUuid());
                if (!op.hasNrsFileUpdate()) {
                    throw new IllegalArgumentException("Update NRS " + uuid + ": missing attribute(s)");
                }

                // Write updates in NrsFile
                final NrsFileUpdate nrsFileUpdate = op.getNrsFileUpdate();
                final NrsFile nrsFile = nrsFileJanitor.openNrsFile(uuid, false);
                try {
                    nrsFile.handleNrsFileUpdate(nrsFileUpdate);
                }
                finally {
                    nrsFileJanitor.unlockNrsFile(nrsFile);
                }
            }
            else if (opCode == OpCode.LIST) {
                // Reply: list of NrsFiles
                final List<NrsVersion> versions = listNrsFiles();
                final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
                opBuilder.addAllNrsVersions(versions);
                return createMessageReply(opBuilder, Type.NRS, OpCode.LIST);
            }
            else if (opCode == OpCode.UPDATE) {
                final UuidT<NrsFile> uuid = VvrRemoteUtils.fromUuidT(op.getUuid());
                final NrsFileMapping mapping = op.getNrsFileMapping();
                final UUID node = VvrRemoteUtils.fromUuid(op.getSource());

                // Open the file read-only, but do not lock it (may be in use)
                final NrsFile nrsFile = nrsFileJanitor.openNrsFile(uuid, true);
                nrsFileJanitor.unlockNrsFile(nrsFile);
                nrsFile.processNrsFileSync(mapping, node);
            }
        }
        catch (final Exception e) {
            LOGGER.warn("Failed to handle NRS message " + getUuid(), e);
        }

        return null;
    }

    private final MessageLite handleMsgIbs(final RemoteOperation op) {

        final OpCode opCode = op.getOp();
        try {
            if (op.getIbsCount() < 0) {
                throw new IllegalArgumentException("Update ibs: missing attribute(s)");
            }

            if (opCode == OpCode.SET) {
                // Write value in IBS or notify put/replace
                for (final VvrRemote.Ibs ibsMsg : op.getIbsList()) {

                    final byte[] key = ibsMsg.getKey().toByteArray();

                    // Has value?
                    final ByteBuffer value;
                    if (ibsMsg.hasValue()) {
                        final byte[] valueArray = ibsMsg.getValue().toByteArray();
                        value = ByteBuffer.wrap(valueArray);
                    }
                    else {
                        value = null;
                    }
                    // Replace?
                    if (ibsMsg.hasKeyOld()) {
                        final byte[] oldKey = ibsMsg.getKeyOld().toByteArray();
                        ibsInstance.replace(oldKey, key, value);
                    }
                    else {
                        ibsInstance.put(key, value);
                    }
                }
            }
            else if (opCode == OpCode.GET) {
                // Look for the requested block (for now, only on block at a time)
                assert op.getIbsCount() == 1;

                ByteBuffer result;
                final VvrRemote.Ibs ibsMsg = op.getIbs(0);
                final byte[] key = ibsMsg.getKey().toByteArray();
                try {
                    result = ibsInstance.get(key, blockSize, ALLOCATE_DIRECT);
                }
                catch (final IbsIOException e) {
                    // Clean reply if the buffer is not found
                    if (e.getErrorCode() == IbsErrorCode.NOT_FOUND) {
                        result = null;
                    }
                    else {
                        throw e;
                    }
                }
                final VvrRemote.Ibs.Builder replyIbsBuilder = VvrRemote.Ibs.newBuilder();
                if (result != null) {
                    result.rewind();
                    replyIbsBuilder.setValue(ByteString.copyFrom(result));
                }
                final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
                opBuilder.addIbs(replyIbsBuilder);
                return createMessageReply(opBuilder, Type.IBS, OpCode.GET);
            }
        }
        catch (final Exception e) {
            LOGGER.warn("Failed to update IBS", e);
        }

        return null;
    }

    /**
     * Find the snapshot corresponding to the given uuid.
     * 
     * @param snapshotUuid
     * @return the snapshot found
     * @throws IllegalArgumentException
     *             if the snapshot is not present
     */
    private final NrsSnapshot getSnapshot(final Uuid snapshotUuid) throws IllegalArgumentException {
        final UUID uuid = VvrRemoteUtils.fromUuid(snapshotUuid);
        final NrsSnapshot snapshot = snapshots.get(uuid);
        if (snapshot == null) {
            throw new IllegalArgumentException("Snapshot " + uuid + " not found");
        }
        return snapshot;
    }

    /**
     * Update the name, the description and/or the user properties of a {@link NrsVvrItem}.
     * 
     * @param vvrItem
     * @param op
     */
    private final void updateItem(final NrsVvrItem vvrItem, final RemoteOperation op) {
        if (op.hasItem()) {
            final Item item = op.getItem();
            boolean persistNeeded = false;

            final String oldName = vvrItem.getName();
            final String oldDescription = vvrItem.getDescription();

            // Name
            if (item.hasName()) {
                vvrItem.setNameLocal(item.getName());
                persistNeeded = true;
            }

            // Description
            if (item.hasDescription()) {
                vvrItem.setDescriptionLocal(item.getDescription());
                persistNeeded = true;
            }

            // tries to persist all properties, as one has certainly changed
            if (persistNeeded) {
                try {
                    vvrItem.persist();
                }
                catch (final IOException e) {
                    LOGGER.warn("Failed to persist '" + getUuid() + "'", e);
                }
            }

            // User properties
            // Set
            {
                final int propertiesCount = item.getSetPropCount();
                if (propertiesCount > 0) {
                    assert propertiesCount % 2 == 0;
                    final List<String> propertiesList = item.getSetPropList();
                    for (int i = 0; i < propertiesCount; i += 2) {
                        vvrItem.setUserPropertiesLocal(propertiesList.get(i), propertiesList.get(i + 1));
                    }
                }
            }
            // Unset
            {
                final int propertiesCount = item.getDelPropCount();
                if (propertiesCount > 0) {
                    for (final String property : item.getDelPropList()) {
                        vvrItem.unsetUserPropertiesLocal(property);
                    }
                }
            }
            // Notify change
            if (item.hasName()) {
                eventBus.post(new ItemChangedEvent(this, vvrItem.getUuid(), VvrItemAttributeType.NAME, oldName, item
                        .getName()));
            }
            if (item.hasDescription()) {
                eventBus.post(new ItemChangedEvent(this, vvrItem.getUuid(), VvrItemAttributeType.DESCRIPTION,
                        oldDescription, item.getDescription()));
            }
        }
    }

    /**
     * List the {@link NrsFile}s of the repository.
     * 
     * @return the list of {@link NrsFile}s.
     * @throws IOException
     */
    private final List<NrsVersion> listNrsFiles() throws IOException {
        final List<NrsVersion> result = new ArrayList<>(parents.size());
        nrsFileJanitor.visitImages(new SimpleFileVisitor<Path>() {

            @Override
            public final FileVisitResult visitFile(final Path headerPath, final BasicFileAttributes attrs)
                    throws IOException {
                final File headerFile = headerPath.toFile();
                assert headerFile.isFile();

                // Get file information
                final NrsFile nrsFile = nrsFileJanitor.loadNrsFile(headerPath);
                final NrsFileHeader<NrsFile> nrsFileHeader = nrsFile.getDescriptor();
                final long version = nrsFile.getVersion();

                final NrsVersion.Builder versionBuilder = NrsVersion.newBuilder();
                versionBuilder.setUuid(VvrRemoteUtils.newTUuid(nrsFileHeader.getFileId()));
                versionBuilder.setSource(VvrRemoteUtils.newUuid(nrsFileHeader.getNodeId()));
                versionBuilder.setVersion(version);
                versionBuilder.setWritable(nrsFileJanitor.isNrsFileWritable(nrsFile));

                // Add new version
                result.add(versionBuilder.build());

                return FileVisitResult.CONTINUE;
            }
        });

        return result;
    }

    /**
     * Repository instance builder adding parameter initialization and build implementation.
     * 
     * 
     */
    public static final class Builder extends AbstractRepositoryImpl.Builder {

        /**
         * Location of the repository persistence for the load.
         */
        private File repositoryPath;

        /**
         * UUID of the root snapshot.
         */
        private UUID rootUuid;

        /**
         * Path of the repository. Necessary to load the repository and ignored for the creation.
         * 
         * @param repositoryPath
         * @return this
         */
        public final Builder repositoryPath(final File repositoryPath) {
            this.repositoryPath = repositoryPath;
            return this;
        }

        /**
         * Sets the uuid of the root snapshot.
         * 
         * @param rootUuid
         * @return this
         */
        public final Builder rootUuid(final UUID rootUuid) {
            this.rootUuid = Objects.requireNonNull(rootUuid);
            return this;
        }

        @Override
        protected MetaConfiguration getConfiguration() {
            return super.getConfiguration();
        }

        /**
         * Load an existing {@link NrsRepository}. The caller must set repositoryPath in the builder.
         * 
         * @return a new loaded {@link NrsRepository}
         */
        public final NrsRepository load() {
            if (!repositoryPath.exists()) {
                throw new IllegalStateException(repositoryPath + " does not exist");
            }
            final File[] contents = repositoryPath.listFiles();
            if (contents == null) {
                throw new IllegalStateException(repositoryPath + " is not a directory");
            }
            if (contents.length == 0) {
                throw new IllegalStateException(repositoryPath + " is empty");
            }

            // Load meta configuration
            final File config = new File(repositoryPath, NRS_CONFIG);
            final MetaConfiguration configuration;
            try {
                configuration = MetaConfiguration.newConfiguration(config, CommonConfigurationContext.getInstance(),
                        FileMapperConfigurationContext.getInstance(), IbsConfigurationContext.getInstance(),
                        NrsConfigurationContext.getInstance(), PersistenceConfigurationContext.getInstance());
                configuration(configuration);
            }
            catch (final IOException | NullPointerException | IllegalArgumentException | ConfigValidationException e) {
                throw new IllegalStateException("Failed to load configuration in '" + config + "'", e);
            }

            // Update builder according to configuration
            name(NameConfigKey.getInstance().getTypedValue(configuration));
            description(DescriptionConfigkey.getInstance().getTypedValue(configuration));

            final NrsRepository result = new NrsRepository(this);

            initSnapDevConfig(result, true);

            return result;
        }

        /**
         * Create a NrsRepository. The NRS_STORAGE must exist and be empty.
         * 
         * @return the new NrsRepository
         */
        public final NrsRepository create() {

            final MetaConfiguration configuration = getConfiguration();

            repositoryPath = NrsStorageConfigKey.getInstance().getTypedValue(configuration);

            if (!repositoryPath.exists()) {
                throw new IllegalStateException(repositoryPath + " does not exist");
            }
            final File[] contents = repositoryPath.listFiles();
            if (contents == null) {
                throw new IllegalStateException(repositoryPath + " is not a directory");
            }
            if (contents.length != 0) {
                throw new IllegalStateException(repositoryPath + " is not empty");
            }

            // Create object (init NrsConfiguration done)
            final NrsRepository result = new NrsRepository(this);
            // TODO try {... to cleanup repositoryPath on error

            initSnapDevConfig(result, false);

            // Check configurations / create IBS directories
            result.initIbsConfiguration(true);
            try {
                result.initHashConfiguration();
                try {
                    result.nrsFileJanitor.init();
                    try {

                        // Store configuration under repository persistence
                        storeConfiguration(configuration, repositoryPath);

                        // Create the root snapshot
                        final NrsSnapshot.BuilderRoot rootSnapshotBuilder = new NrsSnapshot.BuilderRoot();
                        rootSnapshotBuilder.vvr(result);
                        rootSnapshotBuilder.metadataDirectory(result.getSnapshotDir());
                        rootSnapshotBuilder.uuid(rootUuid);
                        rootSnapshotBuilder.create();
                    }
                    finally {
                        result.nrsFileJanitor.fini();
                    }
                }
                finally {
                    result.finiHashConfiguration();
                }
            }
            finally {
                result.finiIbsConfiguration();
            }
            return result;
        }

    }

    /**
     * Initializes the configuration needed for IBS operation.
     * 
     * @param <code>true</code> if the IBP and IBPGen directories must be checked for emptiness
     */
    private final void initIbsConfiguration(final boolean createIbs) {
        final MetaConfiguration config = getConfiguration();

        final ArrayList<File> configIbpPathList = IbsIbpPathConfigKey.getInstance().getTypedValue(config);

        assert (configIbpPathList != null) && (!configIbpPathList.isEmpty());

        // Fake Ibs (for unit test purpose)
        final String fileName = configIbpPathList.size() == 1 ? configIbpPathList.get(0).getName() : null;
        final boolean fake = fileName != null
                && configIbpPathList.get(0).getName().startsWith(Ibs.UNIT_TEST_IBS_HEADER);

        if (!fake) {
            // Check standard configuration
            for (final File currDir : configIbpPathList) {
                assert currDir.isDirectory();
                if (createIbs && (currDir.list().length > 0)) {
                    throw new IllegalStateException("Ibp directory '" + currDir + "' is not empty");
                }
            }

            final File configIbpGenPath = IbsIbpGenPathConfigKey.getInstance().getTypedValue(config);

            assert (configIbpGenPath != null) && (configIbpGenPath.isDirectory());
            if (createIbs && (configIbpGenPath.list().length > 0)) {
                throw new IllegalStateException("IbpGen directory '" + configIbpGenPath + "' is not empty");
            }
        }

        targetIbsConfigFile = null;
        try {
            if (fake) {
                targetIbsConfigFile = new File(fileName);
            }
            else {
                targetIbsConfigFile = File.createTempFile("vvr-ibsconfig", ".tmp");
                try (FileOutputStream outputStream = new FileOutputStream(targetIbsConfigFile)) {
                    IbsConfigurationContext.getInstance().storeIbsConfig(config, outputStream);
                }
            }

            // Create the IBS if necessary
            if (createIbs) {
                final Ibs ibs = IbsFactory.createIbs(targetIbsConfigFile);
                ibs.close();
            }
        }
        catch (final IOException ie) {
            throw new IllegalStateException("Could not write IBS configuration file " + targetIbsConfigFile, ie);
        }
    }

    private final void finiIbsConfiguration() {
        if (targetIbsConfigFile != null) {
            try {
                LOGGER.debug("deleting temporary configuration file: " + targetIbsConfigFile);
                Files.deleteIfExists(this.targetIbsConfigFile.toPath());
            }
            catch (final IOException e) {
                LOGGER.warn("failed to delete temporary IBS configuration file " + targetIbsConfigFile);
            }
            targetIbsConfigFile = null;
        }
    }

    /**
     * Initializes the {@link #hashFactory} and {@link #hashAlgorithm} fields according to configuration.
     */
    private final void initHashConfiguration() {
        this.hashAlgorithm = HashAlgorithmConfigKey.getInstance().getTypedValue(getConfiguration());

        assert hashAlgorithm != null;

        this.hashLength = hashAlgorithm.getPersistedDigestLength();
    }

    private final void finiHashConfiguration() {
        // No op
    }

    /**
     * Initializes the directories in which {@link NrsSnapshot}s and {@link NrsDevice}s are saved.
     */
    private static final void initSnapDevConfig(final NrsRepository repository, final boolean exists) {
        final MetaConfiguration config = repository.getConfiguration();
        final File baseDir = NrsStorageConfigKey.getInstance().getTypedValue(config);

        final File relSnapshotDir = SnapshotFileDirectoryConfigKey.getInstance().getTypedValue(config);
        assert relSnapshotDir != null;
        repository.snapshotDir = initDirectory(baseDir, relSnapshotDir, exists);

        final File relDeviceDir = DeviceFileDirectoryConfigKey.getInstance().getTypedValue(config);
        assert relDeviceDir != null;
        repository.deviceDir = initDirectory(baseDir, relDeviceDir, exists);

        // Check operations supported by the directory and the file system
        checkDirectory(baseDir);
    }

    private static final File initDirectory(final File baseDir, final File relPath, final boolean exists) {
        final File result = new File(baseDir, relPath.getPath());

        final boolean doesExist = result.exists();

        if (exists ^ doesExist) {
            throw new AssertionError(String.format("Storage directory '%s' does%s exist although it should%s", result,
                    (doesExist ? "" : " not"), (exists ? "" : " not")));
        }

        if (!exists && !result.mkdirs()) {
            throw new IllegalStateException("Failed to create directory '" + result + "'");
        }

        return result;
    }

    private static final void checkDirectory(final File baseDir) {
        final File temporary = new File(baseDir, ".tmpfile");
        try {
            try {
                // Fail safe: delete file if any
                io.eguan.utils.Files.deleteRecursive(temporary.toPath());

                // Can create files? (FS mounted read-write?)
                // Should not fail here, as the MetaConfiguration checks if the basedir is writable
                temporary.createNewFile();

                // Can set user defined attributes?
                io.eguan.utils.Files.checkUserAttrSupported(temporary.toPath());
            }
            catch (final IOException e) {
                throw new IllegalStateException("Invalid directory '" + baseDir.getAbsolutePath() + "'", e);
            }
        }
        finally {
            temporary.delete();
        }
    }

    private final void finiSnapDevConfig() {
        // nothing
    }

    private final void initNrsTree() throws NrsException {
        // Initialize Nrs
        nrsFileJanitor.setClientStartpoint(getMsgClientStartpoint(), nrsMsgEnhancer);
        nrsFileJanitor.init();

        // Reset previous objects
        rootItem = null;
        nrsToSnapshot = null;
        snapshots = null;
        parents = null;
        devices = null;

        // Load the NrsFile headers
        final AtomicReference<NrsSnapshot> rootItemNew = new AtomicReference<NrsSnapshot>();
        final BiMap<UuidT<NrsFile>, UUID> nrsToSnapshotNew = HashBiMap.create();
        final ConcurrentHashMap<UUID, NrsSnapshot> snapshotsNew = new ConcurrentHashMap<>();
        final ConcurrentHashMap<UuidT<NrsFile>, UuidT<NrsFile>> parentsNew = new ConcurrentHashMap<>();
        final ConcurrentHashMap<UUID, NrsDevice> devicesNew = new ConcurrentHashMap<>();
        try {

            // Load NrsFile<>NrsSnapshot associations
            Files.walkFileTree(snapshotDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public final FileVisitResult visitFile(final Path headerPath, final BasicFileAttributes attrs)
                        throws IOException {
                    final File snapshotFile = headerPath.toFile();
                    assert snapshotFile.isFile();

                    // Should be a property file, containing a snapshot or its user preferences (if any)
                    final Properties properties = new Properties();
                    try (FileInputStream fis = new FileInputStream(snapshotFile)) {
                        properties.load(fis);
                    }
                    final String uuidStr = properties.getProperty(NrsVvrItem.UUID_KEY);
                    final String nrsFileUuidStr = properties.getProperty(NrsSnapshot.NRSFILE_UUID_KEY);

                    assert (uuidStr == null && nrsFileUuidStr == null) || (uuidStr != null && nrsFileUuidStr != null);

                    if (uuidStr == null && nrsFileUuidStr == null) {
                        // Not a snapshot: ignored
                        return FileVisitResult.CONTINUE;
                    }

                    // New snapshot
                    final boolean deleted = Boolean.valueOf(properties.getProperty(NrsSnapshot.DELETED_KEY))
                            .booleanValue();
                    if (deleted) {
                        // Ignored
                        return FileVisitResult.CONTINUE;
                    }
                    final UUID uuid = UUID.fromString(uuidStr);
                    final UuidT<NrsFile> nrsFileUuid = SimpleIdentifierProvider.fromString(nrsFileUuidStr);
                    nrsToSnapshotNew.put(nrsFileUuid, uuid);

                    return FileVisitResult.CONTINUE;
                }
            });

            // Visit images
            nrsFileJanitor.visitImages(new SimpleFileVisitor<Path>() {

                @Override
                public final FileVisitResult visitFile(final Path headerPath, final BasicFileAttributes attrs)
                        throws IOException {
                    final File headerFile = headerPath.toFile();
                    assert headerFile.isFile();

                    final NrsFileHeader<NrsFile> header = nrsFileJanitor.loadNrsFileHeader(headerPath);
                    final UuidT<NrsFile> nrsFileId = header.getFileId();
                    final UUID snapshotId = nrsToSnapshotNew.get(nrsFileId);

                    // Stores hierarchy
                    parentsNew.put(nrsFileId, header.getParentId());

                    // Is root?
                    if (header.isRoot()) {
                        assert header.getParentId().equals(nrsFileId);
                        assert snapshotId != null;
                        if (rootItemNew.get() == null) {
                            final NrsSnapshot rootTmp = loadSnapshot(header, snapshotId, headerPath);
                            if (rootTmp == null) {
                                throw new NrsException("Persistence of root not found '" + snapshotId + "'");
                            }
                            assert rootTmp.isRoot();

                            // Check the UUID of the node is correct
                            if (!getNodeId().equals(rootTmp.getNodeId())) {
                                throw new NrsException("Persistence of wrong node found: '" + rootTmp.getNodeId()
                                        + "' instead of '" + getNodeId() + "'");
                            }
                            rootItemNew.set(rootTmp);
                            snapshotsNew.put(snapshotId, rootTmp);
                        }
                        else {
                            throw new NrsException("Duplicate root '" + rootItemNew.get().getUuid() + "' and '"
                                    + snapshotId + "'");
                        }
                    }
                    else if (nrsFileJanitor.isSealed(headerPath)) {
                        // Update file: set is read-only if was not done previously
                        if (headerFile.canWrite()) {
                            headerFile.setReadOnly();
                        }

                        if (snapshotId == null) {
                            // Should be an isolated item
                            LOGGER.debug("No snapshot found for '" + nrsFileId + "'");
                        }
                        else {
                            final NrsSnapshot snapshot = loadSnapshot(header, snapshotId, headerPath);
                            assert !snapshot.isRoot();
                            if (snapshot == null) {
                                // Should not happen
                                LOGGER.warn("Persistence of snapshot not found '" + snapshotId + "'");
                            }
                            else if (snapshot.isDeleted()) {
                                LOGGER.debug("Snapshot deleted '" + snapshotId + "'");
                            }
                            else {
                                snapshotsNew.put(snapshotId, snapshot);
                            }
                        }
                    }
                    else {
                        // TODO store item (current snapshot) ID in device persistence
                        final NrsDevice device = loadDevice(header, headerPath);
                        if (device == null) {
                            throw new NrsException("Persistence of device not found '" + header.getDeviceId() + "'");
                        }
                        devicesNew.put(device.getDeviceId(), device);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        }
        catch (final NrsException e) {
            throw e;
        }
        catch (final IOException e) {
            throw new NrsException(e);
        }

        // Sanity check
        rootItem = rootItemNew.get();
        if (rootItem == null) {
            throw new NrsException("root not found");
        }

        // New values loaded
        nrsToSnapshot = nrsToSnapshotNew;
        snapshots = snapshotsNew;
        parents = parentsNew;
        devices = devicesNew;
    }

    private final NrsSnapshot loadSnapshot(final NrsFileHeader<NrsFile> header, final UUID snapshotId,
            final Path nrsFilePath) {
        // Read snapshot persistence to get partial state
        final Properties persistence = NrsVvrItem.loadPersistence(snapshotDir, snapshotId);
        if (persistence == null) {
            // Item, but not a snapshot
            return null;
        }

        // Load NrsFile
        final NrsFile nrsFile = loadNrsFile(nrsFilePath);

        final NrsSnapshot.BuilderLoad builder = new NrsSnapshot.BuilderLoad();
        builder.deleted(Boolean.valueOf(persistence.getProperty(NrsSnapshot.DELETED_KEY)).booleanValue())
                .name(persistence.getProperty(NrsVvrItem.NAME_KEY))
                .description(persistence.getProperty(NrsVvrItem.DESC_KEY))
                .uuid(UUID.fromString(persistence.getProperty(NrsVvrItem.UUID_KEY)));
        builder.header(header);
        builder.sourceFile(nrsFile).vvr(this);
        builder.metadataDirectory(getSnapshotDir());
        return (NrsSnapshot) builder.build();
    }

    private final NrsDevice loadDevice(final NrsFileHeader<NrsFile> header, final Path nrsFilePath) {
        // Read item persistence to get partial state
        final Properties persistence = NrsVvrItem.loadPersistence(deviceDir, header.getDeviceId());
        if (persistence == null) {
            // Item, but not a device
            return null;
        }

        // Load NrsFile
        final NrsFile nrsFile = loadNrsFile(nrsFilePath);

        final NrsDevice.Builder builder = new NrsDevice.Builder();
        builder.name(persistence.getProperty(NrsVvrItem.NAME_KEY)).description(
                persistence.getProperty(NrsVvrItem.DESC_KEY));
        builder.header(header);
        builder.sourceFile(nrsFile).vvr(this);
        builder.metadataDirectory(getDeviceDir());
        return (NrsDevice) builder.build();
    }

    private final NrsFile loadNrsFile(final Path nrsFilePath) {
        try {
            return nrsFileJanitor.loadNrsFile(nrsFilePath);
        }
        catch (final NrsException e) {
            throw new IllegalStateException("Could not load persistent file '" + nrsFilePath + "' for " + getUuid(), e);
        }
    }

    private final void finiNrsTree() {
        snapshots = null;
        parents = null;
        devices = null;
        rootItem = null;
        nrsFileJanitor.fini();
    }

    /**
     * Save the started flag according to the started argument.
     * 
     * @param started
     */
    private final void saveStartedState(final Boolean started) {
        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
        newKeyValueMap.put(StartedConfigKey.getInstance(), started);
        updateConfiguration(newKeyValueMap);
    }

    /**
     * Store the given configuration in the VVR directory.
     * 
     * @param configuration
     * @param repositoryPath
     */
    private final static void storeConfiguration(final MetaConfiguration configuration, final File repositoryPath) {
        // Store configuration under repository persistence
        final File config = new File(repositoryPath, NRS_CONFIG);
        final File configPrev = new File(repositoryPath, NRS_CONFIG_PREV);
        try {
            configuration.storeConfiguration(config, configPrev, true);
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to write configuration in '" + config + "'", e);
        }
    }

    /**
     * Build a {@link NrsFileHeader} for a {@link NrsFile} to create.
     * 
     * @param parentFileId
     *            {@link UuidT} of the parent of the {@link NrsFile} to create.
     * @param size
     * @param deviceUuid
     * @param futureFileUuid
     * @return the device file header
     */
    final NrsFileHeader<NrsFile> doCreateFutureNrsFileHeader(@Nonnull final UuidT<NrsFile> parentFileId,
            final long size, @Nonnull final UUID deviceUuid, @Nonnull final UuidT<NrsFile> futureFileUuid) {
        final NrsDevice.Builder builder = newDeviceBuilder(parentFileId, null, null, size, deviceUuid);
        builder.futureFileId(futureFileUuid);
        return builder.createDefaultNrsFileHeader();
    }

    /**
     * Create a device from the given snapshot.
     * 
     * @param parent
     * @param name
     * @param description
     * @param size
     * @param uuid
     *            uuid of the device
     * @param nrsFileHeader
     *            file header template
     * @return the new Device
     */
    final NrsDevice doCreateDevice(@Nonnull final NrsSnapshot parent, final String name, final String description,
            final long size, @Nonnull final UUID uuid, @Nonnull final NrsFileHeader<NrsFile> nrsFileHeader) {
        final NrsDevice.Builder builder = newDeviceBuilder(parent.getNrsFileId(), name, description, size, uuid);
        return builder.create(nrsFileHeader);
    }

    final NrsDevice.Builder newDeviceBuilder(@Nonnull final UuidT<NrsFile> parentFileId, final String name,
            final String description, final long size, @Nonnull final UUID uuid) {
        final NrsDevice.Builder builder = new NrsDevice.Builder();
        builder.size(size).name(name).description(description);
        builder.vvr(this).parentFile(Objects.requireNonNull(parentFileId));
        builder.metadataDirectory(getDeviceDir());
        builder.nodeID(getNodeId());
        builder.uuid(Objects.requireNonNull(uuid));
        return builder;
    }

    @Override
    public final UUID getId() {
        return getUuid();
    }

    @Override
    public final DtxResourceManagerContext start(final byte[] payload) throws XAException, NullPointerException {
        try {
            return VvrRemoteUtils.createDtxContext(getUuid(), payload);
        }
        catch (final InvalidProtocolBufferException e) {
            LOGGER.error("Exception on start", e);
            final XAException xaException = new XAException(XAException.XAER_INVAL);
            xaException.initCause(xaException);
            throw xaException;
        }
    }

    @Override
    public final Boolean prepare(final DtxResourceManagerContext context) throws XAException {
        final VvrDtxRmContext vvrDtxRmContext = (VvrDtxRmContext) context;
        // TODO handle errors
        try {
            handleMsg(vvrDtxRmContext.getOperation());
            return Boolean.TRUE;
        }
        catch (final IllegalStateException e) {
            // Most of the time, a pre-condition error
            LOGGER.error("Exception on prepare", e);
            final XAException xaException = new XAException(XAException.XA_RBROLLBACK);
            xaException.initCause(e);
            throw xaException;
        }
    }

    @Override
    public final void commit(final DtxResourceManagerContext context) throws XAException {
        // TODO real commit
    }

    @Override
    public final void rollback(final DtxResourceManagerContext context) throws XAException {
        // TODO real rollback
    }

    @Override
    public final void processPostSync() throws Exception {
        // Update NrsFiles
        // - Get status of remote nodes
        final RemoteOperation.Builder builder = RemoteOperation.newBuilder();
        final Collection<MsgServerRemoteStatus> status = sendMessage(builder, Type.NRS, OpCode.LIST, false, null);
        if (status == null || status.isEmpty()) {
            // Stand alone mode or alone
            return;
        }
        // Get the version of NrsFile on each node
        final Map<UUID, Map<UuidT<NrsFile>, NrsVersion>> remoteVersions = extractNrsVersions(status);
        // Get local versions
        final List<NrsVersion> localVersions = listNrsFiles();
        for (int i = localVersions.size() - 1; i >= 0; i--) {
            final NrsVersion nrsVersion = localVersions.get(i);
            // Save some memory
            localVersions.set(i, null);
            processNrsFileCheck(nrsVersion, remoteVersions);
        }

        // If the transactions are handled correctly, there should not have remote NrsFile not found locally
        for (final Map.Entry<UUID, Map<UuidT<NrsFile>, NrsVersion>> entry : remoteVersions.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                // Must replay some transaction(s)
                throw new NrsException("Extra NrsFile on node '" + entry.getKey() + "'");
            }
        }
    }

    /**
     * Build a map that contains the version of the {@link NrsFile}s for each remote node.
     * 
     * @param status
     * @return {@link NrsFile} version for each node
     * @throws InvalidProtocolBufferException
     */
    private final Map<UUID, Map<UuidT<NrsFile>, NrsVersion>> extractNrsVersions(
            final Collection<MsgServerRemoteStatus> status) throws InvalidProtocolBufferException {
        final Map<UUID, Map<UuidT<NrsFile>, NrsVersion>> result = new HashMap<>();
        for (final Iterator<MsgServerRemoteStatus> iterator = status.iterator(); iterator.hasNext();) {
            final MsgServerRemoteStatus msgServerRemoteStatus = iterator.next();
            final RemoteOperation reply = RemoteOperation.parseFrom(msgServerRemoteStatus.getReplyBytes());
            assert reply.getOp() == OpCode.LIST;
            assert reply.getType() == Type.NRS;
            assert reply.getNrsVersionsCount() > 0;
            final List<NrsVersion> versions = reply.getNrsVersionsList();
            final Map<UuidT<NrsFile>, NrsVersion> versionsRes = new HashMap<>();
            for (int i = versions.size() - 1; i >= 0; i--) {
                final NrsVersion nrsVersion = versions.get(i);
                final UuidT<NrsFile> uuidT = VvrRemoteUtils.fromUuidT(nrsVersion.getUuid());
                versionsRes.put(uuidT, nrsVersion);
            }
            result.put(msgServerRemoteStatus.getNodeId(), versionsRes);
        }
        return result;
    }

    private final void processNrsFileCheck(final NrsVersion nrsVersion,
            final Map<UUID, Map<UuidT<NrsFile>, NrsVersion>> remoteVersions) throws IllegalStateException, IOException,
            MsgServerTimeoutException, InterruptedException {
        final long version = nrsVersion.getVersion();
        final UuidT<NrsFile> nrsFileUuid = VvrRemoteUtils.fromUuidT(nrsVersion.getUuid());

        // Look for a more recent remote file
        NrsVersion max = null;
        UUID maxNodeUuid = null;
        long versionMax = version;
        final AtomicReference<Boolean> writable = new AtomicReference<>();
        for (final Map.Entry<UUID, Map<UuidT<NrsFile>, NrsVersion>> entry : remoteVersions.entrySet()) {
            final Map<UuidT<NrsFile>, NrsVersion> value = entry.getValue();
            final NrsVersion remoteVersion = value.remove(nrsFileUuid); // remove to save memory
            if (remoteVersion != null) {
                if (!remoteVersion.getWritable()) {
                    writable.set(Boolean.FALSE);
                }
                final long versionFile = remoteVersion.getVersion();
                if (versionFile > versionMax) {
                    max = remoteVersion;
                    versionMax = versionFile;
                    maxNodeUuid = entry.getKey();
                }
            }
        }

        if (max != null) {
            updateNrsFile(nrsFileUuid, maxNodeUuid, max);
        }
        else {
            // Check if the file should be read-only
            if (writable.get() != null) {
                assert writable.get() == Boolean.FALSE;
                final NrsFile nrsFile = nrsFileJanitor.loadNrsFile(nrsFileUuid);
                // File should not be in use
                nrsFileJanitor.flushNrsFile(nrsFile);
                nrsFileJanitor.setNrsFileNoWritable(nrsFile);
            }
        }
    }

    /**
     * Update a file from the contents of a remote node.
     * 
     * @param fileUuid
     *            UUID of the {@link NrsFile} to update
     * @param nodeUuid
     *            remote node
     * @param nrsVersion
     *            remote version information
     * @return <code>true</code> if the update of the file was aborted
     * @throws IOException
     * @throws IllegalStateException
     * @throws InterruptedException
     * @throws MsgServerTimeoutException
     */
    private final boolean updateNrsFile(final UuidT<NrsFile> fileUuid, final UUID nodeUuid, final NrsVersion nrsVersion)
            throws IllegalStateException, IOException, MsgServerTimeoutException, InterruptedException {
        final NrsFile nrsFile = nrsFileJanitor.loadNrsFile(fileUuid);
        // Prefer update from the creator of the file, if it's online
        final UUID nrsFileCreator = nrsFile.getDescriptor().getNodeId();
        final MsgClientStartpoint clientStartpoint = getMsgClientStartpoint();
        final UUID peer;
        if (clientStartpoint.isPeerConnected(nrsFileCreator)) {
            peer = nrsFileCreator;
        }
        else {
            peer = nodeUuid;
        }

        final boolean aborted;
        nrsFileJanitor.prepareNrsFileUpdate(nrsFile, nrsVersion);
        try {
            final RemoteOperation.Builder builder = nrsFile.getFileMapping(HashAlgorithm.TIGER);
            // Set Uuid of the NrsFile
            builder.setUuid(VvrRemoteUtils.newTUuid(nrsFile.getDescriptor().getFileId()));
            sendMessage(builder, Type.NRS, OpCode.UPDATE, false, peer);
        }
        finally {
            aborted = nrsFileJanitor.endNrsFileUpdate(nrsFile, nrsVersion);
        }
        return aborted;
    }

    @Override
    public final DtxTaskInfo createTaskInfo(final byte[] payload) {
        try {
            final RemoteOperation operation = RemoteOperation.parseFrom(payload);
            return createVvrTaskInfo(getUuid(), operation);
        }
        catch (final InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * Constructs information for vvr task.
     * 
     * @param resourceId
     *            The globally unique ID of the resourceId
     * @param operation
     *            The complete operation used to construct the task info
     */

    private final VvrTaskInfo createVvrTaskInfo(final UUID resourceId, final RemoteOperation operation) {
        VvrTaskOperation op;
        VvrTargetType targetType;
        String targetId;
        final String source = VvrRemoteUtils.fromUuid(operation.getSource()).toString();
        switch (operation.getType()) {
        case SNAPSHOT:
            targetType = VvrTargetType.SNAPSHOT;
            targetId = VvrRemoteUtils.fromUuid(operation.getSnapshot()).toString();
            break;
        case DEVICE:
            targetType = VvrTargetType.DEVICE;
            targetId = VvrRemoteUtils.fromUuid(operation.getUuid()).toString();
            break;
        case VVR:
            targetType = VvrTargetType.VVR;
            targetId = VvrRemoteUtils.fromUuid(operation.getUuid()).toString();
            break;
        default:
            throw new AssertionError("type=" + operation.getType());
        }
        switch (operation.getOp()) {
        case CREATE:
            op = VvrTaskOperation.CREATE;
            break;
        case DELETE:
            op = VvrTaskOperation.DELETE;
            break;
        case SET:
            op = VvrTaskOperation.SET;
            break;
        case CLONE:
            op = VvrTaskOperation.CLONE;
            break;
        default:
            throw new AssertionError("type=" + operation.getOp());
        }
        return new VvrTaskInfo(source, op, targetType, targetId);

    }
}
