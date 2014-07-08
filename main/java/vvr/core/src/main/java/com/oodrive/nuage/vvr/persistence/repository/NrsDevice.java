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
import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.oodrive.nuage.hash.ByteBufferDigest;
import com.oodrive.nuage.ibs.Ibs;
import com.oodrive.nuage.ibs.IbsIOException;
import com.oodrive.nuage.net.MsgServerRemoteStatus;
import com.oodrive.nuage.net.MsgServerTimeoutException;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.nrs.NrsFileFlag;
import com.oodrive.nuage.nrs.NrsFileHeader;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.Common.Uuid;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.utils.SimpleIdentifierProvider;
import com.oodrive.nuage.utils.UuidT;
import com.oodrive.nuage.vvr.remote.VvrRemoteUtils;
import com.oodrive.nuage.vvr.repository.core.api.AbstractDeviceImplHelper;
import com.oodrive.nuage.vvr.repository.core.api.Device;
import com.oodrive.nuage.vvr.repository.core.api.FutureSnapshot;
import com.oodrive.nuage.vvr.repository.core.api.FutureVoid;
import com.oodrive.nuage.vvr.repository.core.api.Snapshot;

/**
 * {@link NrsFile} based device implementation relying essentially on superclass methods.
 * <p>
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * @author pwehrle
 * 
 */
public final class NrsDevice extends NrsVvrItem implements Device {

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsDevice.class);

    final class NrsDeviceImplHelper extends AbstractDeviceImplHelper {

        NrsDeviceImplHelper() {
            super();
        }

        @Override
        protected final long getSize() {
            return NrsDevice.this.getSize();
        }

        @Override
        protected final BlockKeyLookupEx lookupBlockKeyEx(final long blockIndex, final boolean recursive)
                throws IOException {
            if (!NrsDevice.this.isActive()) {
                throw new IllegalStateException("Device is deactivated");
            }
            return (BlockKeyLookupEx) readHash(blockIndex, recursive, true);
        }

        @Override
        protected final void writeBlockKey(final long blockIndex, final byte[] key) throws IOException {
            if (!NrsDevice.this.isActive()) {
                throw new IllegalStateException("Device is deactivated");
            }
            writeBlockHash(blockIndex, key);
        }

        @Override
        protected final void resetBlockKey(final long blockIndex) throws IOException {
            if (!NrsDevice.this.isActive()) {
                throw new IllegalStateException("Device is deactivated");
            }
            resetBlockHash(blockIndex);
        }

        @Override
        protected final void trimBlockKey(final long blockIndex) throws IOException {
            if (!NrsDevice.this.isActive()) {
                throw new IllegalStateException("Device is deactivated");
            }
            trimBlockHash(blockIndex);
        }

        @Override
        protected final Lock getIoLock() {
            return ioLock.readLock();
        }

        @Override
        protected final void notifyIO(@Nonnull final RemoteOperation.Builder opBuilder) {
            try {
                // Send message
                getVvr().sendMessage(opBuilder, Type.IBS, OpCode.SET, true, null);
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to notify peers for device '" + getUuid() + "'", t);
            }
        }

        @Override
        protected final ByteString getRemoteBuffer(final byte[] key, final UUID srcNode) throws InterruptedException {
            // Get block request
            final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
            // Fill IBS info
            final VvrRemote.Ibs.Builder nrsBuilder = VvrRemote.Ibs.newBuilder();
            nrsBuilder.setKey(ByteString.copyFrom(key));
            opBuilder.addIbs(nrsBuilder);

            // First, ask the node that create the NrsFile
            {
                try {
                    final Collection<MsgServerRemoteStatus> reply = getVvr().sendMessage(opBuilder, Type.IBS,
                            OpCode.GET, false, srcNode);
                    if (reply == null) {
                        // Stand alone mode: no remote buffer
                        return null;
                    }

                    assert reply.size() == 1;
                    final ByteString block = getRemoteBufferFromPeer(key, reply);
                    // Block found?
                    if (block != null) {
                        return block;
                    }
                }
                catch (ConnectException | MsgServerTimeoutException | IllegalArgumentException e) {
                    LOGGER.debug("Device " + getUuid() + ": failed to get Ibs block from " + srcNode, e);
                }
            }

            // Read from any peer
            {
                try {
                    final Collection<MsgServerRemoteStatus> reply = getVvr().sendMessage(opBuilder, Type.IBS,
                            OpCode.GET, false, null);
                    final ByteString block = getRemoteBufferFromPeer(key, reply);
                    // Block found?
                    if (block != null) {
                        return block;
                    }
                }
                catch (final MsgServerTimeoutException e) {
                    LOGGER.warn("Device " + getUuid() + ": failed to get Ibs block", e);
                }
                catch (ConnectException | IllegalArgumentException e) {
                    // Should not occur
                    LOGGER.error("Device " + getUuid() + ": failed to get Ibs block", e);
                }
            }

            return null;
        }

        /**
         * Get the requested block from the given reply.
         * 
         * @param reply
         * @return the block found or <code>null</code>
         */
        private final ByteString getRemoteBufferFromPeer(final byte[] key, final Collection<MsgServerRemoteStatus> reply) {
            final Iterator<MsgServerRemoteStatus> ite = reply.iterator();
            while (ite.hasNext()) {
                final MsgServerRemoteStatus status = ite.next();
                final String exceptionName = status.getExceptionName();
                if (exceptionName != null) {
                    LOGGER.debug("Device " + getUuid() + ": failed to get Ibs block from " + status.getNodeId()
                            + ", cause=" + exceptionName);
                }
                else {
                    try {
                        final ByteString msgReply = status.getReplyBytes();
                        final RemoteOperation opReply = RemoteOperation.parseFrom(msgReply, null);
                        assert opReply.getIbsCount() == 1;
                        final ByteString block = opReply.getIbs(0).getValue();
                        // Block found?
                        if (block != null) {
                            // Check block integrity
                            if (checkRemoteBlock(key, block)) {
                                return block;
                            }
                        }
                    }
                    catch (final InvalidProtocolBufferException e) {
                        LOGGER.warn("Device " + getUuid() + ": failed to get Ibs block from " + status.getNodeId(), e);
                    }
                }
            }
            return null;
        }

        /**
         * Checks if the block is correct. If true, try to add it in the local {@link Ibs}.
         * 
         * @param key
         * @param block
         * @return <code>true</code> if the block is correct
         */
        private final boolean checkRemoteBlock(final byte[] key, final ByteString block) {
            assert block.asReadOnlyByteBuffer().capacity() == getBlockSize();
            try {
                if (ByteBufferDigest.match(block, key)) {
                    // Can add this block in Ibs
                    try {
                        getVvr().getIbsInstance().put(key, block);
                    }
                    catch (RuntimeException | IbsIOException e) {
                        LOGGER.warn("Device " + getUuid() + ": failed add the block in Ibs", e);
                    }
                    return true;
                }
            }
            catch (RuntimeException | NoSuchAlgorithmException e) {
                LOGGER.warn("Device " + getUuid() + ": failed to check block integrity", e);
            }
            return false;
        }
    }

    /**
     * The device's active status.
     */
    private volatile boolean active = false;
    /** Impl helper. Not <code>null</code> when activated */
    private NrsDeviceImplHelper deviceImplHelper;

    /**
     * This instance's lock for reading/writing raw data.
     */
    private final ReadWriteLock ioLock = new ReentrantReadWriteLock();

    /**
     * Device constructor to be invoked by builders.
     * 
     * @param builder
     *            the configured builder
     */
    private NrsDevice(final Builder builder) {
        super(builder);
    }

    @Override
    public final FutureVoid setSize(@Nonnegative final long size) {
        final UUID taskId = newNrsFile(size, false);
        return new NrsFutureVoid(getVvr(), taskId, getDeviceId());
    }

    /**
     * Create a new {@link NrsFile} for the device.
     * 
     * @param size
     *            the new size
     * @param force
     *            force the creation of the new file, even if the size does not change
     * @return the UUID of a task or <code>null</code>
     */
    private final UUID newNrsFile(@Nonnegative final long size, final boolean force) {
        // Check size
        if (size <= 0) {
            throw new IllegalArgumentException();
        }

        if (!force && size == getSize()) {
            // No change
            return null;
        }

        // Create payload
        final NrsRepository repository = getVvr();
        final UuidT<NrsFile> parentFileUuid = getNrsFileId();
        final UUID deviceUuid = getUuid();
        final UuidT<NrsFile> futureSnapshotUuid = SimpleIdentifierProvider.newId();
        final NrsFileHeader<NrsFile> deviceNrsFileHeader = repository.doCreateNrsFileHeader(parentFileUuid, size,
                deviceUuid, futureSnapshotUuid);

        // Create and launch transaction
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
        NrsRemoteUtils.setNrsDeviceNrsHeader(opBuilder, this, deviceNrsFileHeader);
        return repository.submitTransaction(opBuilder, Type.DEVICE, OpCode.SET);
    }

    /**
     * Create a new {@link NrsFile} for the device.
     * 
     * @param nrsFileHeader
     *            the NRS file header template, may be <code>null</code>
     */
    final void newNrsFileLocal(@Nonnull final NrsFileHeader<NrsFile> nrsFileHeader) {
        // Freeze IOs while resizing
        ioLock.writeLock().lock();
        try {
            final boolean wasOpened = isNrsFileLocked();
            if (wasOpened) {
                // Close the file
                closeNrsFile(true);
            }

            // New NrsFile
            createNewNrsFile(nrsFileHeader);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Device " + getDeviceId() + " new file " + nrsFileHeader.getFileId() + " (size="
                        + nrsFileHeader.getSize() + ")");
            }

            // Need to open new file?
            // Note: must not re-open the previous file on error (is read-only)
            if (wasOpened) {
                try {
                    openNrsFile();
                }
                catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        finally {
            ioLock.writeLock().unlock();
        }
    }

    /*
     * Forbid null and empty name.
     * 
     * @see com.oodrive.nuage.vvr.repository.core.api.AbstractUniqueVvrObject#setName(java.lang.String)
     */
    @Override
    public final FutureVoid setName(final String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Name is null or empty");
        }
        return super.setName(name);
    }

    @Override
    public final boolean isActive() {
        return this.active;
    }

    @Override
    public final FutureVoid activate() {
        // TODO: add more checks and locking
        if (active) {
            throw new IllegalStateException("Active");
        }
        if (isNrsFileLocked()) {
            throw new IllegalStateException("Opened");
        }

        // Create a new NrsFile if the device was activated on another node
        // TODO creates new NrsFile even if the file is activated read-only!!
        final FutureVoid futureVoid;
        final NrsRepository repository = getVvr();
        final UUID nodeUuid = repository.getNodeId();
        if (!nodeUuid.equals(getNodeId())) {
            final UUID taskId = newNrsFile(getSize(), true);

            // TODO create a local task to open the new NrsFile in background
            futureVoid = new NrsFutureVoid(repository, taskId, getDeviceId());
            try {
                futureVoid.get();
            }
            catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        }
        else {
            // No task
            futureVoid = new NrsFutureVoid(null, null, null);
        }

        this.active = true;
        this.deviceImplHelper = new NrsDeviceImplHelper();
        try {
            openNrsFile();
        }
        catch (final IOException e) {
            throw new IllegalStateException(e);
        }

        return futureVoid;
    }

    @Override
    public final FutureVoid deactivate() {
        // TODO: add more checks and locking

        // TODO create a local task to close the NrsFile in background
        closeNrsFile();

        this.active = false;
        this.deviceImplHelper = null;
        return new NrsFutureVoid(null, null, null);
    }

    /** Date formatter to create the default name */
    private static final DateFormat iso8601DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");

    @Override
    public final FutureSnapshot createSnapshot() {
        return createSnapshot(UUID.randomUUID());
    }

    @Override
    public final FutureSnapshot createSnapshot(final String name) {
        return createSnapshot(name, null, UUID.randomUUID());
    }

    @Override
    public final FutureSnapshot createSnapshot(final UUID uuid) {
        // Default name: device name+date ISO 8601
        final String now;
        // format() is not thread safe
        synchronized (iso8601DateFormat) {
            now = iso8601DateFormat.format(new Date());
        }
        return createSnapshot(getName() + " " + now, null, uuid);
    }

    @Override
    public final FutureSnapshot createSnapshot(final String name, final UUID uuid) {
        return createSnapshot(name, null, uuid);
    }

    @Override
    public final FutureSnapshot createSnapshot(final String name, final String description) {
        return createSnapshot(name, description, UUID.randomUUID());
    }

    @Override
    public final FutureSnapshot createSnapshot(final String name, final String description, final UUID createdSnapshotId) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is null or empty");
        }

        // Create payload
        final NrsRepository repository = getVvr();
        // Create future NRS file header of the device
        final UuidT<NrsFile> futureFileUuid = SimpleIdentifierProvider.newId();
        // Note: the parent fileId set is the current ID, but if there is another transaction pending
        // or in progress on this device, the parent may have changed when the transaction will be run
        final NrsFileHeader<NrsFile> deviceNrsFileHeader = repository.doCreateNrsFileHeader(getNrsFileId(), getSize(),
                getUuid(), futureFileUuid);

        // Create and launch transaction
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
        NrsRemoteUtils.addNrsDevice(opBuilder, deviceNrsFileHeader, createdSnapshotId, name, description);
        final UUID taskId = repository.submitTransaction(opBuilder, Type.SNAPSHOT, OpCode.CREATE);

        // Get the snapshot future
        return new NrsFutureSnapshot(createdSnapshotId, repository, taskId);
    }

    /**
     * Create a new snapshot for this device.
     * 
     * @param uuid
     * @param name
     * @param description
     * @param nrsFileHeader
     *            the NRS file header template, may be <code>null</code>
     * @param notifyPeers
     *            <code>true</code> if the peers must be notified
     * @return the new snapshot
     */
    final Snapshot doCreateSnapshot(final UUID uuid, final String name, final String description,
            final NrsFileHeader<NrsFile> nrsFileHeader) {

        // Freeze IOs while taking a snapshot
        ioLock.writeLock().lock();
        try {
            final boolean wasOpened = isNrsFileLocked();
            if (wasOpened) {
                // Close the file
                closeNrsFile(true);
            }

            final NrsRepository repository = getVvr();
            // Take snapshot
            final NrsSnapshot.Builder builder = new NrsSnapshot.Builder();
            builder.uuid(uuid).name(name).description(description);
            builder.device(this);
            builder.vvr(repository);
            builder.directory(repository.getSnapshotDir());
            builder.nrsFileHeader(nrsFileHeader);
            final Snapshot snapshot = builder.create();
            LOGGER.debug("Snapshot " + snapshot.getUuid() + " created");

            // Need to open new file?
            // Note: must not re-open the previous file on error (is read-only)
            if (wasOpened) {
                try {
                    openNrsFile();
                }
                catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            return snapshot;
        }
        finally {
            ioLock.writeLock().unlock();
        }
    }

    @Override
    public final NrsFutureDevice clone(final String name) {
        return clone(name, null, UUID.randomUUID());
    }

    @Override
    public final NrsFutureDevice clone(final String name, final String description) {
        return clone(name, description, UUID.randomUUID());
    }

    @Override
    public final NrsFutureDevice clone(final String name, final UUID createdDeviceUuid) {
        return clone(name, null, createdDeviceUuid);
    }

    @Override
    public final NrsFutureDevice clone(final String name, final String description, final UUID createdDeviceUuid) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is null");
        }

        // Create payload
        final NrsRepository repository = getVvr();

        // Create transaction
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();

        // Future device (clone)
        final UuidT<NrsFile> futureDeviceNrsUuid = SimpleIdentifierProvider.newId();
        final NrsFileHeader<NrsFile> futureDeviceNrsFileHeader = repository.doCreateNrsFileHeader(getNrsFileId(),
                getSize(), createdDeviceUuid, futureDeviceNrsUuid);
        NrsRemoteUtils.addNrsDevice(opBuilder, futureDeviceNrsFileHeader, getUuid(), name, description);

        // Create NrsFileHeader for original device (gets a new NrsFile)
        final UuidT<NrsFile> futureSnapshotUuid = SimpleIdentifierProvider.newId();
        final NrsFileHeader<NrsFile> origDeviceNrsFileHeader = repository.doCreateNrsFileHeader(getNrsFileId(),
                getSize(), getUuid(), futureSnapshotUuid);
        NrsRemoteUtils.addNrsFileHeaderMsg(opBuilder, origDeviceNrsFileHeader);

        // Launch transaction
        final UUID taskId = repository.submitTransaction(opBuilder, Type.DEVICE, OpCode.CLONE);

        // Get the device future
        return new NrsFutureDevice(createdDeviceUuid, repository, taskId);
    }

    final Device doCloneDevice(final UUID uuid, final String name, final String description,
            final NrsFileHeader<NrsFile> nrsFileHeader) {

        // Freeze IOs while cloning the device (and changing the NrsFile)
        ioLock.writeLock().lock();
        try {
            final boolean wasOpened = isNrsFileLocked();
            if (wasOpened) {
                // Close the file
                closeNrsFile(true);
            }

            final NrsRepository repository = getVvr();
            final NrsDevice.Builder builder = repository.newDeviceBuilder(nrsFileHeader.getParentId(), name,
                    description, getSize(), uuid);
            builder.nrsFileHeader(Objects.requireNonNull(nrsFileHeader));
            final NrsDevice device = (NrsDevice) builder.create();

            // Need to open new file?
            // Note: must not re-open the previous file on error (is read-only)
            if (wasOpened) {
                try {
                    openNrsFile();
                }
                catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            return device;
        }
        finally {
            ioLock.writeLock().unlock();
        }
    }

    @Override
    public final ReadWriteHandle open(final boolean readWrite) {
        // TODO check active
        final NrsRepository repository = getVvr();
        return deviceImplHelper.newReadWriteHandle(repository.getIbsInstance(), repository.getHashAlgorithm(),
                !readWrite, getBlockSize());
    }

    @Override
    public final FutureVoid delete() {
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
        final Uuid deviceUuid = VvrRemoteUtils.newUuid(getUuid());
        opBuilder.setUuid(deviceUuid);
        final NrsRepository repository = getVvr();
        final UUID taskId = repository.submitTransaction(opBuilder, Type.DEVICE, OpCode.DELETE);
        return new NrsFutureVoid(repository, taskId, getDeviceId());
    }

    final void deleteDevice() {
        if (active) {
            throw new IllegalStateException("Active");
        }
        if (isNrsFileLocked()) {
            throw new IllegalStateException("Opened");
        }

        getVvr().unregisterDevice(getUuid());
        // Delete NrsFile
        deleteNrsFile();
        // Delete persistence
        unpersist();
    }

    /**
     * Member builder for device instances.
     * 
     * 
     */
    public static final class Builder extends NrsVvrItem.Builder implements Device.Builder {

        @Override
        protected final UUID deviceID() {
            return uuid();
        }

        private NrsFileHeader<NrsFile> header;

        public final NrsDevice.Builder header(@Nonnull final NrsFileHeader<NrsFile> header) {
            this.header = Objects.requireNonNull(header);
            return this;
        }

        /**
         * New UUID of the NrsFile. New for a create, the one from the header for the load.
         */
        private UuidT<NrsFile> futureId;

        @Override
        protected final UuidT<NrsFile> futureID() {
            return futureId;
        }

        public final NrsDevice.Builder futureFileId(@Nonnull final UuidT<NrsFile> futureId) {
            this.futureId = Objects.requireNonNull(futureId);
            return this;
        }

        /**
         * Originating node.
         */
        private UUID nodeID;

        @Override
        protected final UUID nodeID() {
            return nodeID;
        }

        public final NrsDevice.Builder nodeID(@Nonnull final UUID nodeID) {
            this.nodeID = Objects.requireNonNull(nodeID);
            return this;
        }

        @Override
        public final Device build() {
            uuid(header.getDeviceId());
            parentFile(header.getParentId());
            futureFileId(header.getFileId());
            return new NrsDevice(this);
        }

        public final Device create() {
            // Check UUID conflict
            final NrsRepository repository = getVvr();
            {
                final UUID uuid = deviceID();
                if (repository.getDevice(uuid) != null) {
                    throw new IllegalStateException("Failed to create device, duplicate uuid=" + uuid);
                }
                if (repository.getSnapshot(uuid) != null) {
                    throw new IllegalStateException("Failed to create device, duplicate uuid=" + uuid + " (snapshot)");
                }
            }

            // Create and set NrsFile
            final NrsFile nrsFile = createNrsFile(NrsFileFlag.PARTIAL);
            sourceFile(nrsFile);
            final NrsDevice device = new NrsDevice(this);
            try {
                device.create();
                repository.registerDevice(device);
            }
            catch (final IOException e) {
                // TODO delete NrsFile
                throw new IllegalStateException("Failed to create device", e);
            }

            return device;
        }
    }
}
