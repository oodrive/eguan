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

import static com.oodrive.nuage.vvr.remote.VvrRemoteUtils.newUuid;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nrs.NrsException;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.nrs.NrsFileFlag;
import com.oodrive.nuage.nrs.NrsFileHeader;
import com.oodrive.nuage.nrs.NrsFileJanitor;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.vvr.VvrRemote.Item;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.utils.Files;
import com.oodrive.nuage.utils.UuidT;
import com.oodrive.nuage.vvr.remote.VvrRemoteUtils;
import com.oodrive.nuage.vvr.repository.core.api.AbstractUniqueVvrObject;
import com.oodrive.nuage.vvr.repository.core.api.Device;
import com.oodrive.nuage.vvr.repository.core.api.FutureVoid;
import com.oodrive.nuage.vvr.repository.core.api.Snapshot;
import com.oodrive.nuage.vvr.repository.core.api.VvrItem;

/**
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author jmcaba
 * @author ebredzinski
 */
abstract class NrsVvrItem extends AbstractUniqueVvrObject implements VvrItem {

    static final String UUID_KEY = "uuid";
    // TODO should all be private
    static final String NAME_KEY = "name";
    static final String DESC_KEY = "desc";

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsVvrItem.class);

    /**
     * The underlying VVR.
     */
    private final NrsRepository vvr;

    /** Path of the persistence file. */
    private final File persistenceFile;

    /** Type of object in remote messages */
    private final Type msgObjectType;

    /** The {@link NrsFile} backing this instance. */
    private NrsFile nrsFile;

    /** The UUID of the 'existing' parent snapshot. Lazy init */
    private volatile UUID parentUuid;

    /**
     * Internal builder constructor to be invoked by subclass builders.
     *
     * @param builder
     *            the builder to build this instance from
     */
    NrsVvrItem(final NrsVvrItem.Builder builder) {
        super(builder);
        LOGGER.trace("building new {} instance with uuid {}", NrsVvrItem.class.getSimpleName(), this.getUuid());

        this.vvr = Objects.requireNonNull(builder.vvr);
        this.persistenceFile = newPersistenceFile(builder.metadataDirectory, getUuid());

        this.nrsFile = builder.nrsFile;
        final NrsFileHeader<NrsFile> header = nrsFile.getDescriptor();

        final UuidT<NrsFile> parent = header.getParentId();
        final long size = checkSize(vvr, header.getSize());

        {
            final UuidT<NrsFile> parentIdBuilder = builder.parentFileId;
            if (parentIdBuilder != null) {
                if (!String.valueOf(parent).equals(String.valueOf(parentIdBuilder))) {
                    throw new IllegalStateException("Persistent item has wrong parent ID");
                }
            }
        }

        if (vvr.getBlockSize() != header.getBlockSize()) {
            throw new IllegalStateException("Wrong block size=" + header.getBlockSize() + ", vvr=" + vvr.getBlockSize());
        }

        if (vvr.getHashLength() != header.getHashSize()) {
            throw new IllegalStateException("Wrong hash length=" + header.getHashSize() + ", vvr="
                    + vvr.getHashLength());
        }

        if (builder.size != 0 && builder.size != size) {
            throw new IllegalStateException("Wrong size=" + size + ", expected=" + builder.size);
        }

        // Type in remote messages
        if (this instanceof Device) {
            msgObjectType = Type.DEVICE;
        }
        else if (this instanceof Snapshot) {
            msgObjectType = Type.SNAPSHOT;
        }
        else {
            throw new AssertionError("class=" + getClass());
        }
    }

    /**
     * Allocate and set field to save.
     *
     * @return values to save.
     */
    protected Properties getPersistenceProperties() {
        final Properties result = new Properties();
        result.setProperty(UUID_KEY, getUuid().toString());

        // Do not set the values when they are null
        final String name = getName();
        if (name != null) {
            result.setProperty(NAME_KEY, name);
        }
        final String desc = getDescription();
        if (desc != null) {
            result.setProperty(DESC_KEY, desc);
        }

        return result;
    }

    final void create() throws IOException {
        // Create persistence file
        persist();
    }

    final void persist() throws IOException {
        final Properties properties = getPersistenceProperties();
        try (FileOutputStream fos = new FileOutputStream(persistenceFile)) {
            properties.store(fos, "");
        }
    }

    final void unpersist() {
        // Delete persistence
        persistenceFile.delete();
    }

    /**
     * Create the Java File containing the persistence of an item.
     *
     * @param persistenceDirectory
     * @param uuid
     * @return the file containing the persistence of the item
     */
    private static final File newPersistenceFile(final File persistenceDirectory, final UUID uuid) {
        return new File(persistenceDirectory, uuid.toString());
    }

    /**
     * Load the persistence for an item.
     *
     * @param persistenceDirectory
     * @param uuid
     * @return the persistence for the given UUID or null if there is no persistence.
     */
    static final Properties loadPersistence(final File persistenceDirectory, final UUID uuid) {
        final File persistenceFile = newPersistenceFile(persistenceDirectory, uuid);
        if (!persistenceFile.exists()) {
            return null;
        }
        final Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(persistenceFile)) {
            properties.load(fis);
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to load persistence '" + persistenceFile + "'", e);
        }
        return properties;
    }

    final NrsFile getNrsFilePath() {
        return nrsFile;
    }

    /**
     * Create a new {@link NrsFile} for this item, based on the given NrsFileHeader.
     *
     * @param nrsFileHeader
     */
    final void createNewNrsFile(final NrsFileHeader<NrsFile> nrsFileHeader) {
        try {
            // Seal the local NrsFile
            final NrsFileJanitor nrsFileJanitor = vvr.getNrsFileJanitor();
            nrsFileJanitor.sealNrsFile(nrsFile);

            final UuidT<NrsFile> prevNrsFileUuid = nrsFile.getDescriptor().getFileId();
            final NrsFile nrsFileTmp = nrsFileJanitor.createNrsFile(nrsFileHeader);

            // Clear 'old' parentUuid
            resetParent();
            nrsFile = nrsFileTmp;
            vvr.registerNrsFile(prevNrsFileUuid, nrsFile);
        }
        catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Open the NrsFile read/write. To be called from a device only.
     *
     * @throws IOException
     * @throws IllegalStateException
     */
    final void openNrsFile() throws IllegalStateException, IOException {
        final UuidT<NrsFile> uuid = nrsFile.getDescriptor().getFileId();
        nrsFile = getVvr().getNrsFileJanitor().openNrsFile(uuid, false);
        assert uuid.equals(nrsFile.getDescriptor().getFileId());
    }

    /**
     * Open the NrsFile read-only. <b>To be called from unit tests only.</b>
     *
     * @throws IOException
     * @throws IllegalStateException
     */
    final void openNrsFileTest() throws IllegalStateException, IOException {
        final UuidT<NrsFile> uuid = nrsFile.getDescriptor().getFileId();
        nrsFile = getVvr().getNrsFileJanitor().openNrsFile(uuid, true);
        assert uuid.equals(nrsFile.getDescriptor().getFileId());
    }

    /**
     * Close the {@link NrsFile}. Call from a device only.
     */
    final void closeNrsFile() {
        closeNrsFile(false);
    }

    /**
     * Close the {@link NrsFile}. Call from a device only.
     */
    final void closeNrsFile(final boolean setReadOnly) {
        getVvr().getNrsFileJanitor().closeNrsFile(nrsFile, setReadOnly);
    }

    /**
     * Delete the {@link NrsFile}. Call from a device only.
     */
    final void deleteNrsFile() {
        try {
            getVvr().getNrsFileJanitor().deleteNrsFile(nrsFile);
        }
        catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Tells if the {@link NrsFile} of the item is locked.
     *
     * @return <code>true</code> if the file is locked
     */
    final boolean isNrsFileLocked() {
        // Close the file if not locked and check if it's still opened
        getVvr().getNrsFileJanitor().flushNrsFile(nrsFile);
        return nrsFile.isOpened();
    }

    @Override
    public final FutureVoid setUserProperties(final String... keyValues) {
        final Item.Builder builder = Item.newBuilder();
        // Write key/value pairs
        final int count = keyValues.length;
        if (count < 1 || (count % 2 != 0)) {
            throw new IllegalArgumentException("keyValues.count=" + count);
        }
        for (int i = 0; i < count; i++) {
            builder.addSetProp(keyValues[i]);
        }
        return submitUserPropertyTransaction(builder);
    }

    @Override
    public final FutureVoid unsetUserProperties(final String... keys) {
        final Item.Builder builder = Item.newBuilder();
        // Write key
        for (int i = keys.length - 1; i >= 0; i--) {
            builder.addDelProp(keys[i]);
        }
        return submitUserPropertyTransaction(builder);
    }

    private final FutureVoid submitUserPropertyTransaction(final Item.Builder builder) {
        final UUID itemUuid = getUuid();
        final RemoteOperation.Builder builderOp = RemoteOperation.newBuilder();
        builderOp.setItem(builder);
        builderOp.setUuid(VvrRemoteUtils.newUuid(itemUuid));

        final NrsRepository repository = getVvr();
        final UUID taskId = repository.submitTransaction(builderOp, msgObjectType, OpCode.SET);
        return new NrsFutureVoid(repository, taskId, itemUuid);
    }

    /**
     * Set a user-defined property. Executes a transaction.
     *
     * @param name
     * @param value
     */
    final void setUserPropertiesLocal(final String name, final String value) {
        try {
            synchronized (persistenceFile) {
                Files.setUserAttr(persistenceFile.toPath(), name, value);
            }
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to write user property '" + persistenceFile + "'", e);
        }
    }

    /**
     * Unset a user-defined property. Executes a transaction.
     *
     * @param name
     */
    final void unsetUserPropertiesLocal(final String name) {
        try {
            synchronized (persistenceFile) {
                Files.unsetUserAttr(persistenceFile.toPath(), name);
            }
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to unset user property '" + persistenceFile + "' name=" + name, e);
        }
    }

    @Override
    public final String getUserProperty(final String name) {
        try {
            synchronized (persistenceFile) {
                final Path persistencePath = persistenceFile.toPath();
                return Files.getUserAttr(persistencePath, Objects.requireNonNull(name));
            }
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to get user property '" + persistenceFile + "'", e);
        }
    }

    @Override
    public final Map<String, String> getUserProperties() {
        try {
            synchronized (persistenceFile) {
                final Path persistencePath = persistenceFile.toPath();
                final String[] attrs = Files.listUserAttr(persistencePath);
                final Map<String, String> result = new HashMap<>(attrs.length);
                for (int i = 0; i < attrs.length; i++) {
                    final String attr = attrs[i];
                    result.put(attr, Files.getUserAttr(persistencePath, attr));
                }
                return result;
            }
        }
        catch (final IOException e) {
            throw new IllegalStateException("Failed to get user properties '" + persistenceFile + "'", e);
        }
    }

    @Override
    public final NrsRepository getVvr() {
        return this.vvr;
    }

    @Override
    public final int getBlockSize() {
        return vvr.getBlockSize();
    }

    @Override
    public final UUID getParent() {
        if (parentUuid != null) {
            return parentUuid;
        }
        // Look for the parent snapshot in the repository
        final Snapshot parent = vvr.getParentSnapshot(getParentFile());
        parentUuid = parent.getUuid();
        return parentUuid;
    }

    /**
     * Reset the parent UUID cache.
     */
    protected final void resetParent() {
        parentUuid = null;
    }

    @Override
    public final boolean isPartial() {
        return nrsFile.getDescriptor().isPartial();
    }

    @Override
    public final long getSize() {
        return nrsFile.getDescriptor().getSize();
    }

    @Override
    public final long getDataSize() {
        // TODO: add corresponding method to NrsFile and base computation on that
        return nrsFile.getAllocatedNumberOfRecords() * getBlockSize();
    }

    final Object readHash(final long blockIndex, final boolean recursive, final boolean ex) throws IOException {
        byte[] result = nrsFile.read(blockIndex);
        if (result != null) {
            if (ex) {
                return new NrsBlockKeyLookupEx(result, nrsFile);
            }
            else {
                return result;
            }
        }

        UUID nodeSrc = null;
        NrsFile fileSrc = null;
        if (result == null && recursive && isPartial()) {
            final NrsFileJanitor nrsFileJanitor = getVvr().getNrsFileJanitor();
            UuidT<NrsFile> parentUuid = getParentFile();
            boolean partial = true;
            while (partial) {

                // Get parent file
                final NrsFile parent = nrsFileJanitor.loadNrsFile(parentUuid);
                final NrsFileHeader<NrsFile> header = parent.getDescriptor();

                // No need to try to access to the root snapshot (is empty)
                if (header.isRoot()) {
                    break;
                }

                try {
                    final NrsFile parentOpened = nrsFileJanitor.openNrsFile(parentUuid, true);
                    try {
                        result = parentOpened.read(blockIndex);
                    }
                    finally {
                        nrsFileJanitor.unlockNrsFile(parentOpened);
                    }

                    // Key found: keep the node on which the file was filled
                    if (result != null) {
                        fileSrc = parentOpened;
                        nodeSrc = parentOpened.getDescriptor().getNodeId();
                        break;
                    }
                }
                catch (final IndexOutOfBoundsException ie) {
                    LOGGER.debug("Read offset out of parent range");
                    // TODO: improve reading out of parent limits
                    break;
                }

                // Upper level
                parentUuid = header.getParentId();
                partial = header.isPartial();
            }
        }

        // Not found, recursive or not
        if (result == null) {
            return null;
        }

        // Found, recursive only
        if (ex) {
            return new NrsBlockKeyLookupEx(result, fileSrc, nodeSrc);
        }
        else {
            return result;
        }
    }

    /**
     * Writes a block hash to the internal block mapping.
     *
     * @param blockIndex
     *            index of the block to write
     * @param blockHash
     *            the hash value to write
     * @throws IOException
     */
    final void writeBlockHash(final long blockIndex, final byte[] blockHash) throws IOException {
        nrsFile.write(blockIndex, blockHash);
    }

    /**
     * Release the block written at the given index. Does nothing if the block is not allocated.
     *
     * @param blockIndex
     * @throws IOException
     */
    final void resetBlockHash(final long blockIndex) throws IOException {
        nrsFile.reset(blockIndex);
    }

    /**
     * Trim the block written at the given index. Does nothing if the block is not allocated.
     *
     * @param blockIndex
     * @throws IOException
     */
    final void trimBlockHash(final long blockIndex) throws IOException {
        nrsFile.trim(blockIndex);
    }

    /**
     * Gets the parent {@link NrsFile}.
     *
     * @return the direct parent in Nrs hierarchy
     */
    public final UuidT<NrsFile> getParentFile() {
        return nrsFile.getDescriptor().getParentId();
    }

    /**
     * Validates and sets a new value for the size of this item.
     *
     * @param newSize
     *            the new size in bytes, must be a multiple of the configured block size
     */
    private static final long checkSize(final NrsRepository vvr, final long newSize) {
        final int blockSize = vvr.getBlockSize();
        if ((newSize != 0) && (newSize < blockSize)) {
            throw new IllegalArgumentException("newSize=" + newSize + ", blockSize=" + blockSize);
        }

        if ((newSize % blockSize) != 0) {
            throw new IllegalArgumentException("newSize=" + newSize + ", blockSize=" + blockSize);
        }
        return newSize;
    }

    /**
     * Gets the current frontend ID.
     *
     * @return the frontend ID or null if not defined
     */
    public final UUID getDeviceId() {
        return nrsFile.getDescriptor().getDeviceId();
    }

    /**
     * Gets the originating node of the item.
     *
     * @return the ID of the originating node or null if not defined
     */
    public final UUID getNodeId() {
        return nrsFile.getDescriptor().getNodeId();
    }

    /**
     * File ID.
     *
     * @return the <code>UUID</code> of the {@link NrsFile}
     */
    final UuidT<NrsFile> getNrsFileId() {
        return nrsFile.getDescriptor().getFileId();
    }

    final long getTimestamp() {
        return nrsFile.getDescriptor().getTimestamp();
    }

    @Override
    protected final FutureVoid submitTransaction(final RemoteOperation.Builder opBuilder, final OpCode opCode) {
        final NrsRepository targetVvr = getVvr();
        opBuilder.setUuid(newUuid(getUuid()));
        return new NrsFutureVoid(targetVvr, targetVvr.submitTransaction(opBuilder, msgObjectType, opCode), getUuid());
    }

    @Override
    public final String toString() {
        return com.google.common.base.Objects.toStringHelper(this).add("id", this.getUuid())
                .add("name", this.getName()).add("vvrId", this.getVvr()).add("parent", this.getParent())
                .add("parentItem", this.getParentFile()).add("partial", this.isPartial()).add("size", this.getSize())
                .add("data size", this.getDataSize()).toString();
    }

    /**
     * Parent builder to create or to load {@link NrsVvrItem}s.
     */
    abstract static class Builder extends AbstractUniqueVvrObject.Builder {

        /**
         * VVR instance of the new item belongs to.
         */
        private NrsRepository vvr;

        /**
         * Gets the configured VVR ID.
         *
         * @return the configured VVR ID
         */
        protected final NrsRepository getVvr() {
            return this.vvr;
        }

        /**
         * Sets the VVR owning the future item.
         * <p>
         *
         * <i>REQUIRED.</i>
         * <p>
         *
         * @param vvr
         *            the non-<code>null</code> existing VVR
         * @return the modified builder
         */
        protected final NrsVvrItem.Builder vvr(final @Nonnull NrsRepository vvr) {
            this.vvr = Objects.requireNonNull(vvr);
            return this;
        }

        /**
         * The id of the parent item for the item to be built.
         */
        private UuidT<NrsFile> parentFileId;

        protected final VvrItem.Builder parentFile(final UuidT<NrsFile> parentFile) {
            this.parentFileId = parentFile;
            return this;
        }

        /**
         * Valid {@link NrsFile}.
         */
        private NrsFile nrsFile;

        /**
         * Sets the {@link NrsFile} associated to the item. Must be set after load of the persistence (before build())
         * or before the creation.
         *
         * @param file
         *            the {@link NrsFile}
         * @return the modified builder
         */
        protected final NrsVvrItem.Builder sourceFile(@Nonnull final NrsFile nrsFile) {
            this.nrsFile = Objects.requireNonNull(nrsFile);
            return this;
        }

        /**
         * The size of the item in bytes.
         */
        private long size = -1L;

        protected final VvrItem.Builder size(final long size) {
            assert size >= 0;
            this.size = size;
            return this;
        }

        private Set<NrsFileFlag> flags;

        protected final VvrItem.Builder flags(final Set<NrsFileFlag> flags) {
            this.flags = Collections.unmodifiableSet(flags);
            return this;
        }

        /**
         * The directory in which the metadata of the item will be saved.
         */
        private File metadataDirectory;

        protected final NrsVvrItem.Builder metadataDirectory(final File metadataDirectory) {
            assert metadataDirectory.isDirectory();
            this.metadataDirectory = metadataDirectory;
            return this;
        }

        protected final NrsFile createNrsFile(final NrsFileHeader<NrsFile> nrsFileHeader) {
            try {
                final NrsFileJanitor nrsFileJanitor = vvr.getNrsFileJanitor();
                if (nrsFileHeader == null) {
                    return nrsFileJanitor.createNrsFile(createNrsFileHeader());
                }
                else {
                    return nrsFileJanitor.createNrsFile(nrsFileHeader);
                }
            }
            catch (final NrsException e) {
                throw new IllegalStateException(e);
            }
        }

        protected final NrsFileHeader<NrsFile> createDefaultNrsFileHeader() {
            final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
            flags.add(NrsFileFlag.PARTIAL);
            if (NrsDevice.NRS_BLOCK_FILE_ENABLED) {
                flags.add(NrsFileFlag.BLOCKS);
            }
            return createNrsFileHeader(flags);
        }

        protected final NrsFileHeader<NrsFile> createNrsFileHeader() {
            return createNrsFileHeader(flags);
        }

        private final NrsFileHeader<NrsFile> createNrsFileHeader(final Set<NrsFileFlag> flags) {
            final NrsFileJanitor nrsFileJanitor = vvr.getNrsFileJanitor();
            final NrsFileHeader.Builder<NrsFile> headerBuilder = nrsFileJanitor.newNrsFileHeaderBuilder();
            // Ids
            headerBuilder.parent(parentFileId);
            headerBuilder.device(deviceID());
            headerBuilder.node(nodeID());
            headerBuilder.file(futureID());
            // Data format
            headerBuilder.setFlags(flags).blockSize(vvr.getBlockSize()).hashSize(vvr.getHashLength());
            // Size and time stamp
            headerBuilder.timestamp(System.currentTimeMillis());
            headerBuilder.size(size);
            return headerBuilder.build();
        }

        /**
         * Gets the device ID to create the NrsFile.
         *
         * @return the device ID
         */
        protected abstract UUID deviceID();

        /**
         * Gets the id of the originating node.
         *
         * @return ID of the originating node of the item
         */
        protected abstract UUID nodeID();

        /**
         * Gets the ID of the NrsFile to create.
         *
         * @return the file ID
         */
        protected abstract UuidT<NrsFile> futureID();

    }

}
