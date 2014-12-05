package com.oodrive.nuage.nrs;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.oodrive.nuage.utils.SimpleIdentifierProvider;
import com.oodrive.nuage.utils.UuidT;

/**
 * Immutable header of a {@link NrsFile} or a {@link NrsFileBlock}.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
@Immutable
public final class NrsFileHeader<U> {

    /**
     * File magic identifying the NRS file format.
     * 
     * This must be found at the start of each file encoded in ASCII with the appropriate byte order.
     */
    private static final byte[] MAGIC_VERSION = new byte[] { 'N', 'R', 'S', '1' };

    static final int BYTES_PER_LONG = Long.SIZE / Byte.SIZE;

    /** Number of blocks per cluster in a {@link NrsFileBlock}. */
    private static final int BLOCKS_PER_CLUSER = 4;

    /**
     * The header length in bytes, including the magic or optional fields.
     */
    static final int HEADER_LENGTH = 104;

    private NrsFileHeader(final Builder<U> builder) {
        super();

        this.parentId = Objects.requireNonNull(builder.parentId);
        this.deviceId = Objects.requireNonNull(builder.deviceId);
        this.nodeId = Objects.requireNonNull(builder.nodeId);
        this.fileId = Objects.requireNonNull(builder.fileId);
        this.itemSize = builder.itemSize;
        this.itemBlockSize = builder.itemBlockSize;
        this.hashSize = builder.hashSize;

        // Performs sanity checks
        if (this.itemBlockSize <= 0) {
            throw new IllegalStateException("Invalid block size=" + itemBlockSize);
        }

        if (this.itemSize < 0) {
            throw new IllegalStateException("Invalid size=" + itemSize);
        }

        if ((this.itemSize % this.itemBlockSize) != 0) {
            throw new IllegalStateException("Invalid  block size=" + itemBlockSize + ", size=" + itemSize);
        }

        if (this.hashSize <= 0) {
            throw new IllegalStateException("Invalid hash length=" + hashSize);
        }

        this.clusterSize = builder.clusterSize;

        if (this.clusterSize <= 0) {
            throw new IllegalStateException("Invalid file cluster size");
        }

        if (this.clusterSize < this.getHashSize()) {
            throw new IllegalStateException("File cluster size less than hash size");
        }

        int hOneAddressTmp = clusterSize;
        // Make sure the H1 header is in the first cluster after the header
        while (HEADER_LENGTH > hOneAddressTmp)
            hOneAddressTmp += clusterSize;
        this.hOneAddress = hOneAddressTmp;

        // Compare with the hOneAddress in the header
        if (builder.hOneAddress != 0) {
            if (builder.hOneAddress != this.hOneAddress) {
                throw new IllegalStateException("Invalid H1 address");
            }
        }

        this.timestamp = builder.timestamp;
        final Set<NrsFileFlag> flags = builder.flags;
        if (flags == null) {
            root = false;
            partial = false;
            blocks = false;
        }
        else {
            root = flags.contains(NrsFileFlag.ROOT);
            partial = flags.contains(NrsFileFlag.PARTIAL);
            blocks = flags.contains(NrsFileFlag.BLOCKS);
        }
    }

    /**
     * The parent item's ID.
     */
    private final UuidT<U> parentId;
    /**
     * The associated device's ID.
     */
    private final UUID deviceId;
    /**
     * The ID of the originating node.
     */
    private final UUID nodeId;
    /**
     * The ID of the file.
     */
    private final UuidT<U> fileId;
    /**
     * The item size in bytes.
     */
    private final long itemSize;
    /**
     * The item block size.
     */
    private final int itemBlockSize;
    /**
     * The item hash key size.
     */
    private final int hashSize;

    /**
     * The cluster size in bytes.
     */
    private final int clusterSize;

    /**
     * The offset at which starts the H1 header.
     */
    private final int hOneAddress;

    /**
     * An time-stamp value set on creation of the file.
     */
    private final long timestamp;
    /**
     * <code>true</code> if the {@link NrsFile} is the root snapshot.
     */
    private final boolean root;
    /**
     * <code>true</code> if the {@link NrsFile} is partial.
     */
    private final boolean partial;
    /**
     * <code>true</code> if the {@link NrsFile} is associated to {@link NrsFileBlock}.
     */
    private final boolean blocks;

    /**
     * Gets the ID of the parent item.
     * 
     * @return the parent item's ID (not <code>null</code>)
     */
    public final UuidT<U> getParentId() {
        return parentId;
    }

    /**
     * Gets the ID of the device represented by this item.
     * 
     * @return the device ID (not <code>null</code>)
     */
    public final UUID getDeviceId() {
        return deviceId;
    }

    /**
     * Gets the ID of the originating node of this item.
     * 
     * @return the node ID (not <code>null</code>)
     */
    public final UUID getNodeId() {
        return nodeId;
    }

    /**
     * Gets the unique ID of the {@link NrsFile}.
     * 
     * @return the uuid of the file (not <code>null</code>)
     */
    public final UuidT<U> getFileId() {
        return fileId;
    }

    /**
     * Gets the size of the storage volume represented by this item.
     * 
     * @return the size in bytes
     */
    public final long getSize() {
        return itemSize;
    }

    /**
     * Gets the block size used for addressing blocks of the storage volume.
     * 
     * @return the positive block size in bytes
     */
    public final int getBlockSize() {
        return itemBlockSize;
    }

    /**
     * The size of the hash keys stored by the persistent item.
     * 
     * @return the positive hash size in bytes
     */
    public final int getHashSize() {
        return hashSize;
    }

    /**
     * Gets the cluster size.
     * 
     * 
     * @return the cluster size in bytes
     */
    public final int getClusterSize() {
        return clusterSize;
    }

    /**
     * Gets the address of the H1 header.
     * 
     * 
     * @return the H1 address in bytes if set
     */
    final int getH1Address() {
        return hOneAddress;
    }

    /**
     * Gets the timestamp.
     * 
     * 
     * @return the timestamp in milliseconds since the epoch
     */
    public final long getTimestamp() {
        return timestamp;
    }

    /**
     * Tells if the {@link NrsFile} is associated to the root snapshot.
     * 
     * @return <code>true</code> if root
     */
    public final boolean isRoot() {
        return root;
    }

    /**
     * Tells if the {@link NrsFile} is partial.
     * 
     * @return <code>true</code> if partial
     */
    public final boolean isPartial() {
        return partial;
    }

    /**
     * Tells if the {@link NrsFile} is associated to {@link NrsFileBlock} file(s).
     * 
     * @return <code>true</code> if has blocks file(s).
     */
    public final boolean isBlocks() {
        return blocks;
    }

    /**
     * Gets the flags set for the associated {@link NrsFile}.
     * 
     * @return the flags set on the {@link NrsFile}.
     */
    public final Set<NrsFileFlag> getFlags() {
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        if (root)
            flags.add(NrsFileFlag.ROOT);
        if (partial)
            flags.add(NrsFileFlag.PARTIAL);
        if (blocks)
            flags.add(NrsFileFlag.BLOCKS);
        return flags;
    }

    @Override
    public final String toString() {
        return com.google.common.base.Objects.toStringHelper(this).add("parentID", this.getParentId())
                .add("deviceID", this.getDeviceId()).add("nodeID", this.getNodeId()).add("fileID", this.getFileId())
                .add("size", this.getSize()).add("blockSize", this.getBlockSize())
                .add("clusterSize", this.getClusterSize()).add("hashSize", this.getHashSize())
                .add("H1Address", this.getH1Address()).add("root", this.isRoot()).add("partial", this.isPartial())
                .add("blocks", this.isBlocks()).toString();
    }

    /**
     * Reads the header from a {@link ByteBuffer}.
     * 
     * 
     * @param inputBuffer
     *            a buffer respecting the configured file endian-ness
     * @return an instance of {@link NrsFileHeader} initialized to the information found in the buffer
     * @throws NrsException
     *             if the buffer's too small or is not a valid header
     */
    static final <U> NrsFileHeader<U> readFromBuffer(final ByteBuffer inputBuffer) throws NrsException {

        if (inputBuffer.remaining() < HEADER_LENGTH) {
            throw new NrsException("Not enough data in header, remaining=" + inputBuffer.remaining() + ", headerlen="
                    + HEADER_LENGTH);
        }

        if (!validateMagic(inputBuffer)) {
            throw new NrsException("Invalid magic");
        }

        final UuidT<U> readParent = readUuidTFromBuffer(inputBuffer);
        final UUID readDeviceId = readUuidFromBuffer(inputBuffer);
        final UUID readNodeId = readUuidFromBuffer(inputBuffer);
        final UuidT<U> readFileId = readUuidTFromBuffer(inputBuffer);
        final long readSize = inputBuffer.getLong();
        final int readBlockSize = inputBuffer.getInt();
        final int readClusterSize = inputBuffer.getInt();
        final int readHashSize = inputBuffer.getInt();
        final int readH1Address = inputBuffer.getInt();
        final long readTimeStamp = inputBuffer.getLong();
        final Set<NrsFileFlag> flags = NrsFileFlag.decodeFlags(inputBuffer);

        final NrsFileHeader<U> result = new NrsFileHeader.Builder<U>().parent(readParent).device(readDeviceId)
                .node(readNodeId).file(readFileId).size(readSize).blockSize(readBlockSize).hashSize(readHashSize)
                .clusterSize(readClusterSize).hOneAddress(readH1Address).timestamp(readTimeStamp).flags(flags).build();
        return result;
    }

    /**
     * Writes the header to a byte buffer. The buffer length must be at least {@link #HEADER_LENGTH} bytes long.
     * 
     * @param outputBuffer
     *            the buffer into which the content is written
     */
    final void writeToBuffer(final ByteBuffer outputBuffer) {
        if (outputBuffer.remaining() < HEADER_LENGTH) {
            throw new IllegalArgumentException("Buffer too small=" + outputBuffer.remaining());
        }

        outputBuffer.put(MAGIC_VERSION);

        writeUuidToBuffer(getParentId(), outputBuffer);
        writeUuidToBuffer(getDeviceId(), outputBuffer);
        writeUuidToBuffer(getNodeId(), outputBuffer);
        writeUuidToBuffer(getFileId(), outputBuffer);

        outputBuffer.putLong(getSize());
        outputBuffer.putInt(getBlockSize());
        outputBuffer.putInt(getClusterSize());
        outputBuffer.putInt(getHashSize());

        outputBuffer.putInt(getH1Address());

        outputBuffer.putLong(getTimestamp());
        NrsFileFlag.encodeFlags(outputBuffer, getFlags());
    }

    /**
     * Create a NrsFileHeader&lt;NrsFileBlock&gt; for this NrsFileHeader&lt;NrsFile&gt;.
     * 
     * @return a new NrsFileHeader&lt;NrsFileBlock&gt;.
     * @throws NrsException
     */
    final NrsFileHeader<NrsFileBlock> newBlocksHeader() {

        if (!blocks) {
            throw new IllegalStateException();
        }

        final UuidT<NrsFileBlock> blocksParent = new UuidT<>(parentId.getMostSignificantBits(),
                parentId.getLeastSignificantBits());
        final UuidT<NrsFileBlock> blocksFileId = new UuidT<>(fileId.getMostSignificantBits(),
                fileId.getLeastSignificantBits());
        final int blocksClusterSize = itemBlockSize * BLOCKS_PER_CLUSER;

        // Done not set the flag BLOCKS
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        if (root)
            flags.add(NrsFileFlag.ROOT);
        if (partial)
            flags.add(NrsFileFlag.PARTIAL);

        final NrsFileHeader<NrsFileBlock> result = new NrsFileHeader.Builder<NrsFileBlock>().parent(blocksParent)
                .device(deviceId).node(nodeId).file(blocksFileId).size(itemSize).blockSize(itemBlockSize)
                .hashSize(hashSize).clusterSize(blocksClusterSize).hOneAddress(0).timestamp(timestamp).flags(flags)
                .build();
        return result;
    }

    private static final boolean validateMagic(final ByteBuffer inputBuffer) {
        // reads the file magic
        final byte[] fileMagic = new byte[MAGIC_VERSION.length];
        inputBuffer.get(fileMagic);

        return Arrays.equals(fileMagic, MAGIC_VERSION);
    }

    /**
     * Reads an {@link UUID} from the given {@link ByteBuffer}.
     * 
     * 
     * @param inputBuffer
     *            the buffer from which to read
     * @return the read UUID (never <code>null</code>)
     */
    private static final UUID readUuidFromBuffer(final ByteBuffer inputBuffer) {
        final long msb = inputBuffer.getLong();
        final long lsb = inputBuffer.getLong();
        return new UUID(msb, lsb);
    }

    private static final <U> UuidT<U> readUuidTFromBuffer(final ByteBuffer inputBuffer) {
        final long msb = inputBuffer.getLong();
        final long lsb = inputBuffer.getLong();
        return new UuidT<>(msb, lsb);
    }

    /**
     * Writes the given {@link UUID} to a target {@link ByteBuffer}.
     * 
     * 
     * The data is always written in standard network byte order (i.e. {@link ByteOrder#BIG_ENDIAN} as specified by <a
     * href='http://www.ietf.org/rfc/rfc4122.txt'>RFC4122</a>.
     * 
     * This method will write a 128bit value to the buffer regardless of the
     * 
     * @param uuid
     *            a valid {@link UUID} or {@code null}
     * @param outputBuffer
     *            the buffer to which to write
     */
    private static final void writeUuidToBuffer(final UUID uuid, final ByteBuffer outputBuffer) {
        // Writes always in same byte order
        outputBuffer.putLong(uuid.getMostSignificantBits());
        outputBuffer.putLong(uuid.getLeastSignificantBits());
    }

    /**
     * Same as {@link #writeUuidToBuffer(UUID, ByteBuffer)}, but for {@link UuidT}s.
     * 
     * @param uuid
     * @param outputBuffer
     */
    private static final <U> void writeUuidToBuffer(final UuidT<U> uuid, final ByteBuffer outputBuffer) {
        // Writes always in same byte order
        outputBuffer.putLong(uuid.getMostSignificantBits());
        outputBuffer.putLong(uuid.getLeastSignificantBits());
    }

    /**
     * Builder class to enforce correct construction of headers.
     * 
     * 
     * 
     */
    public static final class Builder<U> {

        /**
         * The ID of parent item.
         */
        private UuidT<U> parentId;
        /**
         * The associated device's ID.
         */
        private UUID deviceId;
        /**
         * The ID of the originating node.
         */
        private UUID nodeId;
        /**
         * The associated ID of the file.
         */
        private UuidT<U> fileId = SimpleIdentifierProvider.newId();
        /**
         * The item size in bytes.
         */
        private long itemSize = -1;
        /**
         * The item block size.
         */
        private int itemBlockSize;
        /**
         * The item hash key size.
         */
        private int hashSize;
        /**
         * The cluster size in bytes.
         */
        private int clusterSize;

        /**
         * Offset of the H1Header.
         */
        private int hOneAddress;

        /**
         * The cluster size in bytes.
         */
        private long timestamp;

        /**
         * File header flags.
         */
        private Set<NrsFileFlag> flags;

        Builder() {
            super();
        }

        public final Builder<U> parent(@Nonnull final UuidT<U> parent) {
            this.parentId = Objects.requireNonNull(parent);
            return this;
        }

        public final Builder<U> device(@Nonnull final UUID device) {
            this.deviceId = Objects.requireNonNull(device);
            return this;
        }

        public final Builder<U> node(@Nonnull final UUID node) {
            this.nodeId = Objects.requireNonNull(node);
            return this;
        }

        public final Builder<U> file(@Nonnull final UuidT<U> file) {
            this.fileId = Objects.requireNonNull(file);
            return this;
        }

        public final Builder<U> size(final @Nonnegative long size) {
            if (size < 0) {
                throw new IllegalStateException("Invalid size=" + size);
            }
            this.itemSize = size;
            return this;
        }

        public final Builder<U> blockSize(final int blockSize) {
            this.itemBlockSize = blockSize;
            return this;
        }

        public final Builder<U> hashSize(final int hashSize) {
            this.hashSize = hashSize;
            return this;
        }

        /**
         * Sets the cluster size.
         * 
         * @param clusterSize
         *            the cluster size to set in bytes
         * @return the modified builder
         */
        final Builder<U> clusterSize(final int clusterSize) {
            this.clusterSize = clusterSize;
            return this;
        }

        /**
         * The cluster size, for test purpose only.
         * 
         * @return the cluster size
         */
        final int clusterSize() {
            return this.clusterSize;
        }

        /**
         * @param hOneAddress
         * @return the modified builder
         */
        final Builder<U> hOneAddress(final int hOneAddress) {
            this.hOneAddress = hOneAddress;
            return this;
        }

        /**
         * 
         * @param timestamp
         * @return the modified builder
         */
        public final Builder<U> timestamp(final long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * 
         * @param flags
         * @return the modified builder
         */
        public final Builder<U> flags(final Set<NrsFileFlag> flags) {
            this.flags = flags;
            return this;
        }

        public final Builder<U> setFlags(final @Nonnull Set<NrsFileFlag> flags) {
            this.flags = EnumSet.noneOf(NrsFileFlag.class);
            this.flags.addAll(Objects.requireNonNull(flags));
            return this;
        }

        public final Builder<U> addFlags(final NrsFileFlag... flags) {
            if (this.flags == null) {
                this.flags = EnumSet.noneOf(NrsFileFlag.class);
            }
            for (int i = 0; i < flags.length; i++) {
                this.flags.add(flags[i]);
            }
            return this;
        }

        /**
         * builds a new instance.
         * 
         * 
         * @return the functional new instance
         */
        public final NrsFileHeader<U> build() {
            return new NrsFileHeader<U>(this);
        }
    }

}
