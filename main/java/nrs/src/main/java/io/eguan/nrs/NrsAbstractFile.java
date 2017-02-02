package io.eguan.nrs;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.hash.ByteBufferDigest;
import io.eguan.hash.HashAlgorithm;
import io.eguan.proto.nrs.NrsRemote;
import io.eguan.proto.nrs.NrsRemote.NrsFileMapping;
import io.eguan.proto.nrs.NrsRemote.NrsFileMapping.NrsClusterHash;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsCluster;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsH1Header;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsKey.NrsKeyHeader;
import io.eguan.proto.nrs.NrsRemote.NrsFileUpdate.NrsUpdate;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.UuidCharSequence;
import io.eguan.utils.UuidT;
import io.eguan.utils.Files.HandledFile;
import io.eguan.utils.mapper.FileMapper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.PhantomReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Abstract class hold the format of the NrsFile containing the image of devices/snapshots in blocks or block digest
 * format.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 * @param <T>
 *            type of the data written or read from the file.
 * @param <U>
 *            type of the file.
 */
abstract class NrsAbstractFile<T, U> extends HandledFile<UuidT<U>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsAbstractFile.class);

    /** Hash header when the hash is not set */
    private static final byte HASH_NOT_ALLOCATED_VALUE = 0;
    /** Hash header when the hash is set */
    private static final byte HASH_ALLOCATED_VALUE = 1;
    /** Hash header when the hash have been trimmed */
    private static final byte HASH_TRIMMED_VALUE = 2;

    /** Byte order for the read/write of header and L1 table */
    final static ByteOrder NRS_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    /** Header of the file */
    private final NrsFileHeader<U> header;

    /** Path of the file */
    private final File mappedFile;

    /** <code>true</code> if the file can be written */
    private boolean writeable;

    /** Lock access to the open state of the file */
    private final ReentrantReadWriteLock openLock = new ReentrantReadWriteLock();
    /** Lock access to L2 tables and channel/mapped buffer during read/write operations */
    private final ReentrantLock ioLock = new ReentrantLock();

    /** Channel opened on the file or <code>null</code> */
    @GuardedBy(value = "openLock")
    private boolean opened;
    /** Channel opened on the file or <code>null</code> if the file is closed or empty */
    @GuardedBy(value = "openLock")
    private FileChannel backendFileChannel;
    /** Lock on the file or <code>null</code> if the file is closed or empty */
    @GuardedBy(value = "openLock")
    private FileLock backendFileLock;
    /** Mapping of the whole file when possible or <code>null</code> */
    @GuardedBy(value = "openLock")
    private MappedByteBuffer backendFileMappedBuffer;
    /** Phantom reference to speed-up unmap() */
    private PhantomReference<MappedByteBuffer> backendFileMappedBufferRef;
    /** <code>true</code> if the file is opened in read-only mode */
    @GuardedBy(value = "openLock")
    private boolean readOnly;

    /** Optional file to store blocks */
    @GuardedBy(value = "openLock")
    private NrsFileBlock fileBlock;

    /** <code>true</code> when this file is updated from the contents of a peer */
    @GuardedBy(value = "ioLock")
    private boolean update = false;
    /** <code>true</code> when the last update of the file was aborted */
    @GuardedBy(value = "ioLock")
    private boolean updateAborted = false;
    /** <code>true</code> when the H1 header have been copied from peer */
    @GuardedBy(value = "ioLock")
    private boolean updateH1 = false;
    /** Set when an update from a peer is in progress */
    @GuardedBy(value = "ioLock")
    private Condition updateCondition;

    /** True when this file was been written */
    private final AtomicBoolean wasWritten = new AtomicBoolean();

    /** L1 table or <code>null</code> when closed. Guarded by openLock for set/get of the field. */
    @GuardedBy(value = "ioLock")
    private H1Header h1Header;

    // Pre-allocated read-only buffers
    @GuardedBy(value = "ioLock")
    private final ByteBuffer NOT_ALLOCATED;
    @GuardedBy(value = "ioLock")
    private final ByteBuffer ALLOCATED;
    @GuardedBy(value = "ioLock")
    private final ByteBuffer TRIMMED;
    @GuardedBy(value = "ioLock")
    private final ByteBuffer EOF;

    /** Value for a trimmed block */
    private final T TRIMMED_VALUE;

    // Pre-allocated read-write buffer
    @GuardedBy(value = "ioLock")
    private final ByteBuffer IS_ALLOCATED;

    /** Size of an element, in byte */
    private final int elementSize;

    /** Block index maximum value (inclusive) */
    private final long blockIndexMax;

    /** Size in bytes of the {@link H1Header} */
    private final long h1Size;

    /** Index of the last cluster of the {@link H1Header} */
    private final long h1LastClusterIdx;

    /** Number of entries in a L2 table */
    private final int l2capacity;

    /** Size of a cluster of the backend file. */
    private final int clusterSize;

    /** Notify remote peers */
    private final NrsMsgPostOffice postOffice;

    /**
     * Constructs an instance from the given builder.
     * 
     * @param fileMapper
     *            file mapper handling that file
     * @param header
     *            read header of the file
     * @param postOffice
     *            optional notification of some remote peers.
     */
    NrsAbstractFile(final int elementSize, final FileMapper fileMapper, final NrsFileHeader<U> header,
            final NrsMsgPostOffice postOffice, final T TRIMMED_VALUE) {
        super();

        this.elementSize = elementSize;

        this.TRIMMED_VALUE = TRIMMED_VALUE;

        this.header = header;

        this.mappedFile = fileMapper.mapIdToFile(new UuidCharSequence(this.header.getFileId()));

        this.writeable = Files.isWritable(mappedFile.toPath());

        this.postOffice = postOffice;

        final long size = header.getSize();
        if (size > 0) {
            blockIndexMax = (size / header.getBlockSize()) - 1;
        }
        else {
            blockIndexMax = -1;
        }

        this.l2capacity = computeL2Capacity();
        assert l2capacity > 0;

        this.clusterSize = header.getClusterSize();
        this.h1Size = computeH1Size();

        // Should fit in an integer
        assert h1Size >>> 31 == 0L;

        // Compute first and last cluster index of the H1 table
        if (size > 0) {
            final long h1StartOffset = header.getH1Address();
            long h1EndOffset = h1StartOffset + h1Size;
            if ((h1EndOffset % clusterSize) != 0) {
                h1EndOffset = (h1EndOffset / clusterSize) * clusterSize + clusterSize;
            }

            assert (h1StartOffset % clusterSize) == 0;
            assert (h1EndOffset % clusterSize) == 0;

            this.h1LastClusterIdx = h1EndOffset / clusterSize - 1;
        }
        else {
            this.h1LastClusterIdx = -1;
        }

        // Initialize buffers to read / write datas
        this.NOT_ALLOCATED = ByteBuffer.allocate(1);
        this.NOT_ALLOCATED.put(HASH_NOT_ALLOCATED_VALUE);
        this.ALLOCATED = ByteBuffer.allocate(1);
        this.ALLOCATED.put(HASH_ALLOCATED_VALUE);
        this.TRIMMED = ByteBuffer.allocate(1);
        this.TRIMMED.put(HASH_TRIMMED_VALUE);
        this.EOF = ByteBuffer.allocate(1);
        this.EOF.put((byte) 0);

        this.IS_ALLOCATED = ByteBuffer.allocate(1);
    }

    /**
     * Tells if the current file is a NrsFile is associated to a NrsFileBlock.
     * 
     * @return true for a {@link NrsFile} with a {@link NrsFileBlock}
     */
    private final boolean isNrsFileBlock() {
        return this instanceof NrsFile && header.isBlocks();
    }

    /**
     * Create the {@link NrsFile} file.
     * 
     * @throws IOException
     */
    final void create() throws IOException {
        openLock.writeLock().lock();
        try {
            // Create parents if needed
            mappedFile.getParentFile().mkdirs();

            // Create file and write header
            if (mappedFile.exists()) {
                throw new NrsException("File already exists: '" + mappedFile + "'");
            }
            try (final FileChannel channel = FileChannel.open(mappedFile.toPath(), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                final ByteBuffer writeBuffer = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH);
                writeBuffer.order(NRS_BYTE_ORDER);
                header.writeToBuffer(writeBuffer);
                if (writeBuffer.remaining() != 0) {
                    throw new AssertionError("remaining=" + writeBuffer.remaining());
                }
                writeBuffer.position(0);
                final int writeLen = channel.write(writeBuffer);
                if (writeLen != NrsFileHeader.HEADER_LENGTH) {
                    throw new NrsException("error writing file header, writeLen=" + writeLen + ", headerlen="
                            + NrsFileHeader.HEADER_LENGTH);
                }
            }

            // Set file size to contain the h1Header to be able to map it
            if (header.getSize() > 0) {
                // The size of the file must be a number of clusters
                final long size = h1LastClusterIdx * clusterSize + clusterSize;
                try (final RandomAccessFile raf = new RandomAccessFile(mappedFile, "rw")) {
                    raf.setLength(size);
                }
            }

            // Update writable state
            writeable = Files.isWritable(mappedFile.toPath());
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    /**
     * Sets the {@link NrsFileBlock} associated to this {@link NrsFile}.
     * 
     * @param fileBlock
     */
    final void setFileBlock(@Nonnull final NrsFileBlock fileBlock) {
        openLock.readLock().lock();
        try {
            assert isNrsFileBlock();

            if (this.fileBlock != null) {
                throw new IllegalStateException("Already set");
            }
            this.fileBlock = Objects.requireNonNull(fileBlock);
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Gets the associated {@link NrsFileBlock}.
     * 
     * @return The associated {@link NrsFileBlock} or <code>null</code>
     */
    final NrsFileBlock getFileBlock() {
        openLock.readLock().lock();
        try {
            return this.fileBlock;
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Opens the file.
     * 
     * @param readOnly
     *            true if the file is opened for read access only
     * @throws IOException
     * @throws IllegalStateException
     *             if the file is already opened
     */
    @Override
    protected final void open(final boolean readOnly) throws IOException, IllegalStateException {
        openLock.writeLock().lock();
        try {
            if (opened) {
                throw new IllegalStateException("'" + mappedFile + "' already opened");
            }
            if (!writeable && !readOnly) {
                throw new AccessDeniedException(getFile().toString());
            }
            if (isNrsFileBlock() && fileBlock == null) {
                throw new IllegalStateException("'" + mappedFile + "': blocks file not set");
            }

            // No need to open any channel if the file is empty (root snapshot)
            if (header.getSize() == 0) {
                opened = true;
                return;
            }

            this.readOnly = readOnly;
            if (readOnly) {
                // Read-only: map the whole file
                backendFileChannel = FileChannel.open(mappedFile.toPath(), StandardOpenOption.READ);
                final long fileSize = backendFileChannel.size();
                if (fileSize < Integer.MAX_VALUE) {
                    // Position in buffer limited to 4GB => almost a device of 1TB with MD5 hash
                    backendFileMappedBuffer = backendFileChannel.map(MapMode.READ_ONLY, 0, fileSize);
                    backendFileMappedBufferRef = new PhantomReference<>(backendFileMappedBuffer, null);
                    backendFileMappedBufferRef.get(); // Make compiler happy
                }
                else {
                    backendFileMappedBuffer = null;
                    backendFileMappedBufferRef = null;
                }
            }
            else {
                // Just open the file
                backendFileChannel = FileChannel.open(mappedFile.toPath(), StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
                backendFileMappedBuffer = null;
                backendFileMappedBufferRef = null;
            }

            // The size of the file must be a number of clusters
            assert (backendFileChannel.size() % clusterSize) == 0;

            try {
                // Make sure that no other instance has opened the file
                // Note: get a NonWritableChannelException if the channel is read-only for an non shared lock
                backendFileLock = backendFileChannel.tryLock(0, 1, readOnly);
                if (backendFileLock == null) {
                    // Lock held by another process
                    throw new OverlappingFileLockException();
                }

                // Map L1table
                final MappedByteBuffer l1Buffer = backendFileChannel.map(readOnly ? MapMode.READ_ONLY
                        : MapMode.READ_WRITE, header.getH1Address(), h1Size);
                l1Buffer.order(NRS_BYTE_ORDER);
                this.h1Header = H1Header.wrap(l1Buffer);

                // Opens the block file if any
                if (fileBlock != null) {
                    fileBlock.open(readOnly);
                }

                // Ok
                opened = true;
            }
            finally {
                if (!opened) {
                    backendFileMappedBuffer = null;
                    backendFileMappedBufferRef = null;

                    if (backendFileLock != null) {
                        backendFileLock.release();
                        backendFileLock = null;
                    }

                    backendFileChannel.close();
                    backendFileChannel = null;
                }
            }
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    /**
     * Closes the file.
     */
    @Override
    protected final void close() {
        openLock.writeLock().lock();
        try {
            if (opened) {

                // Closes the block file if any
                if (fileBlock != null) {
                    try {
                        fileBlock.close();
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to close '" + fileBlock + "'", t);
                    }
                }

                // Unreference buffer to allow unmap
                backendFileMappedBuffer = null;
                backendFileMappedBufferRef = null;

                if (h1Header != null) {
                    h1Header.close();
                    h1Header = null;
                }

                // Unlock file
                if (backendFileLock != null) {
                    try {
                        backendFileLock.release();
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to unlock '" + mappedFile + "'", t);
                    }
                    backendFileLock = null;
                }

                // Close file
                if (backendFileChannel != null) {
                    try {
                        backendFileChannel.close();
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to close '" + mappedFile + "'", t);
                    }
                    backendFileChannel = null;
                }
                opened = false;
            }
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    @Override
    protected final UuidT<U> getId() {
        return getDescriptor().getFileId();
    }

    @Override
    public final boolean isOpened() {
        openLock.readLock().lock();
        try {
            return opened;
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    @Override
    public final boolean isOpenedReadOnly() {
        openLock.readLock().lock();
        try {
            return opened && readOnly;
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Tells if some thread have locked the opening/closing of the file.
     * 
     * @return <code>true</code> if some thread is working on the file or if the file is being opened or closed.
     */
    @Override
    protected final boolean isOpenedLock() {
        return openLock.getReadHoldCount() > 0 || openLock.isWriteLocked();
    }

    /**
     * Deletes the persistent file.
     * 
     * @throws IOException
     *             if deletion fails
     * @throws IllegalStateException
     *             if the file is opened
     */
    final void delete() throws IOException, IllegalStateException {
        openLock.writeLock().lock();
        try {
            if (opened) {
                throw new IllegalStateException("'" + mappedFile + "' already opened");
            }
            Files.deleteIfExists(this.mappedFile.toPath());
            writeable = false;

            // Deletes the block file if any
            if (fileBlock != null) {
                try {
                    fileBlock.delete();
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to delete '" + fileBlock + "'", t);
                }
            }
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    /**
     * Set the file read-only in the file system.
     * 
     * @throws IllegalStateException
     *             if the file is opened
     */
    final void setNotWritable() throws IllegalStateException {
        openLock.writeLock().lock();
        try {
            // Opened?
            if (opened) {
                throw new IllegalStateException("'" + mappedFile + "' already opened");
            }
            // Set FS property
            if (writeable) {
                mappedFile.setReadOnly();
                writeable = false;
            }
            if (fileBlock != null) {
                fileBlock.setNotWritable();
            }
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    final void setWritable() {
        openLock.writeLock().lock();
        try {
            // Set FS property
            mappedFile.setWritable(true);
            writeable = true;
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    /**
     * Returns the writable state of the item.
     * 
     * @return <code>true</code> if this item is writable, <code>false</code> otherwise
     */
    final boolean isWriteable() {
        openLock.readLock().lock();
        try {
            return writeable;
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Gets the version of this file. If the file is opened in write mode, the value may change.
     * 
     * @return the current version of this file.
     * @throws IOException
     * @throws IllegalStateException
     */
    public final long getVersion() throws IllegalStateException, IOException {
        // Try to read under read lock
        openLock.readLock().lock();
        try {
            if (opened) {
                // Read version from the h1Header (under lock)
                ioLock.lock();
                try {
                    if (h1Header == null) {
                        // Empty file, root, ...
                        return 0;
                    }
                    return h1Header.getVersion();
                }
                finally {
                    ioLock.unlock();
                }
            }
        }
        finally {
            openLock.readLock().unlock();
        }

        // Need to open the file under write lock
        openLock.writeLock().lock();
        try {
            if (opened) {
                // No need to open the file
                return getVersion();
            }
            open(true);
            try {
                return getVersion();
            }
            finally {
                close();
            }
        }
        finally {
            openLock.writeLock().unlock();
        }
    }

    /**
     * Create a remote message containing the mapping of the file. The mapping contains the version and the signature of
     * each cluster. The file must be opened.
     * <p>
     * Note: this call freezes read and write access to the file.
     * 
     * @param hashAlgorithm
     *            hash algorithm to apply to clusters.
     * @return a message builder, containing only the NrsFileMapping.
     * @throws IllegalStateException
     *             if the file is not opened or is a root.
     * @throws IOException
     *             if access to the underlying file fails.
     */
    public final RemoteOperation.Builder getFileMapping(final HashAlgorithm hashAlgorithm)
            throws IllegalStateException, IOException {
        openLock.readLock().lock();
        try {
            // Opened?
            if (!opened) {
                throw new IllegalStateException("'" + mappedFile + "' not opened");
            }
            // Does not work with a root element: size is not a number of clusters
            // Anyway, a root file is immutable
            if (header.isRoot()) {
                throw new IllegalStateException("'" + mappedFile + "' is root");
            }

            final NrsFileMapping.Builder builder = NrsFileMapping.newBuilder();

            // Set the cluster size
            builder.setClusterSize(clusterSize);

            // Lock L1 and L2 tables contents (version and clusters)
            ioLock.lock();
            try {
                if (update) {
                    throw new IllegalStateException("'" + mappedFile + "' update");
                }

                // Set the version
                builder.setVersion(h1Header.getVersion());

                // Compute cluster hash (may take some time and may require some heap memory...)

                // Get cluster count and check size
                final long size = backendFileChannel.size();
                final long clusterCount = size / clusterSize;
                if ((size - (clusterCount * clusterSize)) != 0) {
                    throw new IllegalStateException("'" + mappedFile + "' invalid file size=" + size + ", clusterSize="
                            + clusterSize);
                }

                // Allocate buffer once for all and reset file channel position
                final ByteBuffer readCluster = NrsByteBufferCache.allocate(clusterSize);
                try {
                    long clusterIndex = h1LastClusterIdx + 1;
                    backendFileChannel.position(clusterIndex * clusterSize);
                    for (; clusterIndex < clusterCount; clusterIndex++) {
                        // Read and hash cluster
                        readNextCluster(clusterIndex, readCluster);
                        readCluster.rewind();
                        final byte[] hashCluster = ByteBufferDigest.digest(hashAlgorithm, readCluster);

                        // Create message element and add it
                        final NrsClusterHash.Builder hashBuilder = NrsClusterHash.newBuilder();
                        hashBuilder.setIndex(clusterIndex);
                        hashBuilder.setHash(ByteString.copyFrom(hashCluster));
                        builder.addClusters(hashBuilder.build());
                    }
                }
                finally {
                    NrsByteBufferCache.release(readCluster);
                }

                // Ready for update
                update = true;
                updateAborted = false;
                updateH1 = false;
                updateCondition = ioLock.newCondition();
            }
            finally {
                ioLock.unlock();
            }

            final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
            opBuilder.setNrsFileMapping(builder.build());
            return opBuilder;
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Reset the 'update in progress' state. Used as a safety belt if the update fails for some reason.
     */
    public final void resetUpdate() {
        ioLock.lock();
        try {
            if (update) {
                updateAborted = true;
            }
            update = false;
            updateH1 = false;
            updateCondition = null;
        }
        finally {
            ioLock.unlock();
        }
    }

    /**
     * Tells if the last update of the file was aborted. Returns true if no attempt was made for this instance.
     * 
     * @return <code>true</code> if the file update was aborted.
     */
    public final boolean isLastUpdateAborted() {
        ioLock.lock();
        try {
            return updateAborted;
        }
        finally {
            ioLock.unlock();
        }
    }

    /**
     * Wait for the end of the update.
     * 
     * @param time
     * @param unit
     * @return <code>true</code> if the update is still in progress
     */
    public final boolean waitUpdateEnd(final long time, final TimeUnit unit) {
        ioLock.lock();
        try {
            if (update) {
                try {
                    updateCondition.await(time, unit);
                }
                catch (final InterruptedException e) {
                    LOGGER.warn(header.getFileId() + ": update wait interrupted", e);
                }
            }
            return update;
        }
        finally {
            ioLock.unlock();
        }
    }

    /**
     * Process {@link NrsFileMapping} to send cluster updates to the given peer. The file processes groups of clusters
     * under open and IO locks.
     * <p>
     * This operation may take some times, depending on the size of the {@link NrsFile}.
     * 
     * @param nrsFileMapping
     *            contents of the remote file
     * @param peer
     *            destination of cluster updates.
     * @throws IOException
     */
    public final void processNrsFileSync(final NrsFileMapping nrsFileMapping, final UUID peer) throws IOException {
        boolean aborted = true;
        final UuidT<U> nrsFileUuid = header.getFileId();
        final List<NrsClusterHash> peerClusterHashs = nrsFileMapping.getClustersList();
        final int peerClusterHashsCount = peerClusterHashs.size();
        int nextPeerClusterHashsIdx = 0; // Index in peerClusterHashs TODO: overflow for large files?
        boolean prevPeerClusterHashsFound = true;

        final ByteBuffer readCluster = NrsByteBufferCache.allocate(clusterSize); // Allocate buffer once for all
        try {

            postOffice.initPeerSync(nrsFileUuid, peer);

            long nextClusterIdx = h1LastClusterIdx + 1; // First cluster to process in this file
            try {

                // Send H1Header
                boolean goOn = sendH1Header(nrsFileUuid, peer);

                mainloop: while (goOn) {
                    openLock.readLock().lock();
                    try {

                        // Abort loop if the file have been closed
                        if (!opened) {
                            break;
                        }

                        // Do not keep the open lock for too long
                        for (int i = 0; i < 10; i++) {
                            // Lock L1 and L2 tables contents (version, clusters and file size)
                            ioLock.lock();
                            try {
                                // Last cluster processed?
                                if (backendFileChannel.size() <= (nextClusterIdx * clusterSize)) {
                                    aborted = false;
                                    goOn = false;
                                    continue mainloop;
                                }

                                // Look for the given cluster in the peer hashes
                                final NrsClusterHash peerNrsClusterHash;
                                if (prevPeerClusterHashsFound) {
                                    // Look in the map, starting from the element following the last one
                                    boolean found = false;
                                    NrsClusterHash tmpNrsClusterHash = null;
                                    for (int j = peerClusterHashsCount - 1; j >= 0; j--) {
                                        tmpNrsClusterHash = peerClusterHashs.get(nextPeerClusterHashsIdx);
                                        nextPeerClusterHashsIdx = (nextPeerClusterHashsIdx + 1) % peerClusterHashsCount;
                                        if (tmpNrsClusterHash.getIndex() == nextClusterIdx) {
                                            found = true;
                                            break;
                                        }
                                    }
                                    prevPeerClusterHashsFound = found;
                                    peerNrsClusterHash = found ? tmpNrsClusterHash : null;
                                }
                                else {
                                    // Peer file is smaller
                                    peerNrsClusterHash = null;
                                }

                                if (processCluster(nextClusterIdx, peerNrsClusterHash, readCluster)) {
                                    readCluster.rewind();
                                    postOffice.postNrsCluster(nrsFileUuid, peer, nextClusterIdx, readCluster);
                                }

                                // Prepare next iteration
                                nextClusterIdx++;
                            }
                            finally {
                                ioLock.unlock();
                            }
                        }
                    }
                    finally {
                        openLock.readLock().unlock();
                    }
                }
            }
            finally {
                ioLock.lock();
                try {
                    postOffice.finiPeerSync(nrsFileUuid, peer, aborted);
                }
                finally {
                    ioLock.unlock();
                }
            }
        }
        finally {
            NrsByteBufferCache.release(readCluster);
        }
    }

    /**
     * Read a cluster and check if the contents must be sent to the peer.
     * 
     * @param clusterIdx
     * @param peerNrsClusterHash
     * @param readCluster
     * @return <code>true</code> if the cluster must be sent to the peer.
     * @throws IOException
     */
    private final boolean processCluster(final long clusterIdx, final NrsClusterHash peerNrsClusterHash,
            final ByteBuffer readCluster) throws IOException {
        // Read local cluster
        backendFileChannel.position(clusterIdx * clusterSize);
        readNextCluster(clusterIdx, readCluster);
        readCluster.rewind();

        // Check peer cluster sum
        final boolean clusterMatch;
        if (peerNrsClusterHash == null) {
            // Peer have a smaller file
            clusterMatch = false;
        }
        else {
            try {
                clusterMatch = ByteBufferDigest.match(readCluster, peerNrsClusterHash.getHash().toByteArray());
            }
            catch (final NoSuchAlgorithmException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return !clusterMatch;
    }

    /**
     * Update this {@link NrsFile} according to the given message.
     * 
     * @param nrsFileUpdate
     * @throws IOException
     */
    public final void handleNrsFileUpdate(final NrsRemote.NrsFileUpdate nrsFileUpdate) throws IOException {
        final List<NrsUpdate> nrsUpdates = nrsFileUpdate.getUpdatesList();
        openLock.readLock().lock();
        try {
            // Opened?
            if (!opened) {
                throw new IllegalStateException("'" + mappedFile + "' not opened");
            }
            // Read-only?
            if (readOnly) {
                throw new IllegalStateException("'" + mappedFile + "' read-only");
            }

            final boolean broadcastUpdates = nrsFileUpdate.getBroadcast();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(getDescriptor().getFileId() + ": " + nrsUpdates.size() + " updates, version="
                        + getVersion() + ", broadcast=" + broadcastUpdates);
            }

            ioLock.lock();
            try {
                // Should get non-broadcast messages unless an update is in progress
                if (!update && !broadcastUpdates) {
                    LOGGER.warn(getDescriptor().getFileId() + ": ignore update messages");
                    return;
                }

                // Ignore broadcast messages if an update is in progress
                if (update && broadcastUpdates) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getDescriptor().getFileId() + ": ignore broadcast messages version="
                                + getVersion());
                    }
                    return;
                }
            }
            finally {
                ioLock.unlock();
            }

            for (int i = 0; i < nrsUpdates.size(); i++) {
                final NrsUpdate nrsUpdate = nrsUpdates.get(i);

                // Key update
                if (nrsUpdate.hasKeyUpdate()) {
                    // Set or reset a key
                    final NrsKey keyUpdate = nrsUpdate.getKeyUpdate();
                    final long blockIndex = keyUpdate.getBlockIndex();

                    // Write key or reset
                    ioLock.lock();
                    try {
                        // Version already reached?
                        if (broadcastUpdates && getVersion() >= keyUpdate.getVersion()) {
                            // Ignore this update (already applied)
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace(getDescriptor().getFileId() + ": ignore broadcast=" + broadcastUpdates
                                        + ", update=" + update + " version=" + getVersion() + ", keyVers="
                                        + keyUpdate.getVersion());
                            }
                            continue;
                        }
                        if (update && !updateH1) {
                            // Ignore this update (already applied)
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace(getDescriptor().getFileId() + ": ignore (header update) broadcast="
                                        + broadcastUpdates + ", update=" + update + " version=" + getVersion()
                                        + ", keyVers=" + keyUpdate.getVersion());
                            }
                            continue;
                        }
                        if (update)
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace(getDescriptor().getFileId() + ": do broadcast=" + broadcastUpdates
                                        + ", update=" + update + " version=" + getVersion() + ", keyVers="
                                        + keyUpdate.getVersion());
                            }

                        final NrsKeyHeader nrsKeyHeader = keyUpdate.getHeader();
                        if (nrsKeyHeader == NrsKeyHeader.NOT_ALLOCATED) {
                            writeHash(blockIndex, null, false);
                        }
                        else if (nrsKeyHeader == NrsKeyHeader.ALLOCATED) {
                            assert keyUpdate.hasKey();
                            final T key = decodeValue(keyUpdate);
                            writeHash(blockIndex, key, false);
                        }
                        else if (nrsKeyHeader == NrsKeyHeader.TRIMMED) {
                            writeHash(blockIndex, TRIMMED_VALUE, false);
                        }
                        else {
                            throw new AssertionError("nrsKeyHeader=" + nrsKeyHeader);
                        }
                    }
                    finally {
                        ioLock.unlock();
                    }
                }

                // Cluster udpate
                if (nrsUpdate.hasClusterUpdate()) {
                    assert !broadcastUpdates;

                    // Get write information
                    final NrsCluster nrsCluster = nrsUpdate.getClusterUpdate();
                    final long clusterIndex = nrsCluster.getIndex();
                    if (clusterIndex > h1LastClusterIdx) {
                        final ByteBuffer cluster = nrsCluster.getContents().asReadOnlyByteBuffer();

                        ioLock.lock();
                        try {
                            writeCluster(clusterIndex, cluster);
                        }
                        finally {
                            ioLock.unlock();
                        }
                    }
                    else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(header.getFileId() + ": received update for H1 cluster (index=", +clusterIndex
                                    + ", last index=" + h1LastClusterIdx);
                        }
                    }
                }

                // H1Header update
                if (nrsUpdate.hasH1HeaderUpdate()) {
                    assert !broadcastUpdates;

                    // Get write information
                    final NrsH1Header h1HeaderUpdate = nrsUpdate.getH1HeaderUpdate();
                    final ByteBuffer writeHeader = h1HeaderUpdate.getHeader().asReadOnlyByteBuffer();
                    ioLock.lock();
                    try {
                        writeH1Header(writeHeader);
                        updateH1 = true;
                    }
                    finally {
                        ioLock.unlock();
                    }
                }
            }

            // End of update?
            ioLock.lock();
            try {
                if (update && nrsFileUpdate.hasEos()) {
                    assert nrsFileUpdate.getEos();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(getDescriptor().getFileId() + ": end of update, version=" + getVersion()
                                + ", aborted=" + nrsFileUpdate.getAborted());
                    }
                    update = false;
                    updateAborted |= nrsFileUpdate.getAborted();
                    updateH1 = false;
                    updateCondition.signalAll();
                    updateCondition = null;
                }
            }
            finally {
                ioLock.unlock();
            }

        }
        finally {
            openLock.readLock().unlock();
        }
    }

    /**
     * Reads and sends the file H1 header.
     * 
     * @param readH1Header
     *            buffer to fill
     * @return <code>true</code> if the update can go on
     * @throws IOException
     */
    private final boolean sendH1Header(final UuidT<U> nrsFileUuid, final UUID peer) throws IOException {
        final ByteBuffer readHeader;
        openLock.readLock().lock();
        try {
            // Abort loop if the file have been closed
            if (!opened) {
                return false;
            }

            // Send H1Header
            ioLock.lock();
            try {
                readHeader = NrsByteBufferCache.allocate((int) h1Size);
                try {
                    // Prepare read, set file position
                    backendFileChannel.position(header.getH1Address());

                    // Should be able to read the whole cluster at once
                    final int read = backendFileChannel.read(readHeader);
                    if (read == -1) {
                        throw new IOException("Unexpected end of file '" + mappedFile + "' readOffset="
                                + backendFileChannel.position());
                    }
                    if (read != h1Size) {
                        throw new IOException("Unexpected read length of file '" + mappedFile + "': readOffset="
                                + backendFileChannel.position() + ", h1Size=" + h1Size + ", readLen=" + read);
                    }

                    // Must send the buffer under lock to keep version order
                    readHeader.rewind();
                    postOffice.postNrsHeader(nrsFileUuid, peer, readHeader);

                }
                finally {
                    NrsByteBufferCache.release(readHeader);
                }
            }
            finally {
                ioLock.unlock();
            }
        }
        finally {
            openLock.readLock().unlock();
        }

        return true;
    }

    /**
     * Writes the H1Header contents at its position.
     * 
     * @param writeHeader
     *            buffer to read
     * @throws IOException
     */
    private final void writeH1Header(final ByteBuffer writeHeader) throws IOException {
        assert ioLock.isHeldByCurrentThread();

        // Prepare write and check file position
        writeHeader.rewind();
        assert writeHeader.remaining() == h1Size;

        backendFileChannel.position(header.getH1Address());

        // Should be able to write the whole cluster at once
        final int written = backendFileChannel.write(writeHeader);
        if (written != h1Size) {
            throw new IOException("Unexpected write length '" + mappedFile + "': writeOffset="
                    + backendFileChannel.position() + ", size=" + backendFileChannel.size() + ", h1Size=" + h1Size
                    + ", written=" + written);
        }

        // Need to update the file version
        h1Header.loadVersion();

        // Update the size of the file to the max L2 allocation
        // Optimization: read from memory instead of h1header
        h1Header.prepareIterator(writeHeader);
        final long currentFileSize = backendFileChannel.size();
        long fileSize = currentFileSize;
        while (writeHeader.hasRemaining()) {
            final long l2Addr = writeHeader.getLong();
            if (l2Addr > 0) {
                final long l2End = l2Addr + clusterSize;
                if (l2End > fileSize) {
                    fileSize = l2End;
                }
            }
        }
        if (fileSize > currentFileSize) {
            updateFileSize(fileSize);
        }
    }

    /**
     * Reads the file contents for the given cluster.
     * 
     * @param clusterIndex
     *            cluster index
     * @param readCluster
     *            buffer to fill
     * @throws IOException
     */
    private final void readNextCluster(final long clusterIndex, final ByteBuffer readCluster) throws IOException {
        assert ioLock.isHeldByCurrentThread();

        // Prepare read and check file position
        readCluster.rewind();
        assert backendFileChannel.position() == (clusterIndex * clusterSize);

        // Should be able to read the whole cluster at once
        final int read = backendFileChannel.read(readCluster);
        if (read == -1) {
            throw new IOException("Unexpected end of file '" + mappedFile + "' readOffset="
                    + backendFileChannel.position());
        }
        if (read != clusterSize) {
            throw new IOException("Unexpected read length of file '" + mappedFile + "': readOffset="
                    + backendFileChannel.position() + ", clusterSize=" + clusterSize + ", readLen=" + read);
        }
    }

    /**
     * Writes the given cluster contents at the given position.
     * 
     * @param clusterIndex
     * @param writeCluster
     * @throws IOException
     */
    private final void writeCluster(final long clusterIndex, final ByteBuffer writeCluster) throws IOException {
        assert ioLock.isHeldByCurrentThread();

        // Prepare write, set file position
        writeCluster.rewind();
        backendFileChannel.position(clusterIndex * clusterSize);

        // Should be able to write the whole cluster at once
        final int written = backendFileChannel.write(writeCluster);
        if (written != clusterSize) {
            throw new IOException("Unexpected write length '" + mappedFile + "': writeOffset="
                    + backendFileChannel.position() + ", size=" + backendFileChannel.size() + ", clusterSize="
                    + clusterSize + ", written=" + written);
        }
    }

    /**
     * Reads a hash value at the given offset from persistent storage.
     * 
     * @param blockIndex
     *            the index of the block to read
     * @return the hash value stored for this block or <code>null</code> if not present
     * @throws IndexOutOfBoundsException
     *             if <code>blockIndex</code> is out of the item scope.
     */
    public final T read(final long blockIndex) throws IndexOutOfBoundsException, IOException {
        return read(blockIndex, null);
    }

    /**
     * Reads the contents of the block and writes it in <code>dest</code> if the value is not <code>null</code>.
     * 
     * @param blockIndex
     *            the index of the block to read
     * @param dest
     *            destination buffer of <code>null</code>
     * @return <code>dest</code> or a new allocated element.
     * @throws IndexOutOfBoundsException
     *             if <code>blockIndex</code> is out of the item scope.
     */
    final T read(final long blockIndex, final T dest) throws IndexOutOfBoundsException, IOException {
        openLock.readLock().lock();
        try {
            // Opened?
            if (!opened) {
                throw new IllegalStateException("'" + mappedFile + "' not opened");
            }
            // Check input parameter
            this.rangeCheck(blockIndex);

            final int l1Offset = getL1Offset(blockIndex);
            final int l2Index = getL2Index(blockIndex);
            rangeCheckL2(l2Index);

            final long l2Address;
            ioLock.lock();
            try {
                l2Address = h1Header.readL2Address(l1Offset);
            }
            finally {
                ioLock.unlock();
            }
            if (l2Address == 0L) {
                return null;
            }

            // Read from file
            final int elementSize = getElementSize();
            final T result = dest == null ? newElement() : dest;
            final long readOffset = l2Address + l2Index * (1 + elementSize);

            final byte hashValue;
            ioLock.lock();
            try {
                hashValue = readHash(result, readOffset);
            }
            finally {
                ioLock.unlock();
            }
            if (hashValue == HASH_ALLOCATED_VALUE) {
                if (LOGGER.isTraceEnabled()) {
                    final StringBuilder trace = new StringBuilder("R blockIndex=").append(blockIndex)
                            .append(" readOffset=").append(readOffset).append(" value=");
                    appendDebugString(trace, result);
                    LOGGER.trace(trace.toString());
                }
                return result;
            }
            else if (hashValue == HASH_NOT_ALLOCATED_VALUE) {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace("R blockIndex=" + blockIndex + " readOffset=" + readOffset + " value=<null>");
                return null;
            }
            else if (hashValue == HASH_TRIMMED_VALUE) {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace("R blockIndex=" + blockIndex + " readOffset=" + readOffset + " value=<trim>");
                return TRIMMED_VALUE;
            }
            else {
                LOGGER.warn("R blockIndex=" + blockIndex + " readOffset=" + readOffset + " hashValue=" + hashValue);
                throw new AssertionError();
            }
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    private final byte readHash(final T result, final long readOffset) throws IOException {
        if (backendFileMappedBuffer != null) {
            return readHashMappedBuffer(result, readOffset);
        }
        else {
            return readHashFromChannel(result, readOffset);
        }
    }

    private final byte readHashMappedBuffer(final T result, final long readOffset) throws IOException {
        // Mapped only if file size < max integer value
        backendFileMappedBuffer.position((int) readOffset);
        final byte header = backendFileMappedBuffer.get();
        if (header == HASH_NOT_ALLOCATED_VALUE || header == HASH_TRIMMED_VALUE) {
            // Not allocated or trimmed
            return header;
        }
        assert header == HASH_ALLOCATED_VALUE;
        readFully(backendFileMappedBuffer, result);
        return header;
    }

    private final byte readHashFromChannel(final T result, final long readOffset) throws IOException {
        backendFileChannel.position(readOffset);

        // Read flag header
        {
            IS_ALLOCATED.rewind();
            final int read = backendFileChannel.read(IS_ALLOCATED);
            if (read != 1) {
                throw new IOException("Read failed '" + mappedFile + "' readOffset=" + backendFileChannel.position()
                        + " read=" + read);
            }
        }
        IS_ALLOCATED.rewind();
        final byte header = IS_ALLOCATED.get();
        if (header == HASH_NOT_ALLOCATED_VALUE || header == HASH_TRIMMED_VALUE) {
            // Not allocated or trimmed
            return header;
        }
        assert header == HASH_ALLOCATED_VALUE;
        readFully(backendFileChannel, result);
        return header;
    }

    /**
     * Writes the given hash value to the persistent storage.
     * 
     * @param blockIndex
     *            the number of the block to write
     * @param hashValue
     *            the hash value to write
     * @throws NrsException
     * @throws IndexOutOfBoundsException
     *             if <code>blockIndex</code> is out of the item scope.
     */
    public final void write(final long blockIndex, @Nonnull final T hashValue) throws IOException {
        // Check hash length (and NPE if hashValue is null)
        checkValueLength(hashValue);
        writeHash(blockIndex, hashValue, true);
    }

    /**
     * Release the block given by its index. Does nothing if the block is not allocated.
     * 
     * @param blockIndex
     *            the number of the block to reset
     * @throws NrsException
     * @throws IndexOutOfBoundsException
     *             if <code>blockIndex</code> is out of the item scope.
     */
    public final void reset(final long blockIndex) throws IOException {
        writeHash(blockIndex, null, true);
    }

    /**
     * Mark the block given by its index as trimmed. Does nothing if the block is not allocated.
     * 
     * @param blockIndex
     *            the number of the block to trim
     * @throws NrsException
     * @throws IndexOutOfBoundsException
     *             if <code>blockIndex</code> is out of the item scope.
     */
    public final void trim(final long blockIndex) throws IOException {
        writeHash(blockIndex, TRIMMED_VALUE, true);
    }

    /**
     * Write the hash value into the given block. Reset the value if hashValue is <code>null</code>.
     * 
     * @param blockIndex
     * @param hashValue
     *            the hash to write or <code>null</code> to 'release' the block or the 'trim' constant.
     * @throws IOException
     */
    private final void writeHash(final long blockIndex, final T hashValue, final boolean notify) throws IOException {
        openLock.readLock().lock();
        try {
            // Opened?
            if (!opened) {
                throw new IllegalStateException("'" + mappedFile + "' not opened");
            }
            // Read-only?
            if (readOnly) {
                throw new IllegalStateException("'" + mappedFile + "' read-only");
            }

            // Check input parameters
            rangeCheck(blockIndex);

            // Compute position in L2 table
            final int l1Offset = getL1Offset(blockIndex);
            final int l2Index = getL2Index(blockIndex);
            rangeCheckL2(l2Index);

            // Get L2 address
            long l2Address;
            ioLock.lock();
            try {
                l2Address = h1Header.readL2Address(l1Offset);
                if (l2Address == 0) {
                    // Reset or trim: no need to allocate a new L2 area
                    if (hashValue == null || hashValue == TRIMMED_VALUE) {
                        return;
                    }

                    // The contents of the file changes, but no need to increment the file version as we have not
                    // written the hash yet
                    l2Address = allocateNewL2();
                    h1Header.writeL2Address(l1Offset, l2Address);
                }
            }
            finally {
                ioLock.unlock();
            }

            // Write at l2Address
            final long writeOffset = l2Address + l2Index * (1 + getElementSize());
            if (LOGGER.isTraceEnabled()) {
                if (hashValue == null) {
                    LOGGER.trace("W blockIndex=" + blockIndex + " writeOffset=" + writeOffset + " value=<reset>");
                }
                else if (hashValue == TRIMMED_VALUE) {
                    LOGGER.trace("W blockIndex=" + blockIndex + " writeOffset=" + writeOffset + " value=<trim>");
                }
                else {
                    final StringBuilder trace = new StringBuilder("W blockIndex=").append(blockIndex)
                            .append(" writeOffset=").append(writeOffset).append(" value=");
                    appendDebugString(trace, hashValue);
                    LOGGER.trace(trace.toString());
                }
            }
            ioLock.lock();
            try {
                writeHashToChannel(hashValue, writeOffset);

                // Optionally notify peers
                if (notify && postOffice != null) {
                    wasWritten.set(true);
                    final NrsKeyHeader keyHeader;
                    if (hashValue == null) {
                        keyHeader = NrsKeyHeader.NOT_ALLOCATED;
                    }
                    else if (hashValue == TRIMMED_VALUE) {
                        keyHeader = NrsKeyHeader.TRIMMED;
                    }
                    else {
                        keyHeader = NrsKeyHeader.ALLOCATED;
                    }
                    postOffice.postNrsKey(header.getFileId(), h1Header.getVersion(), blockIndex, keyHeader, hashValue);
                }
            }
            finally {
                ioLock.unlock();
            }
        }
        finally {
            openLock.readLock().unlock();
        }
    }

    private final void writeHashToChannel(final T hashValue, final long writeOffset) throws IOException {
        // Contents of the file will change: update version first, to avoid failure in changing the file version after
        // the change of the contents
        h1Header.incrVersion();

        // Write flag allocated or released
        backendFileChannel.position(writeOffset);
        if (hashValue == null) {
            NOT_ALLOCATED.rewind();
            backendFileChannel.write(NOT_ALLOCATED);
        }
        else if (hashValue == TRIMMED_VALUE) {
            TRIMMED.rewind();
            backendFileChannel.write(TRIMMED);
        }
        else {
            ALLOCATED.rewind();
            backendFileChannel.write(ALLOCATED);
            writeFully(backendFileChannel, hashValue);
        }
    }

    /**
     * Gets the header for this file.
     * 
     * @return the header of the file.
     */
    public final NrsFileHeader<U> getDescriptor() {
        return header;
    }

    /**
     * Gets the total currently allocated number of records, i.e. the maximum number of hashes stored at this moment.
     * 
     * @return the allocated number of L2 table entries
     */
    // TODO: improve this metric
    public final int getAllocatedNumberOfRecords() {
        return /* this.l2Tables.size() * */this.l2capacity;
    }

    /**
     * Gets the {@link Path} to the file represented by this instance.
     * 
     * @return the absolute {@link Path} of the file
     */
    final Path getFile() {
        return mappedFile.toPath();
    }

    /**
     * Get and reset the 'wasWritten' flag.
     * 
     * @return <code>true</code> if the file have notify write operations since the last call of this method.
     */
    final boolean wasWritten() {
        return wasWritten.getAndSet(false);
    }

    /**
     * Gets the L1 table offset from the given block offset.
     * 
     * @param blockIndex
     *            the block offset in number of blocks
     * @return index of the l2table in the L1 table
     */
    private final int getL1Offset(final long blockIndex) {
        final long result = blockIndex / l2capacity;
        return (int) result;
    }

    /**
     * Gets the L2Table offset from the given block offset.
     * 
     * @param blockIndex
     *            the block offset in number of blocks
     * @return the offset in number of entries of the L2Table
     */
    private final int getL2Index(final long blockIndex) {
        final long result = blockIndex % l2capacity;
        return (int) result;
    }

    /**
     * Checks if the given block index is within the file limits.
     * 
     * @param blockIndex
     *            the block index (starting from 0) to check
     */
    private final void rangeCheck(final long blockIndex) throws IndexOutOfBoundsException {
        if (blockIndex < 0 || blockIndex > blockIndexMax) {
            throw new IndexOutOfBoundsException("Index=" + blockIndex + " IndexMax=" + blockIndexMax);
        }
    }

    /**
     * Checks if the l2 table index offset is within the L2Table capacity limits.
     * 
     * @param l2Index
     *            the entry offset (starting from 0) to check
     */
    private final void rangeCheckL2(final int l2Index) {
        if ((l2Index < 0) || (l2Index >= this.l2capacity)) {
            throw new AssertionError("L2 index=" + l2Index + ", max index=" + (l2capacity - 1));
        }
    }

    /**
     * Allocates and references a new L2Table.
     * 
     * The newly created L2Table is added to the l2Table and referenced by the {@link #h1Header}.
     * 
     * @return the address value at which the newly created table is referenced in the l2Table
     * @throws IOException
     *             if allocation proves impossible
     */
    private final long allocateNewL2() throws IOException {
        // Extends the file
        final long newTableAddress = backendFileChannel.size();
        updateFileSize(newTableAddress + clusterSize);
        return newTableAddress;
    }

    /**
     * Sets the new size of the backend file.
     * 
     * @param fileSizeNew
     *            the file new size, must be a number of clusters.
     * @throws IOException
     */
    private final void updateFileSize(final long fileSizeNew) throws IOException {
        assert ioLock.isHeldByCurrentThread();

        if ((fileSizeNew % clusterSize) != 0) {
            throw new AssertionError("Size=" + fileSizeNew + ", clusterSize=" + clusterSize);
        }

        // Write a 0 at the end of the file to set the file size
        backendFileChannel.position(fileSizeNew - 1);
        EOF.rewind();
        backendFileChannel.write(EOF);
    }

    /**
     * Computes the size of the H1 header.
     * 
     * 
     * The value depends on:
     * <ul>
     * <li>{@link NrsFileHeader#getSize() size},</li>
     * <li>{@link NrsFileHeader#getBlockSize() block size},</li>
     * <li>{@link NrsFileHeader#getClusterSize() cluster size},</li>
     * <li>{@link #getElementSize() size of a read/written element}</li>
     * </ul>
     * 
     * @return size of the H1 header, in bytes.
     */
    private final long computeH1Size() {
        final int l2capacity = computeL2Capacity();
        final long deviceSize = getDescriptor().getSize();
        final int blockSize = getDescriptor().getBlockSize();

        // Max block count
        final long nbOfBlocks = deviceSize / blockSize;
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("Number of blocks: {}", Long.valueOf(nbOfBlocks));

        final long maxNbOfL2Tables = nbOfBlocks / l2capacity + ((nbOfBlocks % l2capacity == 0) ? 0 : 1);
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("Max nb of L2 tables: {}", Long.valueOf(maxNbOfL2Tables));

        // Version (long) + addresses of the L2 tables
        return NrsFileHeader.BYTES_PER_LONG + maxNbOfL2Tables * NrsFileHeader.BYTES_PER_LONG;
    }

    /**
     * Computes the L2 table capacity as number of entries.
     * 
     * 
     * Fields in the {@link NrsFileHeader} that must be set for the computation to succeed:
     * <ul>
     * <li>{@link NrsFileHeader#getClusterSize() cluster size},</li>
     * <li>{@link #getElementSize() size of a read/written element}</li>
     * </ul>
     * 
     * @return the computed value
     */
    final int computeL2Capacity() {
        final int l2RecSize = 1 + getElementSize(); // Allocated byte+value length
        return getDescriptor().getClusterSize() / l2RecSize;
    }

    /**
     * Gets the length of an element. This value is the length read or written.
     * 
     * @return the length of the element.
     */
    protected final int getElementSize() {
        return elementSize;
    }

    /**
     * Create a new empty element.
     * 
     * @return a new empty element.
     */
    abstract T newElement();

    /**
     * Throws an exception if the value is not as long as expected.
     * 
     * @param value
     */
    abstract void checkValueLength(@Nonnull T value) throws NullPointerException, IllegalArgumentException;

    /**
     * Append the debug information for the given value. Should write the first bytes of the value.
     * 
     * @param dst
     *            destination buffer.
     * @param value
     */
    abstract void appendDebugString(@Nonnull StringBuilder dst, @Nonnull T value);

    /**
     * Read the value from a coded message.
     * 
     * @param value
     *            message to decode.
     * @return the value read.
     */
    abstract T decodeValue(final NrsKey value);

    /**
     * Fills result with some contents of the source buffer.
     * 
     * @param src
     *            source buffer.
     * @param result
     *            area to fill.
     */
    abstract void readFully(@Nonnull MappedByteBuffer src, @Nonnull T result);

    /**
     * Fills result with some contents of the source channel.
     * 
     * @param src
     *            source channel.
     * @param result
     *            area to fill.
     */
    abstract void readFully(@Nonnull FileChannel src, @Nonnull T result) throws IOException;

    /**
     * Writes the value to the destination channel.
     * 
     * @param dst
     *            destination channel.
     * @param value
     *            value to write.
     * @throws IOException
     */
    abstract void writeFully(@Nonnull FileChannel dst, @Nonnull T value) throws IOException;

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + mappedFile + "]";
    }
}
