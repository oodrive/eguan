package com.oodrive.nuage.vvr.repository.core.api;

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

import static com.oodrive.nuage.utils.ByteBuffers.ALLOCATE_DIRECT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.hash.ByteBufferDigest;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.ibs.IbsException;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.utils.ByteBufferCache;
import com.oodrive.nuage.vvr.repository.core.api.AbstractDeviceImplHelper.BlockKeyLookupEx;
import com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle;

/**
 * Reference implementation of a {@link ReadWriteHandle}. Utility class for the {@link Device} implementations.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 */
abstract class DeviceReadWriteHandleImpl implements ReadWriteHandle {

    /** Operation performed by the IoTask */
    enum IoTaskOpe {
        /** Read */
        READ,
        /** Write */
        WRITE,
        /** Trim */
        TRIM;
    }

    /**
     * Elementary read, write or trim operation.
     * 
     */
    private abstract class IoTask implements Callable<Void> {
        /** Data source or destination */
        protected final ByteBuffer data;
        /** Offset in data */
        protected final int dataOffset;
        /** Operation to perform */
        protected final IoTaskOpe ope;
        /** Index of the block in the NRS file */
        protected final long blockIndex;
        /** <code>true</code> when the IO request contains a single task */
        protected final boolean singleTask;
        /** Result of the lookup of the key for the current block. Needed to revert a write operation */
        protected BlockKeyLookupEx blockKeyLookupEx;
        /** block transaction id */
        protected int txId;
        /** Optional builder for the peer notification of block changes */
        @GuardedBy(value = "blockOpBuilder")
        protected VvrRemote.RemoteOperation.Builder blockOpBuilder;

        IoTask(final ByteBuffer data, final int dataOffset, final IoTaskOpe ope, final long blockIndex,
                final boolean singleTask) {
            super();
            this.data = data == null ? null : data.duplicate();
            this.dataOffset = dataOffset;
            this.ope = ope;
            this.blockIndex = blockIndex;
            this.singleTask = singleTask;
        }

        final void setTxId(final int txId) {
            this.txId = txId;
        }

        final void setBlockOpBuilder(final VvrRemote.RemoteOperation.Builder blockOpBuilder) {
            this.blockOpBuilder = blockOpBuilder;
        }

        /**
         * Write the given to data. The position and the limit of the buffer must be set in the source.
         * 
         * @param source
         *            data to read.
         */
        final void writeToData(final ByteBuffer source) {
            data.position(dataOffset);
            data.limit(dataOffset + source.remaining());
            data.put(source);
        }

        /**
         * Fill the destination buffer with the contents of data.
         * 
         * @param dest
         */
        final void readFromData(final ByteBuffer dest) {
            data.position(dataOffset);
            data.limit(dataOffset + dest.remaining());
            dest.put(data);
        }

        /**
         * Revert the Nrs changes performed by the IO task. Does nothing if <code>blockKeyLookupEx</code> is
         * <code>null</code>.
         * 
         * @throws IOException
         */
        final void revertNrs() throws IOException {
            if (blockKeyLookupEx == null) {
                // Nothing to revert
                return;
            }
            restoreKey(blockIndex, blockKeyLookupEx);
        }
    }

    /**
     * Read, write or trim a whole block.
     * 
     * 
     */
    private final class FullIoTask extends IoTask {

        FullIoTask(final ByteBuffer data, final int dataOffset, final IoTaskOpe ope, final long blockIndex,
                final boolean singleTask) {
            super(data, dataOffset, ope, blockIndex, singleTask);
        }

        @Override
        public final Void call() throws Exception {
            // Look for the block, among parents for the read
            final byte[] oldKey;
            if (ope == IoTaskOpe.WRITE && singleTask && !canReplaceOldKey()) {
                // Single write and no replace: replace does not replace, so no need to look for the previous block
                oldKey = null;
            }
            else {
                // Must save the old key in case of revert, no need for a recursive search for write
                blockKeyLookupEx = lookupBlockKeyEx(blockIndex, ope == IoTaskOpe.READ);
                if (blockKeyLookupEx == null) {
                    // Not found, still will have to reset key on revert
                    blockKeyLookupEx = BlockKeyLookupEx.NOT_FOUND;
                    oldKey = null;
                }
                else {
                    oldKey = blockKeyLookupEx.getKey();
                    assert ope == IoTaskOpe.READ || blockKeyLookupEx.isSourceCurrent();
                }
            }

            if (ope == IoTaskOpe.READ) {
                if (oldKey == null || oldKey == NrsFile.HASH_TRIMMED) {
                    // Fill with 0
                    final ByteBuffer source = blockZero.duplicate();
                    assert source.position() == 0;
                    assert source.limit() == blockSize;
                    assert source.capacity() == blockSize;

                    writeToData(source);
                }
                else {
                    // Load block
                    // The native code can not make concurrent accesses to a HeapByteBuffer (native access to
                    // underlying byte array)
                    if (singleTask || data.isDirect()) {
                        // Can safely write into data
                        fillBlock(oldKey, data, dataOffset, blockKeyLookupEx.getSourceNode());
                    }
                    else {
                        final ByteBuffer source = getBlock(oldKey, blockKeyLookupEx.getSourceNode(), true);
                        try {
                            source.rewind();
                            writeToData(source);
                        }
                        finally {
                            releaseBlock(source);
                        }
                    }
                }
            }
            else if (ope == IoTaskOpe.WRITE) {
                // Write: store the new block
                storeBlock(data, dataOffset, blockIndex, oldKey, txId, blockOpBuilder);
            }
            else if (ope == IoTaskOpe.TRIM) {
                // Trim the block
                trimBlock(blockIndex);
            }
            else {
                throw new AssertionError("ope=" + ope);
            }
            return null;
        }
    }

    /**
     * Read, write or trim a part of a block.
     * 
     */
    private final class PartialIoTask extends IoTask {

        final private int ioBlkOffset;
        final private int ioBlkLength;

        PartialIoTask(final ByteBuffer data, final int dataOffset, final IoTaskOpe ope, final long blockIndex,
                final boolean singleTask, final int ioBlkOffset, final int ioBlkLength) {
            super(data, dataOffset, ope, blockIndex, singleTask);
            this.ioBlkOffset = ioBlkOffset;
            this.ioBlkLength = ioBlkLength;
        }

        @Override
        public final Void call() throws Exception {
            // Can not trim part of a block
            if (ope == IoTaskOpe.TRIM) {
                // Nothing to do
                return null;
            }
            // Read or write operation
            final boolean read = ope == IoTaskOpe.READ;

            // Look for the block. Need the previous block for write
            final byte[] oldKey;
            blockKeyLookupEx = lookupBlockKeyEx(blockIndex, true);
            if (blockKeyLookupEx == null) {
                // Not found, still will have to reset key on revert
                blockKeyLookupEx = BlockKeyLookupEx.NOT_FOUND;
                oldKey = null;
            }
            else {
                oldKey = blockKeyLookupEx.getKey();
            }

            if ((oldKey == null || oldKey == NrsFile.HASH_TRIMMED) && read) {
                // Write 0 to caller buffer, can ignore ioBlkOffset
                final ByteBuffer source = blockZero.duplicate();
                assert source.position() == 0;
                assert source.limit() == blockSize;
                assert source.capacity() == blockSize;

                source.limit(ioBlkLength);
                writeToData(source);
                return null;
            }

            // Get the previous block
            final ByteBuffer prevBlock;
            if (oldKey == null || oldKey == NrsFile.HASH_TRIMMED) {
                // Case write: allocate a new buffer filled with 0
                // TODO: really need to clear the block?
                assert !read;
                prevBlock = allocateBlock(true);
            }
            else {
                prevBlock = getBlock(oldKey, blockKeyLookupEx.getSourceNode(), read);
            }

            try {
                // Set block window
                prevBlock.position(ioBlkOffset);
                prevBlock.limit(ioBlkOffset + ioBlkLength);

                if (read) {
                    writeToData(prevBlock);
                }
                else {
                    // Get data and store buffer. No lock here
                    readFromData(prevBlock);
                    // Write the whole block. No replace if the oldKey is from a previous snapshot
                    storeBlock(prevBlock, 0, blockIndex, blockKeyLookupEx != null && blockKeyLookupEx.isSourceCurrent()
                            && oldKey != NrsFile.HASH_TRIMMED ? oldKey : null, txId, blockOpBuilder);
                }
            }
            finally {
                releaseBlock(prevBlock);
            }

            return null;
        }
    }

    /**
     * A {@link IoRequest} execute the {@link IoTask} and make sure that all or none are executed.
     * 
     */
    static final class IoRequest {
        /** Operation to perform */
        private final IoTaskOpe ope;
        /** offset in the device */
        private final long offset;
        /** IO length */
        private final int length;
        /** Tasks to perform */
        private final List<IoTask> ioTasks;

        private final DeviceReadWriteHandleImpl deviceReadWriteHandleImpl;

        /**
         * Create a new {@link IoRequest}
         * 
         * @param ope
         *            Operation to perform.
         * @param offset
         *            offset in device
         * @param length
         *            IO length
         * @param ioTasks
         *            {@link IoTask} to perform.
         * @param deviceReadWriteHandleImpl
         */
        IoRequest(@Nonnull final IoTaskOpe ope, final long offset, final int length, final List<IoTask> ioTasks,
                final DeviceReadWriteHandleImpl deviceReadWriteHandleImpl) {
            super();

            assert offset >= 0;
            assert length > 0;
            assert ioTasks == null || ioTasks.size() > 0; // ioTasks is null in unit tests

            this.ope = Objects.requireNonNull(ope);
            this.offset = offset;
            this.length = length;
            this.ioTasks = ioTasks;
            this.deviceReadWriteHandleImpl = deviceReadWriteHandleImpl;
        }

        final boolean isRead() {
            return ope == IoTaskOpe.READ;
        }

        final boolean isWrite() {
            return ope == IoTaskOpe.WRITE;
        }

        final boolean isTrim() {
            return ope == IoTaskOpe.TRIM;
        }

        /**
         * Executes the {@link IoRequest}.
         * 
         * @throws IOException
         */
        final void exec() throws IOException {

            // Update block notifications (null for a read or a trim request)
            final boolean write = isWrite();
            final VvrRemote.RemoteOperation.Builder blockOpBuilder = write
                    && deviceReadWriteHandleImpl.needsBlockOpBuilder() ? VvrRemote.RemoteOperation.newBuilder() : null;

            final int txId = (write && ioTasks.size() > 1) ? deviceReadWriteHandleImpl.createBlockTransaction() : -1;
            boolean done = false;

            // Ready to revert on any throwable
            try {

                // Add block IO parameters
                if (write) {
                    for (int i = ioTasks.size() - 1; i >= 0; i--) {
                        final IoTask ioTask = ioTasks.get(i);
                        ioTask.setTxId(txId);
                        ioTask.setBlockOpBuilder(blockOpBuilder);
                    }
                }

                // Single task in the current thread
                if (ioTasks.size() == 1) {
                    try {
                        ioTasks.get(0).call();
                    }
                    catch (final IOException e) {
                        throw e;
                    }
                    catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
                else if (SINGLE_THREADED) {
                    // Single threaded IOs
                    try {
                        for (int i = ioTasks.size() - 1; i >= 0; i--) {
                            final IoTask ioTask = ioTasks.get(i);
                            ioTask.call();
                        }
                    }
                    catch (final IOException e) {
                        throw e;
                    }
                    catch (final Exception e) {
                        throw new IOException(e);
                    }
                }
                else {
                    // Run all in the IO executor
                    try {
                        final List<Future<Void>> result = IO_EXEC.invokeAll(ioTasks);

                        // Get exceptions, if any
                        for (int i = result.size() - 1; i >= 0; i--) {
                            final Future<Void> future = result.get(i);
                            try {
                                future.get();
                            }
                            catch (final ExecutionException e) {
                                // Create new exception to get the full stack (thread exec and current thread)
                                throw new IOException(e.getCause());
                            }
                        }
                    }
                    catch (CancellationException | InterruptedException e) {
                        throw new IOException(e);
                    }
                }

                // Block: commit changes and notify peers
                if (txId > 0) {
                    deviceReadWriteHandleImpl.commitBlockTransaction(txId);
                    done = true;
                }
                if (blockOpBuilder != null) {
                    deviceReadWriteHandleImpl.notifyBlockIO(blockOpBuilder);
                }
            }
            catch (IOException | RuntimeException | Error e) {
                // Revert Nrs changes
                if (write || isTrim()) {
                    revertNrs();
                }
                throw e;
            }
            finally {
                // Rolls back block transaction on error
                if (txId > 0 && !done) {
                    deviceReadWriteHandleImpl.rollbackBlockTransaction(txId);
                }
            }
        }

        /**
         * Revert the Nrs changes made by the {@link IoTask}s of the request.
         */
        private final void revertNrs() {
            for (int i = ioTasks.size() - 1; i >= 0; i--) {
                final IoTask ioTask = ioTasks.get(i);
                try {
                    ioTask.revertNrs();
                }
                catch (final Exception e) {
                    LOGGER.warn("Failed to revert IO operation", e);
                }
            }
        }

        /**
         * Check if some bits are in common between this and the other {@link IoRequest}.
         * 
         * @param other
         * @return <code>true</code> if the two requests overlap
         */
        final boolean overlap(final IoRequest other) {
            if (offset == other.offset) {
                // Both length are >0
                return true;
            }
            else if (offset > other.offset) {
                if (offset < (other.offset + other.length)) {
                    return true;
                }
            }
            else {
                // other.offset > offset
                if (other.offset < (offset + length)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceReadWriteHandleImpl.class);

    /** ByteBufferCache to reuse {@link ByteBuffer}s, direct or non direct */
    private static final ByteBufferCache BYTE_BUFFER_CACHE = new ByteBufferCache(ALLOCATE_DIRECT ? 0
            : Integer.MAX_VALUE);

    /** Wait request timeout (in seconds) */
    private static final long DO_NOT_WAIT_FOREVER = 5;
    /** true to make single threaded IOs */
    private static final boolean SINGLE_THREADED = Boolean.getBoolean("com.oodrive.nuage.vvr.io.singleThreaded");

    /** Pool of blocks filled with 0. The key is the buffer size, the value is the block to read */
    private static final HashMap<Integer, ByteBuffer> BLOCK_POOL = new HashMap<>(4);

    /** Executor to execute IO requests in parallel */
    private static final ExecutorService IO_EXEC = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()), new ThreadFactory() {
                private int index = 0;

                @Override
                public final Thread newThread(final Runnable r) {
                    index++;
                    final Thread result = new Thread(r, "IOexec-" + index);
                    result.setPriority(Thread.NORM_PRIORITY + 3);
                    result.setDaemon(true);
                    return result;
                }
            });

    /** Link to the Device */
    protected final AbstractDeviceImplHelper deviceImplHelper;

    /** Hash algorithm to hash blocks. */
    private final HashAlgorithm hashAlgorithm;

    /** True if the device is opened read-only. */
    private final boolean readOnly;

    /** Block size of the device. Not private to avoid synthetic access from IO tasks. */
    protected final int blockSize;

    /** Block filled with 0. Not private to avoid synthetic access from inner classes */
    final ByteBuffer blockZero;

    /** <code>true</code> when the handle is closed */
    @GuardedBy(value = "closedLock")
    private boolean closed = false;
    private final ReadWriteLock closedLock = new ReentrantReadWriteLock();
    /** List of {@link IoRequest} to execute */
    @GuardedBy(value = "pendingIoRequestsLock")
    private final List<IoRequest> pendingIoRequests = new ArrayList<>();
    private final ReadWriteLock pendingIoRequestsLock = new ReentrantReadWriteLock();

    DeviceReadWriteHandleImpl(final AbstractDeviceImplHelper deviceImplHelper, final HashAlgorithm hashAlgorithm,
            final boolean readOnly, final int blockSize) {
        this.deviceImplHelper = deviceImplHelper;
        this.hashAlgorithm = hashAlgorithm;
        this.readOnly = readOnly;
        this.blockSize = blockSize;

        // Get / create buffer filled with 0
        ByteBuffer blockZeroTmp;
        synchronized (BLOCK_POOL) {
            final Integer blockSizeInteger = Integer.valueOf(blockSize);
            blockZeroTmp = BLOCK_POOL.get(blockSizeInteger);
            if (blockZeroTmp == null) {
                blockZeroTmp = ALLOCATE_DIRECT ? ByteBuffer.allocateDirect(blockSize) : ByteBuffer.allocate(blockSize);
                BLOCK_POOL.put(blockSizeInteger, blockZeroTmp);
            }
        }
        this.blockZero = blockZeroTmp;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#read(com.oodrive.nuage.vvr.io.ByteDataSink,
     * int, int, long)
     */
    @Override
    public final void read(@Nonnull final ByteBuffer destination, @Nonnegative final int destinationOffset,
            @Nonnegative final int length, @Nonnegative final long devOffset) throws IOException {
        try {
            performIo(IoTaskOpe.READ, destination, destinationOffset, length, devOffset);
        }
        catch (IOException | RuntimeException | Error e) {
            LOGGER.debug("Read error", e);
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#write(com.oodrive.nuage.vvr.io.ByteDataSource,
     * int, int, long)
     */
    @Override
    public final void write(@Nonnull final ByteBuffer source, @Nonnegative final int sourceOffset,
            @Nonnegative final int length, @Nonnegative final long devOffset) throws IOException {
        try {
            if (readOnly) {
                throw new IOException("Read only");
            }

            performIo(IoTaskOpe.WRITE, source, sourceOffset, length, devOffset);
        }
        catch (IOException | RuntimeException | Error e) {
            LOGGER.debug("Write error", e);
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#trim(com.oodrive.nuage.vvr.io.ByteDataSource,
     * int, int, long)
     */
    @Override
    public final void trim(@Nonnegative long lengthLong, @Nonnegative long devOffset) {
        try {
            if (readOnly) {
                throw new IOException("Read only");
            }

            // Check length positive
            if (lengthLong < 0) {
                throw new IOException("Negative length=" + lengthLong);
            }

            // Handle possible overflow
            while (lengthLong > Integer.MAX_VALUE) {
                performIo(IoTaskOpe.TRIM, null, 0, Integer.MAX_VALUE, devOffset);
                lengthLong -= Integer.MAX_VALUE;
                devOffset += Integer.MAX_VALUE;
            }

            // Remaining length
            final int length = (int) (Integer.MAX_VALUE & lengthLong);
            performIo(IoTaskOpe.TRIM, null, 0, length, devOffset);
        }
        catch (IOException | RuntimeException | Error e) {
            LOGGER.warn("Trim error", e);
        }
    }

    /**
     * Prepare and perform read or write requests.
     * 
     * @param ope
     * @param buffer
     * @param bufferOffset
     * @param length
     * @param devOffset
     * @throws IOException
     */
    private final void performIo(final IoTaskOpe ope, final ByteBuffer buffer, @Nonnegative final int bufferOffset,
            @Nonnegative final int length, @Nonnegative final long devOffset) throws IOException {
        checkIoRange(ope, buffer, bufferOffset, length, devOffset);

        final List<IoTask> ioTasks = prepareIo(ope, buffer, bufferOffset, length, devOffset);
        if (ioTasks.isEmpty()) {
            return;
        }

        final IoRequest ioRequest = new IoRequest(ope, devOffset, length, ioTasks, this);
        execIoRequest(buffer, length, ioRequest);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#getSize()
     */
    @Override
    public final long getSize() {
        return deviceImplHelper.getSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#getBlockSize()
     */
    @Override
    public final int getBlockSize() {
        return blockSize;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle#close()
     */
    @Override
    public final void close() {
        // Forbid close during IO
        closedLock.writeLock().lock();
        try {
            this.closed = true;
        }
        finally {
            closedLock.writeLock().unlock();
        }
    }

    /**
     * Look for a block in the device and its parents.
     * <p>
     * Not private to avoid synthetic access from IO tasks.
     * 
     * @param blockIndex
     * @param recursive
     * @return the block key extended lookup in the NRS or <code>null</code>
     * @throws IOException
     */
    final BlockKeyLookupEx lookupBlockKeyEx(final long blockIndex, final boolean recursive) throws IOException {
        return deviceImplHelper.lookupBlockKeyEx(blockIndex, recursive);
    }

    /**
     * Create a transaction for the support of multiple block IOs.
     * 
     * @return the transaction ID, strictly positive.
     */
    abstract int createBlockTransaction() throws IOException;

    /**
     * Commits the transaction on blocks.
     * 
     * @param txId
     *            the id of the transaction
     */
    abstract void commitBlockTransaction(int txId) throws IOException;

    /**
     * Rolls back the transaction on blocks.
     * 
     * @param txId
     *            the id of the transaction
     */
    abstract void rollbackBlockTransaction(int txId) throws IOException;

    /**
     * Tells if the backing store needs the lookup of the previous key to optimize storage.
     * 
     * @return <code>true</code> if the previous key should be looked-up.
     */
    abstract boolean canReplaceOldKey();

    /**
     * True if the op builder must be created to notify remote nodes.
     * 
     * @return <code>true</code> if an opBuilder must be created to notify remote nodes.
     */
    abstract boolean needsBlockOpBuilder();

    /**
     * Notify peers for the update of blocks.
     * 
     * @param blockOpBuilder
     */
    abstract void notifyBlockIO(VvrRemote.RemoteOperation.Builder blockOpBuilder);

    final void storeBlock(final ByteBuffer block, final int offset, final long blockIndex, final byte[] oldKey,
            final int ibsTxId, final VvrRemote.RemoteOperation.Builder opBuilder) throws IbsException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException, IOException {
        // Compute hash on source. Must set position and limit
        block.position(offset);
        block.limit(offset + blockSize);
        final byte[] newKey = ByteBufferDigest.digest(hashAlgorithm, block);

        // Same key: ignore operation
        if (Arrays.equals(oldKey, newKey)) {
            return;
        }

        storeNewBlock(block, offset, blockIndex, newKey, oldKey, ibsTxId, opBuilder);
    }

    /**
     * Store the given block in the backing store.
     * <p>
     * Note: not private to avoid synthetic access from IO tasks.
     * 
     * @param block
     * @param offset
     * @param blockIndex
     * @param oldKey
     *            the old key for replace, may be <code>null</code>
     * @param txId
     *            valid transaction ID if >0.
     * @param opBuilder
     * @throws IllegalArgumentException
     * @throws IndexOutOfBoundsException
     * @throws NullPointerException
     * @throws IOException
     */
    abstract void storeNewBlock(final ByteBuffer block, final int offset, final long blockIndex, final byte[] newKey,
            final byte[] oldKey, final int txId, final VvrRemote.RemoteOperation.Builder opBuilder)
            throws IllegalArgumentException, IndexOutOfBoundsException, NullPointerException, IOException;

    /**
     * Trim the block in the persistence.
     * <p>
     * Note: not private to avoid synthetic access from IO tasks.
     * 
     * @param blockIndex
     * @throws IOException
     */
    final void trimBlock(final long blockIndex) throws IOException {
        // Trim key in persistence
        deviceImplHelper.trimBlockKey(blockIndex);
    }

    /**
     * Revert the previous key for the given block index.
     * 
     * @param blockIndex
     * @param blockKeyLookupEx
     * @throws IOException
     */
    final void restoreKey(final long blockIndex, @Nonnull final BlockKeyLookupEx blockKeyLookupEx) throws IOException {
        // Restore old key or reset entry
        final byte[] oldKey;
        if (blockKeyLookupEx == BlockKeyLookupEx.NOT_FOUND || !blockKeyLookupEx.isSourceCurrent()) {
            // Reset key
            oldKey = null;
            deviceImplHelper.resetBlockKey(blockIndex);
        }
        else {
            // Revert previous key
            oldKey = blockKeyLookupEx.getKey();
            deviceImplHelper.writeBlockKey(blockIndex, oldKey);
        }
    }

    /**
     * Gets a {@link ByteBuffer} filled with the data associated to <code>key</code>.
     * 
     * @param key
     * @param srcNode
     * @param readOnly
     *            <code>true</code> if the buffer will be only read
     * @return a new {@link ByteBuffer} filled with the data associated to <code>key</code>. The block may be released
     *         if it's not read-only.
     * @throws InterruptedException
     */
    abstract ByteBuffer getBlock(final byte[] key, final UUID srcNode, final boolean readOnly) throws IOException,
            InterruptedException;

    /**
     * Fills <code>data</code> with the contents of the block associated to <code>key</code>.
     * 
     * @param key
     * @param data
     * @param dataOffset
     * @param srcNode
     * @return the <code>data</code> length written in data
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void fillBlock(final byte[] key, final ByteBuffer data, final int dataOffset, final UUID srcNode)
            throws IOException, InterruptedException;

    /**
     * Allocate a (potentially) used block.
     * 
     * @param clear
     *            if <code>true</code>, the block is set to 0 before being returned.
     * @return the allocated block.
     */
    final ByteBuffer allocateBlock(final boolean clear) {
        final ByteBuffer block = BYTE_BUFFER_CACHE.allocate(blockSize);
        if (clear) {
            final ByteBuffer source = blockZero.duplicate();
            assert source.position() == 0;
            assert source.limit() == blockSize;
            assert source.capacity() == blockSize;

            block.put(source);

            assert source.position() == blockSize;
            assert block.position() == block.capacity();

            block.clear();
        }
        return block;
    }

    /**
     * Release an allocated block.
     * 
     * @param block
     *            block to release.
     */
    final void releaseBlock(final ByteBuffer block) {
        // Do not release read-only views of ByteString
        if (!block.isReadOnly()) {
            BYTE_BUFFER_CACHE.release(block);
        }
    }

    /**
     * Check IO range in device and in buffer.
     * 
     * @param data
     *            source or destination
     * @param dataOffset
     *            offset in data
     * @param length
     *            IO length
     * @param devOffset
     *            offset in device
     * @throws IOException
     *             on failure
     */
    private final void checkIoRange(final IoTaskOpe ope, final ByteBuffer data, final int dataOffset, final int length,
            final long devOffset) throws IOException {
        // Accept length=0
        if (length < 0) {
            throw new IOException("Negative length=" + length);
        }
        // Positive offsets
        if (dataOffset < 0) {
            throw new IOException("Negative dataOffset=" + dataOffset);
        }
        if (devOffset < 0) {
            throw new IOException("Negative devOffset=" + devOffset);
        }
        // Check data overflow
        if (ope != IoTaskOpe.TRIM) {
            final int eoIo = dataOffset + length;
            if (eoIo > data.capacity()) {
                throw new IOException("Overflow, size=" + getSize() + ", end offset=" + eoIo);
            }
        }
        // Check device overflow
        {
            final long eoIo = devOffset + length;
            if (eoIo > getSize()) {
                throw new IOException("Overflow, size=" + getSize() + ", end offset=" + eoIo);
            }
        }
    }

    /**
     * Create the list of IO tasks to perform.
     * 
     * @param ope
     * @param data
     * @param offset
     * @param length
     * @param devOffset
     * @return list of IO tasks to perform. May be empty
     */
    private final List<IoTask> prepareIo(final IoTaskOpe ope, final ByteBuffer data, final int offset,
            final int length, final long devOffset) {
        // Something to do?
        if (length <= 0) {
            return Collections.emptyList();
        }

        // First block: compute index and segment of data involved
        final ArrayList<IoTask> result = new ArrayList<>((length / blockSize) + 1);
        final boolean singleTask;
        long blockIndex = devOffset / blockSize;
        int ioBlkOffset = (int) (devOffset % blockSize);

        // length to read/write inside the first block
        int ioBlkLength;
        if ((ioBlkOffset + length) <= blockSize) {
            // One IO, in one block
            ioBlkLength = length;
            // Single IO: no need to prepare a revert
            singleTask = true;
        }
        else {
            // Multiple IOs: first IO on end of the first block
            ioBlkLength = blockSize - ioBlkOffset;
            // Multi IO: may have to revert write access
            singleTask = false;
        }
        result.add(newIoTask(data, offset, ope, blockIndex, singleTask, ioBlkOffset, ioBlkLength));

        // Next blocks
        int ioLengthRemaining = length - ioBlkLength;
        int dataOffset = offset + ioBlkLength;
        while (ioLengthRemaining > 0) {
            // New block
            blockIndex++;
            ioBlkOffset = 0;
            ioBlkLength = ioLengthRemaining >= blockSize ? blockSize : ioLengthRemaining;
            result.add(newIoTask(data, dataOffset, ope, blockIndex, singleTask, ioBlkOffset, ioBlkLength));
            ioLengthRemaining -= blockSize;
            dataOffset += blockSize;
        }

        // Check IO task list consistency
        assert (result.size() > 1 && !singleTask) || (result.size() == 1 && singleTask);

        return result;
    }

    /**
     * Create a task to perform the given IO operation.
     * 
     * @param data
     * @param dataOffset
     * @param ope
     * @param blockIndex
     * @param singleTask
     * @param ioBlkOffset
     * @param ioBlkLength
     * @return IO task for the given parameters
     */
    private final IoTask newIoTask(final ByteBuffer data, final int dataOffset, final IoTaskOpe ope,
            final long blockIndex, final boolean singleTask, final int ioBlkOffset, final int ioBlkLength) {
        if (ioBlkOffset == 0 && ioBlkLength == blockSize) {
            return new FullIoTask(data, dataOffset, ope, blockIndex, singleTask);
        }
        return new PartialIoTask(data, dataOffset, ope, blockIndex, singleTask, ioBlkOffset, ioBlkLength);
    }

    /**
     * Run the {@link IoTask} to read from or write to the given {@link ByteBuffer}. The requests are put in a queue and
     * the requests are executed in the FIFO order. Read requests may be executed if there is no conflict with a pending
     * write or trim.
     * 
     * @param buffer
     * @param length
     * @param ioRequest
     * @throws IOException
     */
    private final void execIoRequest(final ByteBuffer buffer, final int length, final IoRequest ioRequest)
            throws IOException {
        // Forbid close during IO
        closedLock.readLock().lock();
        try {
            if (closed) {
                throw new IOException("Closed");
            }

            // Lock device-related lock during IO
            final Lock deviceLock = deviceImplHelper.getIoLock();
            deviceLock.lock();
            try {

                // Add current request in pending queue
                pendingIoRequestsLock.writeLock().lock();
                try {
                    pendingIoRequests.add(ioRequest);
                }
                finally {
                    pendingIoRequestsLock.writeLock().unlock();
                }

                try {
                    // Wait for execution of the request
                    boolean waitExec = true;
                    while (waitExec) {

                        // Can exec?
                        pendingIoRequestsLock.readLock().lock();
                        try {

                            // First request?
                            if (pendingIoRequests.get(0) == ioRequest) {
                                // Ready
                                waitExec = false;
                                break;
                            }

                            final boolean isRead = ioRequest.isRead();
                            for (int i = 0; i < pendingIoRequests.size(); i++) {
                                final IoRequest ioRequestTmp = pendingIoRequests.get(i);
                                if (ioRequestTmp == ioRequest) {
                                    if (isRead) {
                                        // Can start even if not the first
                                        waitExec = false;
                                    }
                                    // No need to continue checking
                                    break;
                                }

                                // Read request may start if no overlap with a pending write or trim
                                if (isRead && !ioRequestTmp.isRead()) {
                                    if (ioRequest.overlap(ioRequestTmp)) {
                                        // Cannot read
                                        break;
                                    }
                                }
                            }
                        }
                        finally {
                            pendingIoRequestsLock.readLock().unlock();
                        }

                        if (waitExec) {
                            synchronized (pendingIoRequestsLock) {
                                pendingIoRequestsLock.wait(DO_NOT_WAIT_FOREVER * 1000);
                            }
                        }
                    }

                    if (buffer == null) {
                        // Trim: just exec the operation
                        ioRequest.exec();
                    }
                    else {
                        // Task execution must not change the original position
                        final int prevPosition = buffer.position();
                        ioRequest.exec();
                        assert prevPosition == buffer.position();
                        // Update position on success
                        buffer.position(prevPosition + length);
                    }
                }
                catch (final IOException e) {
                    throw e;
                }
                catch (final Exception e) {
                    // Convert any exception to IOException (Interrupted, IllegalState, ...)
                    throw new IOException(e);
                }
                finally {
                    // Make sure the request is removed
                    // Add current request in pending queue
                    pendingIoRequestsLock.writeLock().lock();
                    try {
                        pendingIoRequests.remove(ioRequest);
                    }
                    finally {
                        pendingIoRequestsLock.writeLock().unlock();
                    }

                    // Wake-up waiters
                    synchronized (pendingIoRequestsLock) {
                        pendingIoRequestsLock.notifyAll();
                    }
                }
            }
            finally {
                deviceLock.unlock();
            }
        }
        finally {
            closedLock.readLock().unlock();
        }
    }
}
