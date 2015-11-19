package io.eguan.ibs;

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

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Java wrapper to access to an Immutable Block System (IBS). Call the builder {@link IbsFactory#createIbs(File)} or
 * {@link IbsFactory#openIbs(File)} to create a new IBS instance and call {@link #close()} to release the native
 * resources.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
public interface Ibs extends Closeable {

    /**
     * Header to set in {@link Ibs} path name to enable unit tests with {@link IbsFake}. If set, the {@link IbsFake}
     * causes errors during <code>put()</code> and <code>replace()</code>.
     */
    public static final String UNIT_TEST_IBS_HEADER = "_+_UnitTest_-_";

    static final Logger logger = LoggerFactory.getLogger(Ibs.class);

    /**
     * Starts the IBS. The IBS can be accessed only when it is started.
     * 
     * @throws IllegalStateException
     *             if the IBS is already started or is closed
     * @throws IbsException
     *             If the IBS fails to start
     */
    public void start() throws IllegalStateException, IbsException;

    /**
     * Stops the IBS. All the pending transactions are rolled back. Does nothing if the IBS is already stopped or
     * closed.
     * 
     * @throws IbsException
     *             If the IBS fails to stop
     */
    public void stop() throws IbsException;

    /**
     * Tells if the IBS is started.
     * 
     * @return <code>true</code> if the IBS is started.
     */
    public boolean isStarted();

    /**
     * Close the IBS. Once closed, the IBS can not be started anymore. Does nothing if the IBS is already closed.
     */
    @Override
    public void close();

    /**
     * Tells if the IBS is closed.
     * 
     * @return <code>true</code> if the IBS is closed.
     */
    public boolean isClosed();

    /**
     * Destroys the IBS. Deletes everything: the configuration file, the directories. Does it even if the IBS is
     * started. Before the destroy, the IBS is stopped and closed.
     * <p>
     * Note: this call is blocking and may take some time if the IBS stores a lot of datas.
     * 
     * @throws IbsIOException
     */
    public void destroy() throws IbsIOException;

    /**
     * Tells if the hot data management is enabled for the IBS. If <code>false</code>,
     * {@link #replace(long, long, byte[], byte[], ByteBuffer)} (
     * {@link #replace(long, long, byte[], byte[], ByteBuffer, int, int)}) have the same behavior as
     * {@link #put(long, long, byte[], ByteBuffer)} (respectively {@link #put(long, long, byte[], ByteBuffer, int, int)}
     * ).
     * 
     * @return <code>true</code> if the hot data management is enabled.
     * @throws IbsException
     */
    public boolean isHotDataEnabled() throws IbsException;

    /**
     * Gets the data associated with the given key. A new {@link ByteBuffer} of the given length is allocated.
     * 
     * @param key
     *            key of the data
     * @param length
     *            length of the buffer to allocate. The length of the data associated to the key should be smaller or
     *            equal to <code>length</code>
     * @param allocateDirect
     *            <code>true</code> if the allocated buffer should be a direct {@link ByteBuffer}.
     * @return a newly allocated {@link ByteBuffer} containing the data associated to <code>key</code>
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data load failed
     * @throws IbsBufferTooSmallException
     *             if <code>length</code> is smaller than the length of the data to read
     * @throws NullPointerException
     *             if <code>key</code> is <code>null</code>
     */
    public ByteBuffer get(@Nonnull final byte[] key, @Nonnegative final int length, final boolean allocateDirect)
            throws IbsException, IbsIOException, IbsBufferTooSmallException, NullPointerException;

    /**
     * Gets the data associated to the given key. The given buffer is filled, starting at the current position. The data
     * to load must fit in the remaining size of the buffer. The position of the buffer is incremented by data length.
     * 
     * @param key
     *            key of the data
     * @param data
     *            buffer to fill
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data load failed
     * @throws IbsBufferTooSmallException
     *             if the data length exceeds the remaining buffer size
     * @throws NullPointerException
     *             if <code>key</code> or <code>data</code> is <code>null</code>
     */
    public void get(@Nonnull final byte[] key, @Nonnull final ByteBuffer data) throws IbsException, IbsIOException,
            IbsBufferTooSmallException, NullPointerException;

    /**
     * Gets the data associated to the given key. The given buffer is filled, starting from the given offset. The
     * position of the buffer is not changed.
     * 
     * @param key
     *            key of the data
     * @param data
     *            buffer to fill
     * @param offset
     *            offset to write to in <code>data</code>
     * @param length
     *            maximum length of the data associated to the key
     * @return the data length written in <code>data</code>
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data load failed
     * @throws IbsBufferTooSmallException
     *             if <code>length</code> is smaller than the length of the data to read
     * @throws IllegalArgumentException
     *             if <code>offset</code> is negative or <code>length</code> smaller than 1
     * @throws IndexOutOfBoundsException
     *             if <code>offset</code> and <code>length</code> does not fit into <code>data</code>
     * @throws NullPointerException
     *             if <code>key</code> or <code>data</code> is <code>null</code>
     */
    public int get(@Nonnull final byte[] key, @Nonnull final ByteBuffer data, @Nonnegative final int offset,
            @Nonnegative final int length) throws IbsException, IbsIOException, IbsBufferTooSmallException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException;

    /**
     * Removes from the IBS the data associated to the given key.
     * 
     * @param key
     *            key of the data
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws NullPointerException
     *             if <code>key</code> is <code>null</code>
     */
    public void del(@Nonnull final byte[] key) throws IbsException, IbsIOException, NullPointerException;

    /**
     * Associates the key to the given the data in the IBS. The data written is the remaining in the given source
     * buffer, starting at the current position. The position of the buffer is incremented by data length.
     * 
     * @param key
     *            key of the data
     * @param data
     *            buffer to read from the current position. May be <code>null</code> to notify that a new put() of this
     *            known <code>key</code> has been made.
     * @return <code>true</code> if the key/data pair have been added, <code>false</code> if the key/data pair was
     *         already present in the map
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws NullPointerException
     *             if <code>key</code> is <code>null</code>
     */
    public boolean put(@Nonnull final byte[] key, final ByteBuffer data) throws IbsException, IbsIOException,
            NullPointerException;

    /**
     * Associates the key to the given the data in the IBS. The given buffer is written, starting from the given offset
     * and for the given size. The position of the buffer is not changed.
     * 
     * @param key
     *            key of the data
     * @param data
     *            buffer to read
     * @param offset
     *            offset to read from in <code>data</code>
     * @param length
     *            length of the data associated to the key
     * @return <code>true</code> if the key/data pair have been added, <code>false</code> if the key/data pair was
     *         already present in the map
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws IllegalArgumentException
     *             if <code>offset</code> is negative or <code>length</code> smaller than 1
     * @throws IndexOutOfBoundsException
     *             if (<code>offset</code>, <code>length</code>) does not fit into <code>data</code>
     * @throws NullPointerException
     *             if <code>key</code> or <code>data</code> is <code>null</code>
     */
    public boolean put(@Nonnull final byte[] key, @Nonnull final ByteBuffer data, @Nonnegative final int offset,
            @Nonnegative final int length) throws IbsException, IbsIOException, IllegalArgumentException,
            IndexOutOfBoundsException, NullPointerException;

    /**
     * Associates the key to the given the data in the IBS.
     * 
     * @param key
     *            key of the data
     * @param data
     *            buffer to read, the whole contents of the buffer will be read.
     * @return <code>true</code> if the key/data pair have been added, <code>false</code> if the key/data pair was
     *         already present in the map
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws NullPointerException
     *             if <code>key</code> or <code>data</code> is <code>null</code>
     */
    public boolean put(@Nonnull final byte[] key, @Nonnull final ByteString data) throws IbsException, IbsIOException,
            NullPointerException;

    /**
     * Issue a replace request.
     * 
     * A replace request is an enriched put request that warns the underlying storage that a record has been replaced by
     * a new one. The main purpose of this request is to properly handle short-lived records.
     * 
     * The data written is the remaining in the given source buffer, starting at the current position. The position of
     * the buffer is incremented by data length.
     * 
     * @param oldKey
     *            The oldKey is the key associated to the the record that is being (on client side) overwritten by the
     *            new one.
     * @param newKey
     *            The new key associated to the new record.
     * @param data
     *            buffer to read from the current position. May be <code>null</code> to notify that a new replace() of
     *            this known <code>newKey</code> has been made.
     * @return <code>true</code> if the newKey/data pair have been added, <code>false</code> if the newKey/data pair was
     *         already present in the map
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws NullPointerException
     *             if <code>oldKey</code>, <code>newKey</code> or <code>data</code> is <code>null</code>
     */
    public boolean replace(@Nonnull final byte[] oldKey, @Nonnull final byte[] newKey, final ByteBuffer data)
            throws IbsException, IbsIOException;

    /**
     * Issue a replace request.
     * 
     * A replace request is an enriched put request that warns the underlying storage that a record has been replaced by
     * a new one. The main purpose of this request is to properly handle short-lived records.
     * 
     * @param oldKey
     *            The oldKey is the key associated to the the record that is being (on client side) overwritten by the
     *            new one.
     * @param newKey
     *            The new key associated to the new record.
     * @param data
     *            buffer to read
     * @param offset
     *            offset to read from in <code>data</code>
     * @param length
     *            length of the data associated to the key
     * @return <code>true</code> if the newKey/data pair have been added, <code>false</code> if the newKey/data pair was
     *         already present in the map
     * @throws IbsException
     *             if the IBS is not available (not started or closed)
     * @throws IbsIOException
     *             if the data write failed
     * @throws IllegalArgumentException
     *             if <code>offset</code> is negative or <code>length</code> smaller than 1
     * @throws IndexOutOfBoundsException
     *             if (<code>offset</code>, <code>length</code>) does not fit into <code>data</code>
     * @throws NullPointerException
     *             if <code>oldKey</code>, <code>newKey</code> or <code>data</code> is <code>null</code>
     */
    public boolean replace(@Nonnull final byte[] oldKey, @Nonnull final byte[] newKey, @Nonnull final ByteBuffer data,
            @Nonnegative final int offset, @Nonnegative final int length) throws IbsException,
            IllegalArgumentException, IbsIOException, IndexOutOfBoundsException, NullPointerException;

    /**
     * Create a transaction to modify the {@link Ibs}.
     * 
     * @return the transaction ID.
     */
    public int createTransaction() throws IbsException, IllegalArgumentException, IbsIOException;

    /**
     * Associates the key to the given the data in the IBS. The given buffer is written, starting from the given offset
     * and for the given size. The position of the buffer is not changed.
     * <p>
     * Will not be applied before the commit of the transaction.
     * 
     * @param txId
     *            Id of a valid transaction
     * @see #put(byte[], ByteBuffer, int, int)
     */
    public boolean put(@Nonnegative final int txId, @Nonnull final byte[] key, @Nonnull final ByteBuffer data,
            @Nonnegative final int offset, @Nonnegative final int length) throws IbsException, IbsIOException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException;

    /**
     * Issue a replace request. Will not be applied before the commit of the transaction.
     * 
     * @param txId
     *            Id of a valid transaction
     * @see #replace(byte[], byte[], ByteBuffer, int, int)
     */
    public boolean replace(@Nonnegative final int txId, @Nonnull final byte[] oldKey, @Nonnull final byte[] newKey,
            @Nonnull final ByteBuffer data, @Nonnegative final int offset, @Nonnegative final int length)
            throws IbsException, IllegalArgumentException, IbsIOException, IndexOutOfBoundsException,
            NullPointerException;

    /**
     * Commits the given transaction. All the modifications are applied.
     * 
     * @param txId
     *            Id of a valid transaction
     */
    public void commit(@Nonnegative final int txId) throws IbsException, IllegalArgumentException, IbsIOException;

    /**
     * Rolls back the given transaction. All the modifications are discarded.
     * 
     * @param txId
     *            Id of a valid transaction
     */
    public void rollback(@Nonnegative final int txId) throws IbsException, IllegalArgumentException, IbsIOException;
}
