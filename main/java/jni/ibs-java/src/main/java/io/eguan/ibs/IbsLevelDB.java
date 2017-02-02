package io.eguan.ibs;

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

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

/**
 * Implementation of {@link Ibs} based on LevelDB.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
final class IbsLevelDB extends IbsDBAbstract {

    // Load native code
    static {
        NarSystem.loadLibrary();
    }

    /** Id of the IBS. For native access to the object */
    private final int ibsId;

    /**
     * Creates a new instance. The IBS have been opened.
     * 
     * @param ibsPath
     *            path of the IBS store
     * @param ibsId
     *            IBS native index.
     */
    IbsLevelDB(final String ibsPath, final int ibsId) {
        super(ibsPath);
        this.ibsId = ibsId;
    }

    @Override
    protected final int doStart() {
        return ibsStart(ibsId);
    }

    @Override
    protected int doStop() {
        return ibsStop(ibsId);
    }

    @Override
    protected int doClose() {
        // Release native instance
        return ibsDelete(ibsId);
    }

    @Override
    protected final int doDestroy() {
        // Remove storage
        int retval = ibsDestroy(ibsId);
        if (retval == 0) {
            retval = doClose();
        }
        return retval;
    }

    @Override
    public final boolean isHotDataEnabled() throws IbsException {
        final int retval = ibsIsHotDataEnabled(ibsId);
        if (retval == 1) {
            return true;
        }
        else if (retval == 0) {
            return false;
        }
        else if (retval < 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            throw new IbsException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }
        else {
            throw new AssertionError();
        }
    }

    @Override
    public final int get(@Nonnull final byte[] key, @Nonnull final ByteBuffer data, @Nonnegative final int offset,
            @Nonnegative final int length) throws IbsException, IbsIOException, IbsBufferTooSmallException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException {
        // TODO shared access to the IBS state during the whole get?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        final int retval;
        // Call native get. Direct access for direct buffers only
        if (data.isDirect()) {
            retval = ibsGetDirect(ibsId, key, data, offset, length);
        }
        else {
            retval = ibsGet(ibsId, key, data.array(), offset, length);
        }

        if (retval < 0) {
            // Extract IBS error code
            final int code = retval >> 24;
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(code);
            if (ibsErrorCode == IbsErrorCode.BUFFER_TOO_SMALL) {
                // Get record size
                throw new IbsBufferTooSmallException(toString(), retval & 0x00FFFFFF);
            }
            else {
                throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
            }
        }
        return retval;
    }

    @Override
    public final void del(@Nonnull final byte[] key) throws IbsException, IbsIOException, NullPointerException {
        // TODO shared access to the IBS state during the whole put?
        if (!started || closed) {
            throw new IbsException(toString());
        }
        // Check reference
        Objects.requireNonNull(key);

        final int retval = ibsDel(ibsId, key);
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }
    }

    @Override
    public final boolean put(@Nonnull final byte[] key, final ByteBuffer data) throws IbsException, IbsIOException,
            NullPointerException {
        // New put() of known data?
        if (data == null) {
            final int retval = ibsPutDirect(ibsId, 0, key, data, 0, 1);
            if (retval != 0) {
                final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
                if (ibsErrorCode == IbsErrorCode.KEY_ALREADY_ADDED) {
                    // No new addition in the database
                    return false;
                }
                throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
            }
            // When data is not set, ibs is supposed to return KEY_ALREADY_ADDED or NOT_FOUND
            throw new AssertionError();
        }

        // Real put()
        final int writtenLength = data.remaining();
        final boolean result = put(key, data, data.position(), writtenLength);
        data.position(data.position() + writtenLength);
        return result;
    }

    @Override
    public final boolean put(@Nonnull final byte[] key, @Nonnull final ByteString data) throws IbsException,
            IbsIOException, NullPointerException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        // Check references
        Objects.requireNonNull(key);
        Objects.requireNonNull(data);

        // Call native put: read internal field from native code
        final int retval = ibsPutByteStr(ibsId, key, data);
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            if (ibsErrorCode == IbsErrorCode.KEY_ALREADY_ADDED) {
                // No new addition in the database
                return false;
            }
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }

        // New key/pair added
        return true;
    }

    @Override
    public final boolean replace(@Nonnull final byte[] oldKey, @Nonnull final byte[] newKey, final ByteBuffer data)
            throws IbsException, IbsIOException {
        // New replace() of known data?
        if (data == null) {
            final int retval = ibsReplaceDirect(ibsId, 0, oldKey, newKey, data, 0, 1);
            if (retval != 0) {
                final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
                if (ibsErrorCode == IbsErrorCode.KEY_ALREADY_ADDED) {
                    // No new addition in the database
                    return false;
                }
                throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
            }
            // When data is not set, ibs is supposed
            // to return KEY_ALREADY_ADDED or NOT_FOUND
            throw new AssertionError();
        }

        // Real replace()
        final int writtenLength = data.remaining();
        final boolean result = replace(oldKey, newKey, data, data.position(), writtenLength);
        data.position(data.position() + writtenLength);
        return result;
    }

    @Override
    public final int createTransaction() throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        final int retval = ibsCreateTransaction(ibsId);
        if (retval < 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }
        return retval;
    }

    @Override
    public final void commit(@Nonnegative final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }

        final int retval = ibsCommitTransaction(ibsId, txId);
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }
    }

    @Override
    public final void rollback(@Nonnegative final int txId) throws IbsException, IllegalArgumentException,
            IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }

        final int retval = ibsRollbackTransaction(ibsId, txId);
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return "IBS[" + ibsPath + ", id=" + ibsId + ", started=" + started + ", closed=" + closed + "]";
    }

    @Override
    protected final boolean doPut(final int txId, @Nonnull final byte[] key, @Nonnull final ByteBuffer data,
            @Nonnegative final int offset, @Nonnegative final int length) throws IbsException, IbsIOException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException {
        // TODO shared access to the IBS state during the whole put?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        final int retval;
        // Call native put. Direct access for direct buffers only
        if (data.isDirect()) {
            retval = ibsPutDirect(ibsId, txId, key, data, offset, length);
        }
        else {
            retval = ibsPut(ibsId, txId, key, data.array(), offset, length);
        }
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            if (ibsErrorCode == IbsErrorCode.KEY_ALREADY_ADDED) {
                // No new addition in the database
                return false;
            }
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }

        // New key/pair added
        return true;
    }

    @Override
    protected final boolean doReplace(final int txId, @Nonnull final byte[] oldKey, @Nonnull final byte[] newKey,
            @Nonnull final ByteBuffer data, @Nonnegative final int offset, @Nonnegative final int length)
            throws IbsException, IllegalArgumentException, IbsIOException, IndexOutOfBoundsException,
            NullPointerException {
        // TODO shared access to the IBS state during the whole replace?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        // Check references
        Objects.requireNonNull(oldKey);
        checkArgs(newKey, data, offset, length);

        final int retval;
        // Fake impl
        // Call native replace. Direct access for direct buffers only
        if (data.isDirect()) {
            retval = ibsReplaceDirect(ibsId, txId, oldKey, newKey, data, offset, length);
        }
        else {
            retval = ibsReplace(ibsId, txId, oldKey, newKey, data.array(), offset, length);
        }
        if (retval != 0) {
            final IbsErrorCode ibsErrorCode = IbsErrorCode.valueOf(retval);
            if (ibsErrorCode == IbsErrorCode.KEY_ALREADY_ADDED) {
                // No new addition in the database
                return false;
            }
            throw new IbsIOException(toString() + ": " + ibsErrorCode, ibsErrorCode);
        }

        // New key/pair added
        return true;
    }

    //
    // Native Calls
    //

    /**
     * Create a new IBS.
     * 
     * @param path
     *            The configuration filename.
     * @return The IBS instance id (>0) if successful else an ibsErrorCode
     */
    static native int ibsCreate(final String path);

    /**
     * Init an existing IBS instance.
     * 
     * @param path
     *            The configuration filename.
     * @return The IBS instance id (>0) if successful else an ibsErrorCode
     */
    static native int ibsInit(final String path);

    /**
     * Start an IBS instance.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return 0 if success else an ibsErrorCode
     */
    private static native int ibsStart(final int id);

    /**
     * Stop an IBS instance.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return 0 if success else an ibsErrorCode
     */
    private static native int ibsStop(final int id);

    /**
     * Delete an IBS instance (free memory, leave on-disk data).
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return 0 if success else an ibsErrorCode
     */
    private static native int ibsDelete(int id);

    /**
     * Destroy an IBS instance. Wipe all on-disk data, does not release native instance memory.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return 0 if success else an ibsErrorCode
     */
    private static native int ibsDestroy(final int id);

    /**
     * Getter for the hot data status.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return <code>1</code> if hot data are activated, <code>0</code> for not enabled or an ibsErrorCode
     */
    private static native int ibsIsHotDataEnabled(final int id);

    /**
     * Fetch a record an IBS instance. Loads data from a direct {@link ByteBuffer}.
     * 
     * @see #ibsGet(int, byte[], byte[], int, int)
     */
    private static native int ibsGetDirect(final int id, final byte[] key, final ByteBuffer data, final int offset,
            final int lengthMax);

    /**
     * Fetch a record an IBS instance. Blocking and thread safe.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param key
     *            The key associated to the record
     * @param data
     *            Destination array. The record will be stored in <code>data</code>
     * @param offset
     *            offset in data were the data will be written
     * @param lengthMax
     *            data maximum length
     * @return If positive, the length of data written inside <code>data</code>; if negative, the composition of the IBS
     *         error code (8 higher bits) and the length of the record (24 lower bits)
     */
    private static native int ibsGet(final int id, final byte[] key, final byte[] data, final int offset,
            final int lengthMax);

    /**
     * Delete a record an IBS instance. Blocking & thread safe.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param key
     *            The key associated to the record
     * @return 0 if success, an ibsErrorCode if something went wrong.
     */
    private static native int ibsDel(final int id, final byte[] key);

    /**
     * Save a record associated to a key and store in a direct {@link ByteBuffer}.
     * 
     * @see #ibsPut(int, int, byte[], byte[], int, int)
     */
    private static native int ibsPutDirect(int id, int txId, byte[] key, ByteBuffer data, final int offset,
            final int length);

    /**
     * Save a record associated to a key. Blocking and thread safe.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param txId
     *            id of a transaction or <code>0</code>
     * @param key
     *            The key associated to the record
     * @param data
     *            The record that will be stored
     * @param offset
     *            offset in data
     * @param length
     *            data length
     * @return 0 if success else an ibsErrorCode.
     */
    private static native int ibsPut(int id, int txId, byte[] key, byte[] data, final int offset, final int length);

    /**
     * Save a record associated to a key. Blocking and thread safe.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param key
     *            The key associated to the record
     * @param data
     *            The record that will be stored
     * @return 0 if success else an ibsErrorCode.
     */
    private static native int ibsPutByteStr(int id, byte[] key, ByteString data);

    /**
     * Issue a replace request on a direct {@link ByteBuffer}.
     * 
     * @see #ibsReplace(int, int, byte[], byte[], byte[], int, int)
     */
    private static native int ibsReplaceDirect(int id, int txId, byte[] oldKey, byte[] newKey,
            final ByteBuffer newData, final int offset, final int length);

    /**
     * Issue a replace request.
     * 
     * A replace request is an enriched put request that warns the underlying storage that a record has been replaced by
     * a new one. The main purpose of this request is to properly handle short-lived records.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param txId
     *            id of a transaction or <code>0</code>
     * @param oldKey
     *            The oldKey is the key associated to the the record that is being (on client side) overwritten by the
     *            new one. HighId and lowId must be use to ensure safe replacement. Raw buffer.
     * @param newKey
     *            The new key associated to the new record. Raw buffer.
     * @param data
     *            The record that will be store. Raw buffer.
     * @return 0 if success else an ibsErrorCode.
     */
    private static native int ibsReplace(int id, int txId, byte[] oldKey, byte[] newKey, byte[] newData,
            final int offset, final int length);

    /**
     * Create a new {@link Ibs} transaction.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @return id of the transaction or an negative error code
     */
    private static native int ibsCreateTransaction(int id);

    /**
     * Commits a transaction.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param txId
     *            The transaction id returned by {@link #ibsCreateTransaction(int)}.
     * @return 0 or an negative error code
     */
    private static native int ibsCommitTransaction(int id, int txId);

    /**
     * Rolls back a transaction.
     * 
     * @param id
     *            The IBS id returned by {@link #ibsInit(String)}.
     * @param txId
     *            The transaction id returned by {@link #ibsCreateTransaction(int)}.
     * @return 0 or an negative error code
     */
    private static native int ibsRollbackTransaction(int id, int txId);
}
