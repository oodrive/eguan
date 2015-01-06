package com.oodrive.nuage.ibs;

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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.protobuf.ByteString;

/**
 * Fake {@link Ibs}. In memory implementation, so limited to test. To be able to create/close/open/close a
 * {@link IbsFake}, all the fake Ibs shares the same map of key/value.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
final class IbsFake extends IbsAbstract {

    private static final class FakeItem {
        private final AtomicInteger count;
        private final ByteBuffer buffer;

        FakeItem(final ByteBuffer buffer) {
            super();
            this.buffer = Objects.requireNonNull(buffer);
            this.count = new AtomicInteger(1);
        }

        protected final int incrCount() {
            return count.incrementAndGet();
        }

        protected final int decrCount() {
            return count.decrementAndGet();
        }

        protected final ByteBuffer getBuffer() {
            return buffer;
        }

    }

    @GuardedBy(value = "fakeIbsList")
    private static final Map<String, Map<byte[], FakeItem>> fakeIbsList = new HashMap<>();
    @GuardedBy(value = "fakeIbsStore")
    private final Map<byte[], FakeItem> fakeIbsStore;

    private final IbsMemTransaction ibsMemTransaction;

    // Test error management
    private final AtomicInteger ioCountPut;
    private final int ioCountLimitPut;
    private final AtomicInteger ioCountGet;
    private final int ioCountLimitGet;

    private static Comparator<byte[]> fakeIbsComparator = new Comparator<byte[]>() {
        @Override
        public final int compare(final byte[] o1, final byte[] o2) {
            final int l1 = o1.length;
            final int l2 = o2.length;
            if (l1 != l2) {
                return l1 - l2;
            }
            for (int i = 0; i < l1; i++) {
                final byte b1 = o1[i];
                final byte b2 = o2[i];
                if (b1 != b2) {
                    return b1 - b2;
                }
            }
            return 0;
        }
    };

    private IbsFake(final String ibsPath, @Nonnull final Map<byte[], FakeItem> fakeIbsStore) {
        super(ibsPath);
        this.fakeIbsStore = Objects.requireNonNull(fakeIbsStore);
        if (ibsPath.startsWith(Ibs.UNIT_TEST_IBS_HEADER)) {
            final int lengthHeader = Ibs.UNIT_TEST_IBS_HEADER.length();
            final int indexSep = ibsPath.indexOf(':', lengthHeader);
            ioCountPut = new AtomicInteger();
            if (indexSep > 0) {
                ioCountLimitPut = Integer.valueOf(ibsPath.substring(lengthHeader, indexSep));
                ioCountGet = new AtomicInteger();
                ioCountLimitGet = Integer.valueOf(ibsPath.substring(indexSep + 1));
            }
            else {
                ioCountLimitPut = Integer.valueOf(ibsPath.substring(lengthHeader));
                ioCountGet = null;
                ioCountLimitGet = 0;
            }
        }
        else {
            ioCountPut = null;
            ioCountLimitPut = 0;
            ioCountGet = null;
            ioCountLimitGet = 0;
        }

        ibsMemTransaction = new IbsMemTransaction(this);
    }

    static final Ibs createIbs(final String ibsPath) throws IbsException {
        synchronized (fakeIbsList) {
            if (fakeIbsList.containsKey(ibsPath)) {
                throw new IbsException(ibsPath, IbsErrorCode.CREATE_IN_NON_EMPTY_DIR);
            }

            // Create new map
            final Map<byte[], FakeItem> fakeIbsStore = new TreeMap<>(fakeIbsComparator);
            fakeIbsList.put(ibsPath, fakeIbsStore);
            return new IbsFake(ibsPath, fakeIbsStore);
        }
    }

    // TODO lock fakeIbsStore when opened to avoid concurrent open
    static final Ibs openIbs(final String ibsPath) throws IbsException {
        synchronized (fakeIbsList) {
            // Find map
            final Map<byte[], FakeItem> fakeIbsStore = fakeIbsList.get(ibsPath);
            if (fakeIbsStore == null) {
                throw new IbsException(ibsPath, IbsErrorCode.INIT_FROM_EMPTY_DIR);
            }
            return new IbsFake(ibsPath, fakeIbsStore);
        }
    }

    @Override
    protected final int doStart() {
        return 0;
    }

    @Override
    protected final int doStop() {
        ibsMemTransaction.clear();
        return 0;
    }

    @Override
    protected final int doClose() {
        return 0;
    }

    @Override
    protected final int doDestroy() {
        synchronized (fakeIbsList) {
            fakeIbsList.remove(ibsPath);
        }
        return 0;
    }

    @Override
    public final boolean isHotDataEnabled() throws IbsException {
        return true;
    }

    @Override
    public final int get(final byte[] key, final ByteBuffer data, final int offset, final int length)
            throws IbsException, IbsIOException, IbsBufferTooSmallException, IllegalArgumentException,
            IndexOutOfBoundsException, NullPointerException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        final int prevPosition = data.position();
        final int readLen = getFake(key, data, offset, length, true);
        // Reset previous position
        data.position(prevPosition);
        return readLen;
    }

    @Override
    public final void del(final byte[] key) throws IbsException, IbsIOException, NullPointerException {
        if (!started || closed) {
            throw new IbsException(toString());
        }
        // Check reference
        Objects.requireNonNull(key);

        synchronized (fakeIbsStore) {
            fakeIbsStore.remove(key);
        }
    }

    @Override
    public final boolean put(final byte[] key, final ByteBuffer data) throws IbsException, IbsIOException,
            NullPointerException {
        if (data == null) {
            return replaceFake(null, key, null, 0, 0, true);
        }
        final int writtenLength = data.remaining();
        final boolean result = put(key, data, data.position(), writtenLength);
        data.position(data.position() + writtenLength);
        return result;
    }

    @Override
    public final boolean put(final byte[] key, final ByteBuffer data, final int offset, final int length)
            throws IbsException, IbsIOException, IllegalArgumentException, IndexOutOfBoundsException,
            NullPointerException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        return replaceFake(null, key, data, offset, length, true);
    }

    @Override
    public final boolean put(final byte[] key, final ByteString data) throws IbsException, IbsIOException,
            NullPointerException {
        final ByteBuffer dataBB = data.asReadOnlyByteBuffer();
        return put(key, dataBB);
    }

    @Override
    public final boolean replace(final byte[] oldKey, final byte[] newKey, final ByteBuffer data) throws IbsException,
            IbsIOException {
        if (data == null) {
            return replaceFake(null, newKey, null, 0, 0, true);
        }
        final int writtenLength = data.remaining();
        final boolean result = replace(oldKey, newKey, data, data.position(), writtenLength);
        data.position(data.position() + writtenLength);
        return result;
    }

    @Override
    public final boolean replace(final byte[] oldKey, final byte[] newKey, final ByteBuffer data, final int offset,
            final int length) throws IbsException, IllegalArgumentException, IbsIOException, IndexOutOfBoundsException,
            NullPointerException {
        // TODO shared access to the IBS state during the whole put?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        Objects.requireNonNull(oldKey);
        checkArgs(newKey, data, offset, length);

        return replaceFake(oldKey, newKey, data, offset, length, true);
    }

    @Override
    public final int createTransaction() {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        return ibsMemTransaction.createTransaction();
    }

    @Override
    public final boolean put(final int txId, final byte[] key, final ByteBuffer data, final int offset, final int length)
            throws IbsException, IbsIOException, IllegalArgumentException, IndexOutOfBoundsException,
            NullPointerException {

        if (txId <= 0) {
            if (txId == Integer.MIN_VALUE) {
                replaceFake(null, key, data, offset, length, false);
                return false;
            }
            else {
                throw new IllegalArgumentException("txId=" + txId);
            }
        }

        checkArgs(key, data, offset, length);

        final boolean newKeyToAdd;
        synchronized (fakeIbsStore) {
            newKeyToAdd = fakeIbsStore.get(key) == null;
        }

        // Count operations
        if (ioCountPut != null) {
            final int currentCount = ioCountPut.incrementAndGet();
            if ((currentCount % ioCountLimitPut) == 0) {
                throw new IbsIOException(IbsErrorCode.UNKNOW_ERROR);
            }
        }

        // Add new operation
        ibsMemTransaction.put(txId, key, data, offset, length);

        return newKeyToAdd;
    }

    @Override
    public final boolean replace(final int txId, final byte[] oldKey, final byte[] newKey, final ByteBuffer data,
            final int offset, final int length) throws IbsException, IllegalArgumentException, IbsIOException,
            IndexOutOfBoundsException, NullPointerException {

        if (txId <= 0) {
            if (txId == Integer.MIN_VALUE) {
                replaceFake(oldKey, newKey, data, offset, length, false);
                return false;
            }
            else {
                throw new IllegalArgumentException("txId=" + txId);
            }
        }
        Objects.requireNonNull(oldKey);
        checkArgs(newKey, data, offset, length);

        final boolean newKeyToAdd;
        synchronized (fakeIbsStore) {
            newKeyToAdd = fakeIbsStore.get(newKey) == null;
        }

        // Count operations
        if (ioCountPut != null) {
            final int currentCount = ioCountPut.incrementAndGet();
            if ((currentCount % ioCountLimitPut) == 0) {
                throw new IbsIOException(IbsErrorCode.UNKNOW_ERROR);
            }
        }

        // Add new operation
        ibsMemTransaction.replace(txId, oldKey, newKey, data, offset, length);
        return newKeyToAdd;
    }

    @Override
    public final void commit(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        ibsMemTransaction.commit(txId);
    }

    @Override
    public final void rollback(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        ibsMemTransaction.rollback(txId);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return "IBS[" + ibsPath + ", started=" + started + ", closed=" + closed + "]";
    }

    /**
     * Puts a value in the fake IBS.
     * 
     * @param key
     * @param data
     * @param offset
     * @param length
     * @throws IbsIOException
     */
    private final boolean replaceFake(final byte[] oldKey, final byte[] key, final ByteBuffer data, final int offset,
            final int length, final boolean count) throws IbsIOException {
        // Reached limit to send error
        if (count && ioCountPut != null) {
            final int currentCount = ioCountPut.incrementAndGet();
            if ((currentCount % ioCountLimitPut) == 0) {
                throw new IbsIOException(IbsErrorCode.UNKNOW_ERROR);
            }
        }

        final ByteBuffer val;
        if (data == null) {
            val = null;
        }
        else {
            if (data.isDirect())
                val = ByteBuffer.allocateDirect(length);
            else
                val = ByteBuffer.allocate(length);
            final ByteBuffer src = data.duplicate();
            src.position(offset).limit(offset + length);
            val.put(src);
        }
        synchronized (fakeIbsStore) {
            // REPLACE: remove oldKey if not referenced
            if (oldKey != null) {
                final FakeItem fakeItem = fakeIbsStore.get(oldKey);
                if (fakeItem != null) {
                    if (fakeItem.decrCount() <= 0) {
                        fakeIbsStore.remove(oldKey);
                        // logger.warn("REM " + oldKey[0] + oldKey[1] + oldKey[2] + oldKey[3] + oldKey[4] + oldKey[5]);
                    }
                }
            }

            // PUT
            final FakeItem fakeItem = fakeIbsStore.get(key);
            if (fakeItem != null) {
                // Keep current value
                fakeItem.incrCount();
                // logger.warn("PUT " + key[0] + key[1] + key[2] + key[3] + key[4] + key[5] + " incr");
                return false;
            }
            else if (val == null) {
                // Key not found
                throw new IbsIOException(IbsErrorCode.NOT_FOUND);
            }
            if (fakeIbsStore.put(key, new FakeItem(val)) != null) {
                throw new AssertionError();
            }
            // logger.warn("PUT " + key[0] + key[1] + key[2] + key[3] + key[4] + key[5] + " new");
            return true;
        }
    }

    /**
     * Gets a value from the fake IBS.
     * 
     * @param key
     * @param data
     * @param offset
     * @param length
     * @throws IbsIOException
     */
    private final int getFake(final byte[] key, final ByteBuffer data, final int offset, final int length,
            final boolean count) throws IbsIOException {
        // Reached limit to send error
        if (count && ioCountGet != null) {
            final int currentCount = ioCountGet.incrementAndGet();
            if ((currentCount % ioCountLimitGet) == 0) {
                throw new IbsIOException(IbsErrorCode.UNKNOW_ERROR);
            }
        }

        // logger.warn("GET " + key[0] + key[1] + key[2] + key[3] + key[4] + key[5]);

        final FakeItem fakeItem;
        synchronized (fakeIbsStore) {
            fakeItem = fakeIbsStore.get(key);
        }
        if (fakeItem == null) {
            throw new IbsIOException(toString(), IbsErrorCode.NOT_FOUND);
        }
        final ByteBuffer val = (ByteBuffer) fakeItem.getBuffer().rewind();
        if (val.capacity() > (data.capacity() - offset)) {
            throw new IbsBufferTooSmallException(val.capacity());
        }
        data.position(offset);
        data.put(val);
        return data.position() - offset;
    }
}
