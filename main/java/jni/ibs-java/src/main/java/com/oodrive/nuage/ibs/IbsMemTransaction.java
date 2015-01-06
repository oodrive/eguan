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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

/**
 * Handles {@link Ibs} transactions. Stores the pending blocks in memory. The associated {@link Ibs} must consider the
 * transaction id <code>Integer.MIN_VALUE</code> as the commit of a key/value pair.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class IbsMemTransaction {

    private static enum FakeTxOperationCode {
        PUT, REPLACE;
    }

    private static final class FakeTxOperation {
        final FakeTxOperationCode opCode;
        final byte[] oldKey;
        final byte[] newKey;
        final ByteBuffer data;
        final int offset;
        final int length;

        FakeTxOperation(final FakeTxOperationCode opCode, final byte[] oldKey, final byte[] newKey,
                final ByteBuffer data, final int offset, final int length) {
            super();
            this.opCode = opCode;
            this.oldKey = oldKey;
            this.newKey = newKey;

            // Defensive copy of the buffer
            final ByteBuffer dataTmp = data.duplicate();
            if (dataTmp.isDirect())
                this.data = ByteBuffer.allocateDirect(dataTmp.capacity());
            else
                this.data = ByteBuffer.allocate(dataTmp.capacity());
            dataTmp.position(0).limit(dataTmp.capacity());
            this.data.put(dataTmp);
            this.data.clear();

            this.offset = offset;
            this.length = length;
        }

    }

    // TODO: handle overflow
    private final AtomicInteger nextTxId = new AtomicInteger(1);
    @GuardedBy(value = "transactions")
    private final IntObjectOpenHashMap<List<FakeTxOperation>> transactions = new IntObjectOpenHashMap<>();

    private final Ibs ibs;

    IbsMemTransaction(final Ibs ibs) {
        super();
        this.ibs = ibs;
    }

    final int createTransaction() {

        final int txId = nextTxId.getAndIncrement();
        assert txId > 0;
        // New operation list
        synchronized (transactions) {
            transactions.put(txId, new ArrayList<FakeTxOperation>());
        }
        return txId;
    }

    final void put(final int txId, final byte[] key, final ByteBuffer data, final int offset, final int length)
            throws IbsException, IbsIOException, IllegalArgumentException, IndexOutOfBoundsException,
            NullPointerException {
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }

        final List<FakeTxOperation> operations;
        synchronized (transactions) {
            operations = transactions.get(txId);
        }
        if (operations == null) {
            throw new IbsIOException(IbsErrorCode.INVALID_TRANSACTION_ID);
        }

        // Add new operation
        synchronized (operations) {
            operations.add(new FakeTxOperation(FakeTxOperationCode.PUT, null, key, data, offset, length));
        }
    }

    final void replace(final int txId, final byte[] oldKey, final byte[] newKey, final ByteBuffer data,
            final int offset, final int length) throws IbsException, IllegalArgumentException, IbsIOException,
            IndexOutOfBoundsException, NullPointerException {
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }

        final List<FakeTxOperation> operations;
        synchronized (transactions) {
            operations = transactions.get(txId);
        }
        if (operations == null) {
            throw new IbsIOException(IbsErrorCode.INVALID_TRANSACTION_ID);
        }

        // Add new operation
        synchronized (operations) {
            operations.add(new FakeTxOperation(FakeTxOperationCode.REPLACE, oldKey, newKey, data, offset, length));
        }
    }

    final void commit(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        final List<FakeTxOperation> operations;
        synchronized (transactions) {
            operations = transactions.remove(txId);
        }
        if (operations == null) {
            throw new IbsIOException(IbsErrorCode.INVALID_TRANSACTION_ID);
        }
        // Apply changes
        for (final FakeTxOperation fakeTxOperation : operations) {
            if (fakeTxOperation.opCode == FakeTxOperationCode.PUT) {
                ibs.put(Integer.MIN_VALUE, fakeTxOperation.newKey, fakeTxOperation.data, fakeTxOperation.offset,
                        fakeTxOperation.length);
            }
            else if (fakeTxOperation.opCode == FakeTxOperationCode.REPLACE) {
                ibs.replace(Integer.MIN_VALUE, fakeTxOperation.oldKey, fakeTxOperation.newKey, fakeTxOperation.data,
                        fakeTxOperation.offset, fakeTxOperation.length);
            }
            else {
                throw new AssertionError("opcode=" + fakeTxOperation.opCode);
            }
        }
    }

    final void rollback(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        final List<FakeTxOperation> operations;
        synchronized (transactions) {
            operations = transactions.remove(txId);
        }
        if (operations == null) {
            throw new IbsIOException(IbsErrorCode.INVALID_TRANSACTION_ID);
        }
    }

    /**
     * Roll back all the pending transactions.
     */
    final void clear() {
        synchronized (transactions) {
            transactions.clear();
        }
    }
}
