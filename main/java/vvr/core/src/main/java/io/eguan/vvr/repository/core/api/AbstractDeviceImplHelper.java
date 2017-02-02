package io.eguan.vvr.repository.core.api;

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

import io.eguan.hash.HashAlgorithm;
import io.eguan.ibs.Ibs;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.vvr.repository.core.api.Device.ReadWriteHandle;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

/**
 * Helper class for {@link Device} implementation. Handle IO management and locks.
 *
 * @author oodrive
 * @author llambert
 * @author jmcaba
 */
public abstract class AbstractDeviceImplHelper {

    /**
     * Create a new {@link AbstractDeviceImplHelper}.
     */
    protected AbstractDeviceImplHelper() {
        super();
    }

    /**
     * Create a new {@link ReadWriteHandle} for the device.
     */
    public ReadWriteHandle newReadWriteHandle(final Ibs ibs, final HashAlgorithm hashAlgorithm, final boolean readOnly,
            final int blockSize) {
        return new IbsDeviceReadWriteHandleImpl(this, ibs, hashAlgorithm, readOnly, blockSize);
    }

    /**
     * Gets the device size.
     *
     * @return the size of the device.
     */
    protected abstract long getSize();

    /**
     * Look for the given block key. May lookup for the block key in the parents of the device.
     *
     * @param blockIndex
     * @param recursive
     *            <code>true</code> to search for the given block in the parents of the device.
     * @return the key found or <code>null</code>
     * @throws IOException
     */
    protected abstract BlockKeyLookupEx lookupBlockKeyEx(long blockIndex, boolean recursive) throws IOException;

    /**
     * Stores the given key for the block.
     *
     * @param blockIndex
     * @param key
     * @throws IOException
     */
    protected abstract void writeBlockKey(long blockIndex, byte[] key) throws IOException;

    /**
     * Reset the key for the block. Does nothing if there is no block written at the given position.
     *
     * @param blockIndex
     * @throws IOException
     */
    protected abstract void resetBlockKey(long blockIndex) throws IOException;

    /**
     * Trim the key for the block. Does nothing if there is no block written at the given position.
     *
     * @param blockIndex
     * @throws IOException
     */
    protected abstract void trimBlockKey(long blockIndex) throws IOException;

    /**
     * Lock to acquire to perform a read/write operation on the device.
     *
     * @return lock to take for read/write.
     */
    protected abstract Lock getIoLock();

    /**
     * Notify the IOs made to some peers.
     *
     * @param opBuilder
     *            Ibs updates to notify
     */
    protected abstract void notifyIO(@Nonnull VvrRemote.RemoteOperation.Builder opBuilder);

    /**
     * Look for a block on remote nodes.
     *
     * @param key
     *            key of the block to search
     * @param srcNode
     *            preferred node
     * @return the block found or <code>null</code>
     */
    protected abstract ByteString getRemoteBuffer(@Nonnull byte[] key, @Nonnull UUID srcNode)
            throws InterruptedException;
}
