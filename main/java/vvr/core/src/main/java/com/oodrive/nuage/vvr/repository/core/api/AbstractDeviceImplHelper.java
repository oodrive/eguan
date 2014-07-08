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

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.ibs.Ibs;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle;

/**
 * Helper class for {@link Device} implementation. Handle IO management and locks.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 */
public abstract class AbstractDeviceImplHelper {

    /**
     * Result of a block key extended lookup.
     * 
     */
    public static class BlockKeyLookupEx {

        /** Marker instance, can be used when a lookup has failed */
        public static final BlockKeyLookupEx NOT_FOUND = new BlockKeyLookupEx(new byte[0], new UUID(0, 0));

        private final byte[] key;
        private final boolean sourceCurrent;
        private final UUID sourceNode;

        public BlockKeyLookupEx(@Nonnull final byte[] key) {
            super();
            this.key = Objects.requireNonNull(key);
            this.sourceCurrent = true;
            this.sourceNode = null;
        }

        /**
         * @param key
         * @param sourceNode
         *            node on which the {@link NrsFile} source was created/filled
         */
        public BlockKeyLookupEx(@Nonnull final byte[] key, @Nonnull final UUID sourceNode) {
            super();
            this.key = Objects.requireNonNull(key);
            this.sourceCurrent = false;
            this.sourceNode = sourceCurrent ? null : Objects.requireNonNull(sourceNode);
        }

        /**
         * The key found, no <code>null</code>.
         * 
         * @return the key found
         */
        public final byte[] getKey() {
            return key;
        }

        /**
         * Tells if the key have been found in the current storage source.
         * 
         * @return <code>true</code> if the key was found in the current block key storage source.
         */
        public final boolean isSourceCurrent() {
            return sourceCurrent;
        }

        public final UUID getSourceNode() {
            return sourceNode;
        }

    }

    /**
     * Create a new {@link AbstractDeviceImplHelper}.
     */
    protected AbstractDeviceImplHelper() {
        super();
    }

    /**
     * Create a new {@link ReadWriteHandle} for the device.
     */
    public final ReadWriteHandle newReadWriteHandle(final Ibs ibs, final HashAlgorithm hashAlgorithm,
            final boolean readOnly, final int blockSize) {
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
