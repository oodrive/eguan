package com.oodrive.nuage.vvr.persistence.repository;

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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation.Builder;
import com.oodrive.nuage.vvr.persistence.repository.NrsDevice.NrsDeviceImplHelper;
import com.oodrive.nuage.vvr.repository.core.api.BlockKeyLookupEx;
import com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle;
import com.oodrive.nuage.vvr.repository.core.api.DeviceReadWriteHandleImpl;

/**
 * Implementation of a {@link ReadWriteHandle} storing blocks in the {@link NrsFile}.
 *
 * @author oodrive
 * @author llambert
 *
 */
final class NrsBlkDeviceReadWriteHandleImpl extends DeviceReadWriteHandleImpl {
    private static final int DUMMY_TXID = 55;

    NrsBlkDeviceReadWriteHandleImpl(final NrsDeviceImplHelper deviceImplHelper, final HashAlgorithm hashAlgorithm,
            final boolean readOnly, final int blockSize) {
        super(deviceImplHelper, hashAlgorithm, readOnly, blockSize);
    }

    @Override
    protected final int createBlockTransaction() throws IOException {
        // TODO real transaction
        return DUMMY_TXID;
    }

    @Override
    protected final void commitBlockTransaction(final int txId) throws IOException {
        assert txId == DUMMY_TXID;
    }

    @Override
    protected final void rollbackBlockTransaction(final int txId) throws IOException {
        assert txId == DUMMY_TXID;
    }

    @Override
    protected final boolean canReplaceOldKey() {
        // No replace here
        return false;
    }

    @Override
    protected final boolean needsBlockOpBuilder() {
        // Not yet
        return false;
    }

    @Override
    protected final void notifyBlockIO(final Builder blockOpBuilder) {
        // Should not get here (see previous method)
        throw new AssertionError();
    }

    @Override
    protected final void storeNewBlock(final ByteBuffer block, final int offset, final long blockIndex,
            final byte[] newKey, final byte[] oldKey, final int txId, final VvrRemote.RemoteOperation.Builder opBuilder)
                    throws IllegalArgumentException, IndexOutOfBoundsException, NullPointerException, IOException {
        assert txId == -1 || txId == DUMMY_TXID;
        assert opBuilder == null;

        // Write block to NrsBlockFile - no concurrent access to the block, as the block has been duplicated
        block.position(offset);
        block.limit(offset + blockSize);
        final NrsFile dstFile = ((NrsDeviceImplHelper) deviceImplHelper).getCurrentNrsFile();
        dstFile.writeBlock(blockIndex, block);

        // Store key in persistence
        ((NrsDeviceImplHelper) deviceImplHelper).writeBlockKey(blockIndex, newKey);
    }

    @Override
    protected final ByteBuffer getBlock(final long blockIndex, final byte[] key,
            final BlockKeyLookupEx blockKeyLookupEx, final boolean readOnly) throws IOException, InterruptedException {
        final ByteBuffer data = allocateBlock(false);
        try {
            fillBlock(blockIndex, key, data, 0, blockKeyLookupEx);
        }
        catch (IOException | InterruptedException | RuntimeException | Error e) {
            releaseBlock(data);
            throw e;
        }
        return data;
    }

    @Override
    protected final void fillBlock(final long blockIndex, final byte[] key, final ByteBuffer data,
            final int dataOffset, final BlockKeyLookupEx blockKeyLookupEx) throws IOException, InterruptedException {
        // Write block to NrsBlockFile - no concurrent access to the block, as the block has been duplicated
        data.position(dataOffset);
        //data.limit(dataOffset + blockSize);
        final NrsFile nrsFileSource = ((NrsBlockKeyLookupEx) blockKeyLookupEx).getNrsFileSource();
        nrsFileSource.readBlock(blockIndex, data);
    }

}
