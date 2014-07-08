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
import java.nio.ByteBuffer;
import java.util.UUID;

import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.nrs.NrsFile;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation.Builder;
import com.oodrive.nuage.vvr.repository.core.api.Device.ReadWriteHandle;

/**
 * Implementation of a {@link ReadWriteHandle} storing blocks in the {@link NrsFile}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class NrsBlkDeviceReadWriteHandleImpl extends DeviceReadWriteHandleImpl {
    /** Associated {@link NrsFile}. */
    //private final NrsFile nrsFile;

    NrsBlkDeviceReadWriteHandleImpl(final AbstractDeviceImplHelper deviceImplHelper, final NrsFile nrsFile,
            final HashAlgorithm hashAlgorithm, final boolean readOnly, final int blockSize) {
        super(deviceImplHelper, hashAlgorithm, readOnly, blockSize);
        //this.nrsFile = nrsFile;
    }

    @Override
    final int createBlockTransaction() throws IOException {
        // TODO real transaction
        return 55;
    }

    @Override
    final void commitBlockTransaction(final int txId) throws IOException {
        assert txId == 55;
    }

    @Override
    final void rollbackBlockTransaction(final int txId) throws IOException {
        assert txId == 55;
    }

    @Override
    final boolean canReplaceOldKey() {
        // No replace here
        return false;
    }

    @Override
    final boolean needsBlockOpBuilder() {
        // Not yet
        return false;
    }

    @Override
    final void notifyBlockIO(final Builder blockOpBuilder) {
        // Should not get here (see previous method)
        throw new AssertionError();
    }

    @Override
    final void storeNewBlock(final ByteBuffer block, final int offset, final long blockIndex, final byte[] newKey,
            final byte[] oldKey, final int txId, final Builder opBuilder) throws IllegalArgumentException,
            IndexOutOfBoundsException, NullPointerException, IOException {

    }

    @Override
    final ByteBuffer getBlock(final byte[] key, final UUID srcNode, final boolean readOnly) throws IOException,
            InterruptedException {
        return null;
    }

    @Override
    final void fillBlock(final byte[] key, final ByteBuffer data, final int dataOffset, final UUID srcNode)
            throws IOException, InterruptedException {
    }

}
