package io.eguan.iscsisrv;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jscsi.target.storage.IStorageModule;

/**
 * Implementation of a {@link IStorageModule}, based on a {@link IscsiDevice}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
final class IStorageModuleImpl implements IStorageModule {
    /** Associated device. */
    private final IscsiDevice device;

    /**
     * Create a new storage module.
     * 
     * @param device
     */
    IStorageModuleImpl(@Nonnull final IscsiDevice device) {
        this.device = Objects.requireNonNull(device);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#checkBounds(long, int)
     */
    @Override
    public final int checkBounds(final long logicalBlockAddress, final int transferLengthInBlocks) {
        final long sizeInBlocks = getSizeInBlocks();
        if (logicalBlockAddress < 0 || logicalBlockAddress >= sizeInBlocks) {
            return 1;
        }
        if (transferLengthInBlocks < 0 || logicalBlockAddress + transferLengthInBlocks > sizeInBlocks) {
            return 2;
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#getSizeInBlocks()
     */
    @Override
    public final long getSizeInBlocks() {
        final long size = device.getSize();
        return size / getBlockSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#getSizeInBlocks()
     */
    @Override
    public final int getBlockSize() {
        return device.getBlockSize();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#isWriteProtected()
     */
    @Override
    public final boolean isWriteProtected() {
        return device.isReadOnly();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#read(byte[], long)
     */
    @Override
    public final void read(final ByteBuffer bytes, final long storageIndex) throws IOException {
        final int length = bytes.capacity();
        device.read(bytes, length, storageIndex);
        assert bytes.position() == bytes.capacity();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#write(byte[], long)
     */
    @Override
    public final void write(final ByteBuffer bytes, final long storageIndex) throws IOException {
        final int length = bytes.capacity();
        device.write(bytes, length, storageIndex);
        assert bytes.position() == bytes.capacity();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.jscsi.target.storage.IStorageModule#close()
     */
    @Override
    public final void close() throws IOException {
        device.close();
    }
}
