package com.oodrive.nuage.iscsisrv;

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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Device associated to a {@link IscsiTarget}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public interface IscsiDevice extends Closeable {

    /**
     * Tells is the device is read-only.
     * 
     * @return <code>true</code> if the device is read-only.
     */
    boolean isReadOnly();

    /**
     * Gets the size of the device.
     * 
     * @return size of the device in bytes.
     */
    long getSize();

    /**
     * Gets the block size of the device.
     * 
     * @return block size of the device in bytes.
     */
    int getBlockSize();

    /**
     * Copies bytes from storage to the passed byte buffer. The bytes are copied in the current position of the buffer.
     * In case of success, the buffer position is updated.
     * 
     * @param bytes
     *            the buffer into which the data will be copied
     * @param length
     *            the number of bytes to copy
     * @param storageIndex
     *            the position of the first byte to be copied in the source storage.
     * @throws IOException
     */
    void read(ByteBuffer bytes, int length, long storageIndex) throws IOException;

    /**
     * Writes part of the passed byte buffer content. The bytes must be copied from the current position.
     * 
     * @param bytes
     *            the source of the data to be written
     * @param length
     *            the number of bytes to be copied
     * @param storageIndex
     *            byte offset in the storage area
     * @throws IOException
     */
    void write(ByteBuffer bytes, int length, long storageIndex) throws IOException;

}
