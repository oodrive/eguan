package com.oodrive.nuage.nbdsrv;

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

public interface NbdDevice extends Closeable {

    /**
     * Tells if the device is write protected.
     * 
     * @return <code>true</code> if the device is write protected
     */
    public boolean isReadOnly();

    /**
     * Gets the size of the device.
     * 
     * @return the number of bytes which can be read/write in the device
     */
    public long getSize();

    /**
     * Read bytes from device to a byte buffer.
     * 
     * @param dst
     *            the {@link ByteBuffer} which will be filled with data from device
     * @param length
     *            the number of bytes to read
     * @param offset
     *            the offset of the first byte to be read
     * 
     * @throws IOException
     */
    public void read(ByteBuffer dst, int length, long offset) throws IOException;

    /**
     * Write bytes from a byte Buffer to the device.
     * 
     * @param src
     *            the {@link ByteBuffer} which contains the data to be written
     * @param length
     *            the number of bytes to write
     * @param offset
     *            the offset of the first byte to write
     * @throws IOException
     */
    public void write(ByteBuffer src, int length, long offset) throws IOException;

    /**
     * trim in a device.
     * 
     * @param length
     *            the length to trim
     * @param offset
     *            the offset of the first byte to trim
     */
    public void trim(long length, long offset) throws IOException;

}
