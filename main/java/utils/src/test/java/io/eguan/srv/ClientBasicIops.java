package io.eguan.srv;

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

public interface ClientBasicIops {

    /**
     * Write data on a device
     * 
     * @param targetName
     *            the name of the device
     * @param src
     *            the {@link ByteBuffer} which contains data
     * @param logicalBlockAddress
     *            the first block address
     * @param transferLength
     *            the number of bytes to transfer
     * @param blockSize
     *            the block size
     * 
     * @throws Exception
     */
    public void write(final String targetName, final ByteBuffer src, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws Exception;

    /**
     * Read data on a device
     * 
     * @param targetName
     *            the name of the device
     * @param dst
     *            the {@link ByteBuffer} to write the data
     * @param logicalBlockAddress
     *            the first block address
     * @param transferLength
     *            the number of bytes to transfer
     * @param blockSize
     *            the size of a block
     * @throws Exception
     */
    public void read(final String targetName, final ByteBuffer dst, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws Exception;

    /**
     * Create a session on the target.
     * 
     * @param targetName
     *            the name of the device
     * @throws Exception
     */
    public void createSession(final String targetName) throws Exception;

    /**
     * Close a session.
     * 
     * @param targetName
     *            the name of the device
     * @throws Exception
     */
    public void closeSession(final String targetName) throws Exception;

    /**
     * Check if all the device has been filled
     * 
     * @param target
     *            the name of the target
     * @param size
     *            the size of the target
     * @throws Exception
     */
    public void checkCapacity(String target, long size) throws Exception;
}
