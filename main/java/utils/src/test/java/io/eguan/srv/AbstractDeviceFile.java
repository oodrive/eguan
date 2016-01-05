package io.eguan.srv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDeviceFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDeviceFile.class);

    protected AbstractDeviceFile(final FileChannel fileChannel, final String path) {
        super();
        this.fileChannel = fileChannel;
        this.path = path;
    }

    /** Synchronize access on the position and on the limit on this */
    private final FileChannel fileChannel;
    /** For logs and toString() */
    private final String path;

    public final long getSize() {
        try {
            return fileChannel.size();
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final synchronized void read(final ByteBuffer bytes, final int length, final long storageIndex)
            throws IOException {
        fileChannel.position(storageIndex);
        bytes.limit(bytes.position() + length);
        LOGGER.debug("Target read '" + path + "' index=" + storageIndex + ", length=" + length + ", filesize="
                + fileChannel.size());
        final int readCount;
        try {
            readCount = fileChannel.read(bytes);
        }
        catch (final IOException e) {
            LOGGER.warn("Read failed '" + path + "' index=" + storageIndex + ", length=" + length + ", filesize="
                    + fileChannel.size(), e);
            throw e;
        }
        LOGGER.debug("Target read '" + path + "' index=" + storageIndex + ", length=" + length + ", readCount="
                + readCount);
    }

    public final synchronized void write(final ByteBuffer bytes, final int length, final long storageIndex)
            throws IOException {
        fileChannel.position(storageIndex);
        bytes.limit(bytes.position() + length);
        LOGGER.debug("Target write '" + path + "' index=" + storageIndex + ", length=" + length + ", filesize="
                + fileChannel.size());
        final int writeCount;
        try {
            writeCount = fileChannel.write(bytes);
        }
        catch (final IOException e) {
            LOGGER.warn("Write failed '" + path + "' index=" + storageIndex + ", length=" + length + ", filesize="
                    + fileChannel.size(), e);
            throw e;
        }
        LOGGER.debug("Target write '" + path + "' index=" + storageIndex + ", length=" + length + ", writeCount="
                + writeCount);
    }

    public final void close() throws IOException {
        LOGGER.debug("Target closed file='" + path + "'");
        fileChannel.close();
    }

}
