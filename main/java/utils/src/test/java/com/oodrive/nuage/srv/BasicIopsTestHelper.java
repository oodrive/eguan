package com.oodrive.nuage.srv;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import junit.framework.Assert;

public class BasicIopsTestHelper extends AbstractIopsTestHelper {

    public BasicIopsTestHelper(final int blockSize, final int numBlocks, final int length) {
        super(blockSize, numBlocks, length);
    }

    private final void writeData(final File dataDump, final ClientBasicIops client, final String target,
            final int increment) throws Exception {

        final Random random = new Random(System.currentTimeMillis());
        final ByteBuffer readData = ByteBuffer.allocate(blockSize * numBlocks);
        try (final RandomAccessFile raf = new RandomAccessFile(dataDump.getAbsolutePath(), "rw")) {
            final FileChannel out = raf.getChannel();
            final ByteBuffer writeData = ByteBuffer.allocate(blockSize * numBlocks);
            for (int i = 0; i < length; i += increment) {
                random.nextBytes(writeData.array());
                final int lba = i * numBlocks;
                client.write(target, writeData, lba, writeData.capacity(), blockSize);
                writeData.rewind();
                out.write(writeData, lba * blockSize);
                writeData.rewind();
                client.read(target, readData, lba, readData.capacity(), blockSize);
                readData.rewind();
                Assert.assertEquals(readData, writeData);
            }
        }
    }

    private final void readData(final File dataDump, final ClientBasicIops client, final String target, final long size)
            throws Exception {

        final ByteBuffer readData = ByteBuffer.allocate(blockSize * numBlocks);
        final ByteBuffer refData = ByteBuffer.allocate(blockSize * numBlocks);

        try (final FileInputStream fis = new FileInputStream(dataDump.getAbsolutePath())) {
            final FileChannel in = fis.getChannel();
            for (int i = 0; i < length; i++) {
                final int lba = i * numBlocks;
                int toRead = blockSize * numBlocks;
                toRead -= in.read(refData, lba * blockSize);
                while (toRead > 0) {
                    toRead -= in.read(refData);
                }
                refData.rewind();
                client.read(target, readData, lba, readData.capacity(), blockSize);
                readData.rewind();
                Assert.assertEquals("Error i=" + i + ", lba=" + lba, readData, refData);
            }
        }
        client.checkCapacity(target, size);
    }

    /**
     * Read data on a node with an Initiator and compare it with the original file
     * 
     * @param referenceFile
     *            the reference File to compare the data read from the target
     * @param client
     *            the initiator used to connect the iscsi server
     * @param targetName
     *            the target name
     * @param targetSize
     *            the target size
     * 
     */
    public final void initiatorReadData(final File referenceFile, final ClientBasicIops client,
            final String targetName, final long targetSize) throws Exception {

        client.createSession(targetName);
        try {
            readData(referenceFile, client, targetName, targetSize);
        }
        finally {
            client.closeSession(targetName);
        }
    }

    public final File initiatorReadWriteData(final ClientBasicIops client, final String targetName,
            final long targetSize) throws Exception {
        return initiatorReadWriteData(client, targetName, targetSize, 1, null);
    }

    /**
     * Write data on a node and on a reference file with an Initiator and read it to compare with the reference file
     * 
     * @param client
     *            the initiator used to connect the iscsi server.
     * @param targetName
     *            the target name.
     * @param targetSize
     *            the target size.
     * @param increment
     *            define the scope of the write of the file: 1 for all the file, 2 for half, etc...
     * @param
     * 
     * @return the new file created
     */
    public final File initiatorReadWriteData(final ClientBasicIops client, final String targetName,
            final long targetSize, final int increment, final File referencePrev) throws Exception {

        final File referenceFile = referencePrev == null ? File.createTempFile("vold", "test") : referencePrev;
        try {
            client.createSession(targetName);
            try {
                writeData(referenceFile, client, targetName, increment);
                readData(referenceFile, client, targetName, targetSize);
            }
            finally {
                client.closeSession(targetName);
            }
        }
        catch (final Exception | Error e) {
            referenceFile.delete();
            throw e;
        }
        return referenceFile;
    }

    /**
     * Multi thread R/W method.
     * 
     * @param executor
     * @param target
     * @param iopClient
     * @param size
     * @param the
     *            file used to check the data
     * 
     * @return the future to wait the result
     */
    public Future<File> multiThreadRW(final ExecutorService executor, final String target,
            final ClientBasicIops iopClient, final long size, final File filename) {

        final Future<File> future = executor.submit(new Callable<File>() {
            @Override
            public File call() throws Exception {
                return initiatorReadWriteData(iopClient, target, size, 1, filename);
            }
        });
        return future;
    }

    /**
     * Multi thread R/W method.
     * 
     * @param executor
     * @param target
     * @param iopClient
     * @param size
     * 
     * @return the future to wait the result
     */
    public Future<File> multiThreadRW(final ExecutorService executor, final String target,
            final ClientBasicIops iopClient, final long size) {

        final Future<File> future = executor.submit(new Callable<File>() {
            @Override
            public File call() throws Exception {
                return initiatorReadWriteData(iopClient, target, size);
            }
        });
        return future;
    }

}
