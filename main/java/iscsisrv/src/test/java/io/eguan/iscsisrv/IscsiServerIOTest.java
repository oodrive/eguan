package io.eguan.iscsisrv;

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

import io.eguan.iscsisrv.IscsiServerConfig;
import io.eguan.iscsisrv.IscsiTarget;
import io.eguan.srv.TestAbstractServerIO;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Future;

import org.jscsi.target.TargetServer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IscsiServerIOTest extends
        TestAbstractServerIO<TargetServer, IscsiTarget, IscsiServerConfig, IscsiServerTargetImpl> {

    static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerIOTest.class);

    public IscsiServerIOTest() {
        super(new IscsiServerTargetImpl());
    }

    @Test
    public void testTargetMultiThreadedWrite() throws Exception {

        final File deviceFile = File.createTempFile("testDevice", null);
        targets.put(deviceFile, Long.valueOf(size));
        mgr.addTarget(server, targets);

        final File tmpFile = File.createTempFile("vold", "test");
        try {
            final InitiatorClientBasicIops iopClient = (InitiatorClientBasicIops) mgr.initClient();

            iopClient.createSession(mgr.getTargetName(deviceFile));
            try {
                writeData(tmpFile, iopClient, mgr.getTargetName(deviceFile));
                readData(tmpFile, iopClient, mgr.getTargetName(deviceFile), size);
            }
            finally {
                iopClient.closeSession(mgr.getTargetName(deviceFile));
            }
        }
        finally {
            tmpFile.delete();
        }
    }

    private final void writeData(final File dataDump, final InitiatorClientBasicIops initiator, final String target)
            throws Exception {

        final Random random = new Random(System.currentTimeMillis());

        final ArrayList<Future<Void>> writes = new ArrayList<>();

        try (final RandomAccessFile raf = new RandomAccessFile(dataDump.getAbsolutePath(), "rw")) {
            final FileChannel out = raf.getChannel();
            for (int i = 0; i < LENGTH; i++) {
                final ByteBuffer writeData = ByteBuffer.allocate(BLOCKSIZE * NUMBLOCKS);
                final int lba = i * NUMBLOCKS;
                random.nextBytes(writeData.array());
                // Write buffer in the tmp file
                out.write(writeData, lba * BLOCKSIZE);
                writeData.rewind();
                // Write it on the target
                writes.add(initiator.multiThreadedWrite(target, writeData, lba, writeData.capacity(), BLOCKSIZE));
            }
            for (int i = 0; i < LENGTH; i++) {
                writes.get(i).get();
            }
        }
    }

    private final void readData(final File dataDump, final InitiatorClientBasicIops initiator, final String target,
            final long size) throws Exception {

        final ByteBuffer readData = ByteBuffer.allocate(BLOCKSIZE * NUMBLOCKS);
        final ByteBuffer refData = ByteBuffer.allocate(BLOCKSIZE * NUMBLOCKS);

        try (final FileInputStream fis = new FileInputStream(dataDump.getAbsolutePath())) {
            final FileChannel in = fis.getChannel();
            for (int i = 0; i < LENGTH; i++) {
                final int lba = i * NUMBLOCKS;
                int toRead = BLOCKSIZE * NUMBLOCKS;
                toRead -= in.read(refData, lba * BLOCKSIZE);
                while (toRead > 0) {
                    toRead -= in.read(refData);
                }
                refData.rewind();
                initiator.read(target, readData, lba, readData.capacity(), BLOCKSIZE);
                readData.rewind();
                Assert.assertEquals("Error i=" + i + ", lba=" + lba, readData, refData);
            }
        }
        /* !!! Initiator.getCapacity() == last block address */
        initiator.checkCapacity(target, size);
    }
}
