package io.eguan.nbdsrv;

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

import io.eguan.nbdsrv.NbdExport;
import io.eguan.nbdsrv.NbdDeviceFile.TestTrim;
import io.eguan.nbdsrv.client.Client;
import io.eguan.nbdsrv.packet.NbdException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import junit.framework.AssertionFailedError;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataPushing extends TestNbdAbstract {
    private Client client;
    static final int blockSize = 4096;
    static final int numBlocks = 4 * 64;
    static final int length = 4;
    static final long size = 8192 * 1024L * 1024L;

    @Before
    public final void handshake() throws IOException, InterruptedException, NbdException {
        client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
        client.handshake();
        client.setExportName(deviceFileName);
    }

    @After
    public final void disconnect() throws IOException, NbdException, InterruptedException {
        client.disconnect();
    }

    @Test
    public void testTargetTrim() throws Exception {

        for (int i = 0; i < 128; i++) {
            client.trim(i, 4096);
        }

        for (int i = 0; i < 128; i++) {
            final TestTrim trim = device.peekTrim();
            Assert.assertEquals(i, trim.getOffset());
            Assert.assertEquals(4096, trim.getLength());
        }
        try {
            device.peekTrim();
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IndexOutOfBoundsException e) {
            // Ok
        }
    }

    @Test(expected = NbdException.class)
    public void testTargetWriteTooLong() throws Exception {

        // Create another little device
        final File littleDeviceFile = File.createTempFile("testLittleDevice", null);
        try {
            final String littleDeviceFileName = littleDeviceFile.getAbsolutePath();
            final NbdExport export = new NbdExport(littleDeviceFileName, Main.createNbdDeviceFile(littleDeviceFileName,
                    1024));
            try {
                server.addTarget(export);

                final Client newClient = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
                newClient.handshake();
                newClient.setExportName(littleDeviceFileName);

                final ByteBuffer writeData = ByteBuffer.allocate(blockSize);

                newClient.write(writeData, 0);

                // Should not go here
                newClient.disconnect();
            }
            finally {
                server.removeTarget(export.getTargetName());
            }
        }
        finally {
            if (littleDeviceFile != null) {
                littleDeviceFile.delete();
            }
        }
    }

    @Test(expected = NbdException.class)
    public void testTargetWriteOffsetNegative() throws Exception {

        // Create another little device
        final File littleDeviceFile = File.createTempFile("testLittleDevice", null);
        try {
            final String littleDeviceFileName = littleDeviceFile.getAbsolutePath();
            final NbdExport export = new NbdExport(littleDeviceFileName, Main.createNbdDeviceFile(littleDeviceFileName,
                    1024));
            try {
                server.addTarget(export);

                final Client newClient = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
                newClient.handshake();
                newClient.setExportName(littleDeviceFileName);

                final ByteBuffer writeData = ByteBuffer.allocate(blockSize);

                newClient.write(writeData, -4);

                // Should not go here
                newClient.disconnect();
            }
            finally {
                server.removeTarget(export.getTargetName());
            }
        }
        finally {
            if (littleDeviceFile != null) {
                littleDeviceFile.delete();
            }
        }
    }

    @Test(expected = NbdException.class)
    public void testTargetWriteOnReadOnlyDevice() throws Exception {
        // Create another little device
        final File readOnlyDeviceFile = File.createTempFile("testReadOnlyDevice", null);
        try {
            final String readOnlyDeviceFileName = readOnlyDeviceFile.getAbsolutePath();
            final NbdExport export = new NbdExport(readOnlyDeviceFileName, Main.createReadOnlyDeviceFile(
                    readOnlyDeviceFileName, blockSize * 4));
            try {
                server.addTarget(export);

                final Client newClient = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
                newClient.handshake();
                newClient.setExportName(readOnlyDeviceFileName);

                final ByteBuffer writeData = ByteBuffer.allocate(blockSize);

                newClient.write(writeData, 0);

                // Should not go here
                newClient.disconnect();
            }
            finally {
                server.removeTarget(export.getTargetName());
            }
        }
        finally {
            if (readOnlyDeviceFile != null) {
                readOnlyDeviceFile.delete();
            }
        }
    }

    @Test(expected = NbdException.class)
    public void testTargetReadTooLong() throws Exception {
        client.readTooLong();
    }
}
