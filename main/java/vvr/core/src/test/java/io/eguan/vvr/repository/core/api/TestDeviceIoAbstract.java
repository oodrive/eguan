package io.eguan.vvr.repository.core.api;

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

import io.eguan.utils.ByteArrays;
import io.eguan.vvr.repository.core.api.Device;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests to read and write in VVR {@link Device}.
 *
 * @author oodrive
 * @author llambert
 *
 */
public abstract class TestDeviceIoAbstract extends TestDeviceAbstract {

    protected TestDeviceIoAbstract(final boolean helpersErr) {
        super(helpersErr);
    }

    /**
     * Write datas in the device and in a file, than compare the contents of the file and the device.
     *
     * @throws IOException
     */
    @Test
    public void testReadWrite() throws IOException {
        final File fileTmp = File.createTempFile("vvrIO", ".tmp");
        try {
            try (RandomAccessFile raf = new RandomAccessFile(fileTmp, "rw")) {
                // Set file size
                raf.setLength(device.getSize());

                // Compare: should have only 0
                compareDirect(fileTmp, device, deviceBlockSize);
                compare(fileTmp, device, deviceBlockSize);

                final byte[] buffer = new byte[deviceBlockSize * 4];
                final Random random = new SecureRandom();
                random.nextBytes(buffer);
                try (Device.ReadWriteHandle handle = device.open(true)) {
                    // Write first block
                    long position = 0L;
                    int bufOffset = 0;
                    int bufLength = deviceBlockSize;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Write last block
                    position = device.getSize() - deviceBlockSize;
                    bufOffset = deviceBlockSize;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Write a block in the middle
                    position = 3 * deviceBlockSize;
                    bufOffset = 2 * deviceBlockSize;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Same block in the middle, a second time
                    position = 3 * deviceBlockSize;
                    bufOffset = 2 * deviceBlockSize;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Write a portion of a block
                    position = 2 * deviceBlockSize + 5;
                    bufOffset = deviceBlockSize / 5 + 2;
                    bufLength = deviceBlockSize / 6 + 12;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Write two blocks, aligned
                    position = 6 * deviceBlockSize;
                    bufOffset = deviceBlockSize + 512;
                    bufLength = deviceBlockSize * 2;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Write two blocks, not aligned
                    position += deviceBlockSize / 11;
                    bufOffset = deviceBlockSize + 112;
                    bufLength = deviceBlockSize * 2;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    device.createSnapshot("Test snap");

                    // Write some data, not aligned
                    position += deviceBlockSize;
                    bufOffset = 12;
                    bufLength = deviceBlockSize * 3 + 33;
                    write(buffer, position, bufOffset, bufLength, raf, handle);

                    // Rewrite some data, not aligned
                    position -= deviceBlockSize / 2;
                    bufOffset = 14;
                    bufLength = deviceBlockSize + 33;
                    write(buffer, position, bufOffset, bufLength, raf, handle);
                }
            }

            // Compare, read device with direct buffer
            // Read, block by block
            compareDirect(fileTmp, device, deviceBlockSize);
            // Read, 2 blocks at a time
            compareDirect(fileTmp, device, 2 * deviceBlockSize);
            // Read, less than one block
            compareDirect(fileTmp, device, deviceBlockSize / 11 + 3);
            // Read, more than one block
            compareDirect(fileTmp, device, deviceBlockSize + deviceBlockSize / 5 + 3);

            // Compare, read device with HeapBuffer
            // Read, block by block
            compare(fileTmp, device, deviceBlockSize);
            // Read, 2 blocks at a time
            compare(fileTmp, device, 2 * deviceBlockSize);
            // Read, less than one block
            compare(fileTmp, device, deviceBlockSize / 11 + 3);
            // Read, more than one block
            compare(fileTmp, device, deviceBlockSize + deviceBlockSize / 5 + 3);
        }
        finally {
            fileTmp.delete();
        }
    }

    private static final void write(final byte[] data, final long position, final int offset, final int length,
            final RandomAccessFile raf, final Device.ReadWriteHandle handle) throws IOException {
        // Write in file
        LOGGER.info("Write pos=" + position + ", offset=" + offset + ", length=" + length);
        raf.seek(position);
        raf.write(data, offset, length);

        // Write in device
        final ByteBuffer src = ByteBuffer.wrap(data);
        handle.write(src, offset, length, position);
    }

    private static final void compare(final File file, final Device device, final int readSize)
            throws FileNotFoundException, IOException {
        final byte[] bufFile = new byte[readSize];
        final byte[] bufDev = new byte[readSize];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        long devOffset = 0;
        try (Device.ReadWriteHandle handle = device.open(false)) {
            try (FileInputStream fis = new FileInputStream(file)) {
                int readLen;
                while ((readLen = fis.read(bufFile)) != -1) {
                    bds.rewind();
                    handle.read(bds, 0, readLen, devOffset);
                    // Compare read bytes
                    ByteArrays.assertEqualsByteArrays("devOffset=" + devOffset + " ", bufFile, bufDev);
                    devOffset += readLen;
                }
            }
        }
    }

    private static final void compareDirect(final File file, final Device device, final int readSize)
            throws FileNotFoundException, IOException {
        final byte[] bufFile = new byte[readSize];
        final ByteBuffer bds = ByteBuffer.allocateDirect(readSize);
        long devOffset = 0;
        try (Device.ReadWriteHandle handle = device.open(false)) {
            try (FileInputStream fis = new FileInputStream(file)) {
                int readLen;
                while ((readLen = fis.read(bufFile)) != -1) {
                    bds.rewind();
                    handle.read(bds, 0, readLen, devOffset);
                    // Compare read bytes
                    for (int i = 0; i < readLen; i++) {
                        Assert.assertEquals("devOffset=" + devOffset + i, bufFile[i], bds.get(i));
                    }
                    devOffset += readLen;
                }
            }
        }
    }

    /**
     * Reads and writes data from different threads on the same device handle.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testConcurrentReadWrite() throws IOException, InterruptedException {
        final Random random = new SecureRandom();

        final int wlen1 = deviceBlockSize * 32;
        final int wlen2 = deviceBlockSize * 16;
        final int rlen = deviceBlockSize * 64;

        final long offsetW1 = deviceBlockSize / 2L;

        final long offsetW2 = deviceBlockSize / 2L + wlen1 + deviceBlockSize;

        final long offsetWOverride = deviceBlockSize / 2L + wlen1 - deviceBlockSize;

        final long offsetR3 = 0L;

        final byte[] writeBuf1 = new byte[wlen1];
        random.nextBytes(writeBuf1);
        final byte[] writeBuf2 = new byte[wlen2];
        random.nextBytes(writeBuf2);

        final byte[] readBuf1 = new byte[rlen];
        final byte[] readBuf2 = new byte[rlen];
        final byte[] readBuf3 = new byte[rlen];
        { // First write2
            System.arraycopy(writeBuf2, 0, readBuf1, 0, wlen2);
        }
        { // First write2+override
            System.arraycopy(writeBuf2, 0, readBuf2, 0, wlen2);
            final int deltaOffset = (int) (offsetW2 - offsetWOverride);
            System.arraycopy(writeBuf2, deltaOffset, readBuf2, 0, wlen2 - deltaOffset);
        }
        { // write2
            int destOffset = (int) (offsetW2 - offsetR3);
            System.arraycopy(writeBuf2, 0, readBuf3, destOffset, wlen2);
            // override
            destOffset = (int) (offsetWOverride - offsetR3);
            System.arraycopy(writeBuf2, 0, readBuf3, destOffset, wlen2);
            // write1
            destOffset = (int) (offsetW1 - offsetR3);
            System.arraycopy(writeBuf1, 0, readBuf3, destOffset, wlen1);
        }

        try (final Device.ReadWriteHandle handle = device.open(true)) {

            // Init device contents for read
            {
                ByteBuffer source = ByteBuffer.wrap(writeBuf1);
                handle.write(source, 0, wlen1, offsetW1);
                source = ByteBuffer.wrap(writeBuf2);
                handle.write(source, 0, wlen2, offsetW2);
            }

            // Reader
            final AtomicReference<Throwable> readerRef = new AtomicReference<>();
            final Thread reader;
            {
                reader = new Thread(new Runnable() {

                    @Override
                    public final void run() {
                        try {
                            final byte[] readBuf = new byte[rlen];
                            final ByteBuffer destination = ByteBuffer.wrap(readBuf);

                            do {
                                destination.rewind();
                                handle.read(destination, 0, rlen, offsetW2);
                                Assert.assertEquals(rlen, destination.position());
                            } while (Arrays.equals(readBuf1, readBuf));

                            // Override done: get device final contents
                            destination.rewind();
                            handle.read(destination, 0, rlen, offsetR3);
                            ByteArrays.assertEqualsByteArrays(readBuf3, readBuf);
                        }
                        catch (final Throwable t) {
                            LOGGER.warn("Reader unexpected end", t);
                            readerRef.set(t);
                        }
                    }
                }, "Reader");
                reader.setDaemon(true);
                reader.start();
                Thread.yield();
            }

            // Writer1 - write first area, no conflict with read
            final AtomicReference<Throwable> writer1Ref = new AtomicReference<>();
            final Thread writer1;
            {
                writer1 = new Thread(new Runnable() {

                    @Override
                    public final void run() {
                        try {
                            Thread.sleep(100);
                            final ByteBuffer source = ByteBuffer.wrap(writeBuf1);
                            while (reader.isAlive()) {
                                source.rewind();
                                handle.write(source, 0, wlen1, offsetW1);
                                Assert.assertEquals(wlen1, source.position());
                            }
                        }
                        catch (final Throwable t) {
                            LOGGER.warn("Writer1 unexpected end", t);
                            writer1Ref.set(t);
                        }
                    }
                }, "Writer1");
                writer1.setDaemon(true);
                writer1.start();
            }

            // Writer2 - write first area, conflict with read (add delay in loop)
            final AtomicReference<Throwable> writer2Ref = new AtomicReference<>();
            final CyclicBarrier writer2Stop = new CyclicBarrier(2);
            final Thread writer2;
            {
                writer2 = new Thread(new Runnable() {

                    @Override
                    public final void run() {
                        try {
                            final ByteBuffer source = ByteBuffer.wrap(writeBuf2);
                            while (writer2Stop.getNumberWaiting() == 0) {
                                if (!reader.isAlive()) {
                                    return;
                                }

                                // Wait and write
                                Thread.sleep(100);
                                source.rewind();
                                handle.write(source, 0, wlen2, offsetW2);
                                Assert.assertEquals(wlen2, source.position());
                            }

                            // Trip barrier
                            writer2Stop.await();
                        }
                        catch (final Throwable t) {
                            LOGGER.warn("Writer2 unexpected end", t);
                            writer2Ref.set(t);
                        }
                    }
                }, "Writer2");
                writer2.setDaemon(true);
                writer2.start();
            }

            // Writer override: write one shot
            final AtomicReference<Throwable> writerOverrideRef = new AtomicReference<>();
            final Thread writerOverride;
            {
                writerOverride = new Thread(new Runnable() {

                    @Override
                    public final void run() {
                        try {
                            Thread.sleep(2000);
                            // Wait for end of writer2
                            writer2Stop.await();

                            final ByteBuffer source = ByteBuffer.wrap(writeBuf2);
                            handle.write(source, 0, wlen2, offsetWOverride);
                            Assert.assertEquals(wlen2, source.position());
                        }
                        catch (final Throwable t) {
                            LOGGER.warn("WriterOverride unexpected end", t);
                            writerOverrideRef.set(t);
                        }
                    }
                }, "Writer override");
                writerOverride.setDaemon(true);
                writerOverride.start();
            }

            reader.join();

            writer1.join();
            writer2.join();
            while (writerOverride.isAlive()) {
                writer2Stop.reset(); // Make sure the thread is not waiting on the barrier
                writerOverride.join(500);
            }

            assertRunOk("Reader", readerRef);
            assertRunOk("Writer1", writer1Ref);
            assertRunOk("Writer2", writer2Ref);
            assertRunOk("WriterOverride", writerOverrideRef);

            // Check device contents
            {
                final byte[] readBuf = new byte[wlen1];
                final ByteBuffer destination = ByteBuffer.wrap(readBuf);
                handle.read(destination, 0, wlen1, offsetW1);
                ByteArrays.assertEqualsByteArrays(writeBuf1, readBuf);
            }
            {
                final byte[] readBuf = new byte[rlen];
                final ByteBuffer destination = ByteBuffer.wrap(readBuf);
                handle.read(destination, 0, rlen, offsetW2);
                ByteArrays.assertEqualsByteArrays(readBuf2, readBuf);
            }
            {
                final byte[] readBuf = new byte[rlen];
                final ByteBuffer destination = ByteBuffer.wrap(readBuf);
                handle.read(destination, 0, rlen, offsetR3);
                ByteArrays.assertEqualsByteArrays(readBuf3, readBuf);
            }
        }
    }

    private final void assertRunOk(final String threadName, final AtomicReference<Throwable> throwableRef) {
        final Throwable runThrowable = throwableRef.get();
        if (runThrowable != null) {
            final AssertionFailedError afe = new AssertionFailedError(threadName);
            afe.initCause(runThrowable);
            throw afe;
        }
    }

    @Test(expected = IOException.class)
    public void testReadClosed() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 0;
        final Device.ReadWriteHandle handle = device.open(true);
        handle.close();
        handle.read(bds, 0, len, devOffset);
    }

    @Test(expected = IOException.class)
    public void testWriteClosed() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 0;
        final Device.ReadWriteHandle handle = device.open(true);
        handle.close();
        handle.write(bds, 0, len, devOffset);
    }

    @Test(expected = IOException.class)
    public void testWriteOpenedReadOnly() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 0;
        try (final Device.ReadWriteHandle handle = device.open(false)) {
            handle.write(bds, 0, len, devOffset);
        }
    }

    @Test(expected = IOException.class)
    public void testWriteDevOffsetNegative() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = -5;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(bds, 0, len, devOffset);
        }
    }

    @Test(expected = IOException.class)
    public void testWriteBufOffsetNegative() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 5;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(bds, -1, len, devOffset);
        }
    }

    @Test(expected = IOException.class)
    public void testWriteLenNegative() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 5;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(bds, 0, -12, devOffset);
        }
    }

    @Test(expected = IOException.class)
    public void testWriteDevOffsetOverflow() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = device.getSize() - 221;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(bds, 0, len, devOffset);
        }
    }

    @Test(expected = IOException.class)
    public void testWriteBufOffsetOverflow() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 5;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(bds, len / 2, len, devOffset);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testWriteNullBuffer() throws IOException {
        final int len = 14 * 1024;
        final long devOffset = 5;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            handle.write(null, 0, len, devOffset);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testReadNullBuffer() throws IOException {
        final int len = 14 * 1024;
        final long devOffset = 5;
        try (final Device.ReadWriteHandle handle = device.open(false)) {
            handle.read(null, 0, len, devOffset);
        }
    }

    @Test
    public void testWriteNullLen() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final Random random = new SecureRandom();
        random.nextBytes(bufDev);
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);

        final byte[] bufCheck = Arrays.copyOf(bufDev, len);
        final byte[] bufRead = new byte[len];
        final ByteBuffer bdr = ByteBuffer.wrap(bufRead);

        final long devOffset = 22;
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            // Write random contents
            handle.write(bds, 0, len, devOffset);
            Assert.assertEquals(len, bds.position());

            // Read contents
            handle.read(bdr, 0, len, devOffset);
            Assert.assertTrue(Arrays.equals(bufCheck, bufRead));
            Assert.assertEquals(len, bdr.position());

            // Empty write
            bds.rewind();
            handle.write(bds, 2, 0, devOffset);
            Assert.assertEquals(0, bds.position());

            // Read contents again
            bdr.rewind();
            handle.read(bdr, 0, len, devOffset);
            Assert.assertEquals(len, bdr.position());
            Assert.assertTrue(Arrays.equals(bufCheck, bufRead));

        }
    }

    @Test
    public void testReadNullLen() throws IOException {
        final int len = 14 * 1024;
        final byte[] bufDev = new byte[len];
        final Random random = new SecureRandom();
        random.nextBytes(bufDev);
        final byte[] bufCheck = Arrays.copyOf(bufDev, len);
        final ByteBuffer bds = ByteBuffer.wrap(bufDev);
        final long devOffset = 5;

        // Read one byte to check read
        try (final Device.ReadWriteHandle handle = device.open(true)) {
            // Should not change contents when reading one byte
            handle.read(bds, 0, 0, devOffset);
            Assert.assertTrue(Arrays.equals(bufCheck, bufDev));

            // Read one byte to check read and compare
            handle.read(bds, 1, 1, devOffset);
            Assert.assertFalse(Arrays.equals(bufCheck, bufDev));
            Assert.assertEquals(0, bufDev[1]);
        }
    }

}
