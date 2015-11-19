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

import io.eguan.utils.ByteBuffers;
import io.eguan.utils.ByteBuffers.ByteBufferFactory;
import io.eguan.vvr.repository.core.api.Device;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * Unit tests on device with Ibs that have error.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestDeviceIoErr extends TestDeviceAbstract {

    public TestDeviceIoErr() {
        super(true);
    }

    @Test
    public void testMultiWriteErrReadArray() throws IOException {
        testMultiWriteErrRead(ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testMultiWriteErrReadDirect() throws IOException {
        testMultiWriteErrRead(ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    /**
     * Write, re-write with failure than read first written blocks. Take a snapshot to have block coming from a
     * different NRS file.
     * 
     * @param factory
     * @throws IOException
     */
    private void testMultiWriteErrRead(final ByteBufferFactory factory) throws IOException {

        final int bufLen = deviceBlockSize * 7;
        final byte[] buf1 = new byte[bufLen];
        final byte[] buf2 = new byte[bufLen];
        final Random random = new SecureRandom();
        random.nextBytes(buf1);
        random.nextBytes(buf2);
        final ByteBuffer byteBuf1 = factory.newByteBuffer(buf1);
        final ByteBuffer byteBuf2 = factory.newByteBuffer(buf2);

        try (Device.ReadWriteHandle handle = device.open(true)) {
            // Make sure there are partial IOs
            final long position = deviceBlockSize / 2;

            final int bufOffset = 0;
            final int bufLength = bufLen;

            // Write first blocks
            handle.write(byteBuf1, bufOffset, bufLength, position);
            {
                // Read blocks: should be equals to the first ones
                final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                handle.read(byteBufRead, bufOffset, bufLen, position);

                // Check position and compare
                Assert.assertEquals(bufLen, byteBuf1.position());
                Assert.assertEquals(bufLen, byteBufRead.position());
                ByteBuffers.assertEqualsByteBuffers(byteBuf1, byteBufRead);
            }

            // Write second blocks at the same position: should fail
            {
                try {
                    handle.write(byteBuf2, bufOffset, bufLength, position);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // OK
                }
            }

            // Read blocks: should be equals to the first ones
            {
                // Read blocks: should be equals to the first ones
                final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                handle.read(byteBufRead, bufOffset, bufLen, position);

                // Check position and compare
                Assert.assertEquals(bufLen, byteBuf1.position());
                Assert.assertEquals(bufLen, byteBufRead.position());
                ByteBuffers.assertEqualsByteBuffers(byteBuf1, byteBufRead);
            }

            // Take snapshot to have blocks coming from another NRS file
            device.createSnapshot();

            // Write second blocks at the same position: should fail again
            {
                try {
                    handle.write(byteBuf2, bufOffset, bufLength, position);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // OK
                }
            }

            // Read blocks: should be equals to the first ones
            {
                // Read blocks: should be equals to the first ones
                final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                handle.read(byteBufRead, bufOffset, bufLen, position);

                // Check position and compare
                Assert.assertEquals(bufLen, byteBuf1.position());
                Assert.assertEquals(bufLen, byteBufRead.position());
                ByteBuffers.assertEqualsByteBuffers(byteBuf1, byteBufRead);
            }

        }
    }

    /**
     * First write fails on an empty device, than read blocks that should be filled with 0.
     * 
     * @throws IOException
     */
    @Test
    public void testMultiWriteErrReadZeroArray() throws IOException {
        testMultiWriteErrReadZero(ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testMultiWriteErrReadZeroDirect() throws IOException {
        testMultiWriteErrReadZero(ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    private void testMultiWriteErrReadZero(final ByteBufferFactory factory) throws IOException {

        final int bufLen = deviceBlockSize * 10;
        final byte[] buf1 = new byte[bufLen];
        final Random random = new SecureRandom();
        random.nextBytes(buf1);
        final ByteBuffer byteBuf1 = factory.newByteBuffer(buf1);
        final ByteBuffer byteBufRef = factory.newByteBuffer(bufLen);
        byteBufRef.position(bufLen);

        try (Device.ReadWriteHandle handle = device.open(true)) {
            // Make sure there are partial IOs
            final long position = deviceBlockSize / 2;

            final int bufOffset = 0;
            final int bufLength = bufLen;

            // Write first blocks: should fail
            {
                try {
                    handle.write(byteBuf1, bufOffset, bufLength, position);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // OK
                }
            }

            // Read blocks: should be full of 0
            {
                final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                handle.read(byteBufRead, bufOffset, bufLen, position);

                // Check position and compare
                Assert.assertEquals(bufLen, byteBufRef.position());
                Assert.assertEquals(bufLen, byteBufRead.position());
                ByteBuffers.assertEqualsByteBuffers(byteBufRef, byteBufRead);
            }

            // Take snapshot to have a previous NRS file
            device.createSnapshot();

            // Write first blocks: should fail again
            {
                try {
                    handle.write(byteBuf1, bufOffset, bufLength, position);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // OK
                }
            }

            // Read blocks: should be full of 0
            {
                final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                handle.read(byteBufRead, bufOffset, bufLen, position);

                // Check position and compare
                Assert.assertEquals(bufLen, byteBufRef.position());
                Assert.assertEquals(bufLen, byteBufRead.position());
                ByteBuffers.assertEqualsByteBuffers(byteBufRef, byteBufRead);
            }
        }
    }

    /**
     * Test of data re-write, but on a single full write.
     * 
     * @throws IOException
     */
    @Test
    public void testSingleWriteFullErrReadArray() throws IOException {
        testSingleWriteErrRead(deviceBlockSize, deviceBlockSize, ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testSingleWriteFullErrReadDirect() throws IOException {
        testSingleWriteErrRead(deviceBlockSize, deviceBlockSize, ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    /**
     * Test of data write failure, but on a single full write.
     * 
     * @throws IOException
     */
    @Test
    public void testSingleWriteFullErrReadZeroArray() throws IOException {
        testSingleWriteErrReadZero(deviceBlockSize, deviceBlockSize, ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testSingleWriteFullErrReadZeroDirect() throws IOException {
        testSingleWriteErrReadZero(deviceBlockSize, deviceBlockSize, ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    /**
     * Test of data re-write, but on a single partial write.
     * 
     * @throws IOException
     */
    @Test
    public void testSingleWritePartialErrReadArray() throws IOException {
        testSingleWriteErrRead(deviceBlockSize / 3, deviceBlockSize / 10, ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testSingleWritePartialErrReadDirect() throws IOException {
        testSingleWriteErrRead(deviceBlockSize / 3, deviceBlockSize / 10, ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    /**
     * Test of data write failure, but on a single partial write.
     * 
     * @throws IOException
     */
    @Test
    public void testSingleWritePartialErrReadZeroArray() throws IOException {
        testSingleWriteErrReadZero(deviceBlockSize / 3, deviceBlockSize / 10, ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testSingleWritePartialErrReadZeroDirect() throws IOException {
        testSingleWriteErrReadZero(deviceBlockSize / 3, deviceBlockSize / 10, ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    private void testSingleWriteErrRead(final int bufLen, final long position, final ByteBufferFactory factory)
            throws IOException {

        final byte[] buf1 = new byte[bufLen];
        final Random random = new SecureRandom();

        final int bufOffset = 0;
        int writeCount = 0;

        try (Device.ReadWriteHandle handle = device.open(true)) {

            ByteBuffer byteBufPrev = null;

            while (true) {

                // New contents
                random.nextBytes(buf1);
                final ByteBuffer byteBufCurrent = factory.newByteBuffer(buf1);

                // Write
                try {
                    handle.write(byteBufCurrent, bufOffset, bufLen, position);

                    writeCount++;

                    // Read blocks: should be equals to the current one
                    final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                    handle.read(byteBufRead, bufOffset, bufLen, position);

                    // Check position and compare
                    Assert.assertEquals(bufLen, byteBufCurrent.position());
                    Assert.assertEquals(bufLen, byteBufRead.position());
                    ByteBuffers.assertEqualsByteBuffers(byteBufCurrent, byteBufRead);

                    // Keep previous buffer for the IOException to come
                    byteBufPrev = byteBufCurrent;
                }
                catch (final IOException e) {

                    // Write count: must fail after 9 writes (see
                    // TestValidIbsConfigurationContext.ContextTestHelperIbsError)
                    Assert.assertEquals(9, writeCount);

                    // Read blocks: should be equals to previous
                    final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                    handle.read(byteBufRead, bufOffset, bufLen, position);

                    // Check position and compare
                    Assert.assertEquals(bufLen, byteBufPrev.position());
                    Assert.assertEquals(bufLen, byteBufRead.position());
                    ByteBuffers.assertEqualsByteBuffers(byteBufPrev, byteBufRead);

                    // Test done
                    break;
                }
            }
        }
    }

    private void testSingleWriteErrReadZero(final int bufLen, long position, final ByteBufferFactory factory)
            throws IOException {
        final byte[] buf1 = new byte[bufLen];
        final Random random = new SecureRandom();

        final ByteBuffer byteBufRef = factory.newByteBuffer(bufLen);
        byteBufRef.position(bufLen);

        final int bufOffset = 0;
        final int bufLength = bufLen;
        int writeCount = 0;

        try (Device.ReadWriteHandle handle = device.open(true)) {

            while (true) {
                position += deviceBlockSize;

                // Switch buffers and fill current with new bytes
                random.nextBytes(buf1);
                final ByteBuffer byteBufCurrent = factory.newByteBuffer(buf1);

                // Write
                try {
                    handle.write(byteBufCurrent, bufOffset, bufLength, position);

                    writeCount++;

                    // Read blocks: should be equals to the current one
                    final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                    handle.read(byteBufRead, bufOffset, bufLen, position);

                    // Check position and compare
                    Assert.assertEquals(bufLen, byteBufCurrent.position());
                    Assert.assertEquals(bufLen, byteBufRead.position());
                    ByteBuffers.assertEqualsByteBuffers(byteBufCurrent, byteBufRead);
                }
                catch (final IOException e) {

                    // Write count: must fail after 9 writes (see
                    // TestValidIbsConfigurationContext.ContextTestHelperIbsError)
                    Assert.assertEquals(9, writeCount);

                    final ByteBuffer byteBufRead = factory.newByteBuffer(bufLen);
                    handle.read(byteBufRead, bufOffset, bufLen, position);

                    // Check position and compare
                    Assert.assertEquals(bufLen, byteBufRef.position());
                    Assert.assertEquals(bufLen, byteBufRead.position());
                    ByteBuffers.assertEqualsByteBuffers(byteBufRef, byteBufRead);

                    // Test done
                    break;
                }
            }
        }
    }
}
