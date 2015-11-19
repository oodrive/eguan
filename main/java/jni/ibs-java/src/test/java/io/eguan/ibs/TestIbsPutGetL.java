package io.eguan.ibs;

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

import static io.eguan.utils.ByteBuffers.FACTORY_BYTE_ARRAY;
import static io.eguan.utils.ByteBuffers.FACTORY_BYTE_DIRECT;
import io.eguan.utils.ByteBuffers;
import io.eguan.utils.ByteBuffers.ByteBufferFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import junit.framework.AssertionFailedError;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;

/**
 * Test IBS put, get and replace.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
@RunWith(value = Parameterized.class)
public final class TestIbsPutGetL extends TestIbsTest {

    // Test keys and datas
    private static final byte[] KEY1 = "My Tailor is rich".getBytes();
    private static final byte[] KEY2 = "My Tailor is not so rich".getBytes();
    private static final byte[] KEY3 = "My Baker is rich too".getBytes();
    private static final byte[] DATA1 = "It's smaller than the garden of my uncle, but larger than the pen of my aunt."
            .getBytes();
    private static final byte[] DATA2 = "The spirit is willing, but the flesh is weak.".getBytes();
    private static final byte[] DATA3 = "The vodka is good, but the meat is rotten.".getBytes();

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB, "front" }, { IbsType.LEVELDB, "back" },
                { IbsType.LEVELDB, "no" }, { IbsType.FS, "no" }, { IbsType.FAKE, "no" } };
        return Arrays.asList(data);
    }

    public TestIbsPutGetL(final IbsType ibsType, final String compression) {
        super(ibsType, compression);
    }

    /**
     * IBS must be started for these tests.
     */
    @Before
    public void startIbs() {
        ibs.start();
    }

    /**
     * Stops the IBS.
     */
    @After
    public void stopIbs() {
        ibs.stop();
    }

    /**
     * Put and get data stored in a byte array.
     * 
     * @throws IbsIOException
     * @throws IbsException
     */
    @Test
    public void putGetArray() throws IbsException, IbsIOException {
        putOrReplaceGet(true, false, FACTORY_BYTE_ARRAY);
    }

    /**
     * Put and get data stored in a direct ByteBuffer.
     * 
     * @throws IbsIOException
     * @throws IbsException
     */
    @Test
    public void putGetDirect() throws IbsException, IbsIOException {
        putOrReplaceGet(true, true, FACTORY_BYTE_DIRECT);
    }

    /**
     * Replace and get data stored in a byte array.
     * 
     * @throws IbsIOException
     * @throws IbsException
     */
    @Test
    public void replaceGetArray() throws IbsException, IbsIOException {
        putOrReplaceGet(false, false, FACTORY_BYTE_ARRAY);
    }

    /**
     * Replace and get data stored in a direct ByteBuffer.
     * 
     * @throws IbsIOException
     * @throws IbsException
     */
    @Test
    public void replaceGetDirect() throws IbsException, IbsIOException {
        putOrReplaceGet(false, true, FACTORY_BYTE_DIRECT);
    }

    private void putOrReplaceGet(final boolean put, final boolean allocateDirect,
            final ByteBufferFactory byteBufferFactory) throws IbsException, IbsIOException {
        // Put some data
        final byte[] key = KEY1;
        final ByteBuffer dataPut = byteBufferFactory.newByteBuffer(DATA1);
        if (put) {
            Assert.assertTrue(ibs.put(key, dataPut));
        }
        else {
            final byte[] keyOld = KEY2;
            Assert.assertTrue(ibs.replace(keyOld, key, dataPut));
        }
        final int dataLength = dataPut.position();
        Assert.assertEquals(77, dataLength);

        // Get data - call get(byte[], int, boolean)
        // Larger buffer
        {
            final ByteBuffer dataGet = ibs.get(key, 256, allocateDirect);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet);
        }
        // Get data - call get(byte[], int, boolean)
        // Buffer of the exact size
        {
            final ByteBuffer dataGet = ibs.get(key, dataLength, allocateDirect);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet);
        }

        // Get data - call get(byte[], ByteBuffer)
        // Larger buffer
        {
            final ByteBuffer dataGet = byteBufferFactory.newByteBuffer(256);
            ibs.get(key, dataGet);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet);
        }
        // Get data - call get(byte[], ByteBuffer)
        // Buffer of the exact size
        {
            final ByteBuffer dataGet = byteBufferFactory.newByteBuffer(dataLength);
            ibs.get(key, dataGet);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet);
        }

        // Get data - call get(byte[], ByteBuffer, int, int)
        // Larger buffer
        {
            final int offset = 33;
            final int length = 100;
            final ByteBuffer dataGet = byteBufferFactory.newByteBuffer(256);
            final int readLen = ibs.get(key, dataGet, offset, length);
            Assert.assertEquals(dataLength, readLen);
            // position not set
            Assert.assertEquals(0, dataGet.position());
            // Set position for the comparison
            dataGet.position(offset + readLen);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet, offset);
        }
        // Get data - call get(byte[], ByteBuffer, int, int)
        // Buffer of the exact size
        {
            final int offset = 55;
            final int length = dataLength;
            final ByteBuffer dataGet = byteBufferFactory.newByteBuffer(offset + length);
            final int readLen = ibs.get(key, dataGet, offset, length);
            Assert.assertEquals(dataLength, readLen);
            // position not set
            Assert.assertEquals(0, dataGet.position());
            // Set position for the comparison
            dataGet.position(offset + readLen);

            // Compare contents
            ByteBuffers.assertEqualsByteBuffers(dataPut, dataGet, offset);
        }
    }

    /**
     * Simple put / replace / get test.
     * 
     * @throws IbsException
     * @throws IbsIOException
     * @throws InterruptedException
     */
    @Test
    public void putReplaceGetArray() throws IbsException, IbsIOException, InterruptedException {
        putReplaceGet(FACTORY_BYTE_ARRAY);
    }

    /**
     * Simple put / replace / get test.
     * 
     * @throws IbsException
     * @throws IbsIOException
     * @throws InterruptedException
     */
    @Test
    public void putReplaceGetDirect() throws IbsException, IbsIOException, InterruptedException {
        putReplaceGet(FACTORY_BYTE_DIRECT);
    }

    private void putReplaceGet(final ByteBufferFactory byteBufferFactory) throws IbsException, IbsIOException,
            InterruptedException {
        // put some data
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        Assert.assertTrue(ibs.put(KEY1, data1));

        // replace data
        final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
        Assert.assertTrue(ibs.replace(KEY1, KEY2, data2));

        if (ibs.isHotDataEnabled()) {
            // Check that KEY1/data1 was discarded
            final ByteBuffer readK1 = byteBufferFactory.newByteBuffer(DATA1.length);
            try {
                ibs.get(KEY1, readK1);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsIOException e) {
                Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
            }
        }
        else {
            // Replace did not replaced
            final ByteBuffer result = byteBufferFactory.newByteBuffer(256);
            ibs.get(KEY1, result);
            ByteBuffers.assertEqualsByteBuffers(data1, result);
        }

        // get data and compare
        final ByteBuffer result = byteBufferFactory.newByteBuffer(256);
        ibs.get(KEY2, result);
        ByteBuffers.assertEqualsByteBuffers(data2, result);
    }

    /**
     * Get key never added.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void getNotPutArray() throws IbsException, IbsIOException {
        getNotPut(FACTORY_BYTE_ARRAY);
    }

    /**
     * Get key never added.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void getNotPutDirect() throws IbsException, IbsIOException {
        getNotPut(FACTORY_BYTE_DIRECT);
    }

    private void getNotPut(final ByteBufferFactory byteBufferFactory) throws IbsException, IbsIOException {
        // put some data
        final ByteBuffer data = byteBufferFactory.newByteBuffer(DATA2);
        Assert.assertTrue(ibs.put(KEY1, data));

        // get data from another key
        final ByteBuffer result = byteBufferFactory.newByteBuffer(256);
        try {
            ibs.get(KEY2, result);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
        }
    }

    /**
     * Get buffer too small.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void getTooSmallArray() throws IbsException, IbsIOException {
        getTooSmall(false, FACTORY_BYTE_ARRAY);
    }

    /**
     * Get buffer too small.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void getTooSmallDirect() throws IbsException, IbsIOException {
        getTooSmall(true, FACTORY_BYTE_DIRECT);
    }

    private void getTooSmall(final boolean allocateDirect, final ByteBufferFactory byteBufferFactory)
            throws IbsException, IbsIOException {
        // put some data
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        final int len1 = DATA1.length / 2;
        Assert.assertTrue(ibs.put(KEY1, data1));

        // get data, with a buffer too small
        try {
            ibs.get(KEY1, len1, allocateDirect);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsBufferTooSmallException e) {
            Assert.assertEquals(IbsErrorCode.BUFFER_TOO_SMALL, e.getErrorCode());
            final int newLen = e.getRecordLength();
            final ByteBuffer read = ibs.get(KEY1, newLen, allocateDirect);
            ByteBuffers.assertEqualsByteBuffers(data1, read);
        }
    }

    /**
     * Put a buffer with 2 times with different values, then replace it.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putTwiceReplaceArray() throws IbsException, IbsIOException {
        putTwiceReplace(FACTORY_BYTE_ARRAY);
    }

    /**
     * Put a buffer with 2 times with different values, then replace it.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putTwiceReplaceDirect() throws IbsException, IbsIOException {
        putTwiceReplace(FACTORY_BYTE_DIRECT);
    }

    private void putTwiceReplace(final ByteBufferFactory byteBufferFactory) throws IbsException, IbsIOException {
        // put K1 / D1 for 2 IDs
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
        final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
        Assert.assertTrue(ibs.put(KEY1, data1));
        Assert.assertFalse(ibs.put(KEY1, data2));
        Assert.assertTrue(ibs.replace(KEY1, KEY2, data3));

        // get KEY1/data2 and KEY2/data2
        final ByteBuffer readK1 = byteBufferFactory.newByteBuffer(DATA1.length);
        ibs.get(KEY1, readK1);
        ByteBuffers.assertEqualsByteBuffers(data1, readK1);

        final ByteBuffer readK2 = byteBufferFactory.newByteBuffer(DATA3.length);
        ibs.get(KEY2, readK2);
        ByteBuffers.assertEqualsByteBuffers(data3, readK2);
    }

    /**
     * Put a buffer with 2 times with different values, then replace it. Stop and start the IBS before the read.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putTwiceReplaceStopStartArray() throws IbsException, IbsIOException {
        putTwiceReplaceStopStart(FACTORY_BYTE_ARRAY);
    }

    /**
     * Put a buffer with 2 times with different values, then replace it. Stop and start the IBS before the read.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putTwiceReplaceStopStartDirect() throws IbsException, IbsIOException {
        putTwiceReplaceStopStart(FACTORY_BYTE_DIRECT);
    }

    private void putTwiceReplaceStopStart(final ByteBufferFactory byteBufferFactory) throws IbsException,
            IbsIOException {
        // put K1 / D1 for 2 IDs
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
        final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
        Assert.assertTrue(ibs.put(KEY1, data1));
        Assert.assertFalse(ibs.put(KEY1, data2));
        Assert.assertTrue(ibs.replace(KEY1, KEY2, data3));

        // Stop then restart the IBS
        ibs.stop();
        ibs.start();

        // get KEY1/data1 and KEY2/data3
        final ByteBuffer readK1 = byteBufferFactory.newByteBuffer(DATA1.length);
        ibs.get(KEY1, readK1);
        ByteBuffers.assertEqualsByteBuffers(data1, readK1);

        final ByteBuffer readK2 = byteBufferFactory.newByteBuffer(DATA3.length);
        ibs.get(KEY2, readK2);
        ByteBuffers.assertEqualsByteBuffers(data3, readK2);
    }

    /**
     * Put one buffer, replace another and get. Stop and start the IBS before the read.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putReplaceStopStartArray() throws IbsException, IbsIOException {
        putReplaceStopStart(FACTORY_BYTE_ARRAY);
    }

    /**
     * Put one buffer, replace another and get. Stop and start the IBS before the read.
     * 
     * @throws IbsException
     * @throws IbsIOException
     */
    @Test
    public void putReplaceStopStartDirect() throws IbsException, IbsIOException {
        putReplaceStopStart(FACTORY_BYTE_DIRECT);
    }

    private void putReplaceStopStart(final ByteBufferFactory byteBufferFactory) throws IbsException, IbsIOException {
        // put K1 / D1 for 2 IDs
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
        Assert.assertTrue(ibs.put(KEY1, data1));
        Assert.assertTrue(ibs.replace(KEY1, KEY2, data2));

        // Stop then restart the IBS
        ibs.stop();
        ibs.start();

        if (ibs.isHotDataEnabled()) {
            // Check that KEY1/data1 was discarded
            final ByteBuffer readK1 = byteBufferFactory.newByteBuffer(DATA1.length);
            try {
                ibs.get(KEY1, readK1);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsIOException e) {
                Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
            }
        }
        else {
            // Replace did not replaced
            final ByteBuffer readK1 = byteBufferFactory.newByteBuffer(DATA1.length);
            ibs.get(KEY1, readK1);
            ByteBuffers.assertEqualsByteBuffers(data1, readK1);
        }

        // Get KEY2/data2
        final ByteBuffer readK2 = byteBufferFactory.newByteBuffer(DATA2.length);
        ibs.get(KEY2, readK2);
        ByteBuffers.assertEqualsByteBuffers(data2, readK2);
    }

    @Test
    public void putGetByteString() throws IbsException, IbsIOException, NullPointerException {
        // Put some data
        final byte[] key = KEY1;
        final ByteString dataPut = ByteString.copyFrom(DATA1);
        Assert.assertTrue(ibs.put(key, dataPut));
        final ByteString dataPut2 = ByteString.copyFrom(DATA2);
        Assert.assertFalse(ibs.put(key, dataPut2));

        // Get data - call get(byte[], int, boolean)
        // Larger buffer
        {
            final ByteBuffer dataGet = ibs.get(key, 256, true);

            // Compare contents. Set position to buffer length
            final ByteBuffer dataPutBuf = dataPut.asReadOnlyByteBuffer();
            dataPutBuf.position(dataPut.size());
            ByteBuffers.assertEqualsByteBuffers(dataPutBuf, dataGet);
        }
    }

    /**
     * Delete record.
     * 
     * @throws NullPointerException
     * @throws IbsIOException
     * @throws IbsException
     */
    @Test
    public void delSimpleDirect() throws IbsException, IbsIOException, NullPointerException {
        final ByteBuffer data1 = FACTORY_BYTE_DIRECT.newByteBuffer(DATA1);
        Assert.assertTrue(ibs.put(KEY1, data1));
        final ByteBuffer data2 = FACTORY_BYTE_DIRECT.newByteBuffer(DATA2);
        Assert.assertTrue(ibs.put(KEY2, data2));

        // Get data
        final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
        ibs.get(KEY1, readK1);
        ByteBuffers.assertEqualsByteBuffers(data1, readK1);

        // Remove record and try get again
        ibs.del(KEY1);
        try {
            readK1.position(0);
            ibs.get(KEY1, readK1);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
        }
    }

    @Test
    public void putTxArray() throws IbsException, IbsIOException, NullPointerException {
        putTx(FACTORY_BYTE_ARRAY);
    }

    @Test
    public void putTxDirect() throws IbsException, IbsIOException, NullPointerException {
        putTx(FACTORY_BYTE_DIRECT);
    }

    private void putTx(final ByteBufferFactory byteBufferFactory) throws IbsException, IllegalArgumentException,
            IbsIOException {
        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        {
            boolean commit = false;
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertTrue(ibs.put(txId, KEY1, data1, 0, data1.capacity()));

                // Get data: should not be found
                final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
                try {
                    ibs.get(KEY1, readK1);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsIOException e) {
                    Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                }

                ibs.commit(txId);
                commit = true;

                // Should be found
                readK1.rewind();
                ibs.get(KEY1, readK1);
                data1.position(data1.capacity());
                ByteBuffers.assertEqualsByteBuffers(data1, readK1);
            }
            finally {
                if (!commit) {
                    ibs.rollback(txId);
                }
            }
        }

        // Check KEY1 already known on put()
        {
            final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
            Assert.assertFalse(ibs.put(KEY1, data2, 0, data2.capacity()));

            // Should be found
            final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
            ibs.get(KEY1, readK1);
            ByteBuffers.assertEqualsByteBuffers(data1, readK1);
        }
        // Check KEY1 already known on replace()
        {
            final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
            Assert.assertFalse(ibs.replace(KEY3, KEY1, data2, 0, data2.capacity()));

            // Should be found
            final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
            ibs.get(KEY1, readK1);
            ByteBuffers.assertEqualsByteBuffers(data1, readK1);
        }
        // Check KEY1 already known on putTx()
        {
            final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertFalse(ibs.put(txId, KEY1, data2, 0, data2.capacity()));
            }
            finally {
                ibs.rollback(txId);
            }

            // Should be found
            final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
            ibs.get(KEY1, readK1);
            ByteBuffers.assertEqualsByteBuffers(data1, readK1);
        }
        // Check KEY1 already known on replaceTx()
        {
            final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertFalse(ibs.replace(txId, KEY3, KEY1, data2, 0, data2.capacity()));
            }
            finally {
                ibs.rollback(txId);
            }

            // Should be found
            final ByteBuffer readK1 = FACTORY_BYTE_DIRECT.newByteBuffer(256);
            ibs.get(KEY1, readK1);
            ByteBuffers.assertEqualsByteBuffers(data1, readK1);
        }
    }

    @Test
    public void replaceTxArray() throws IbsException, IbsIOException, NullPointerException {
        replaceTx(FACTORY_BYTE_ARRAY);
    }

    @Test
    public void replaceTxDirect() throws IbsException, IbsIOException, NullPointerException {
        replaceTx(FACTORY_BYTE_DIRECT);
    }

    private void replaceTx(final ByteBufferFactory byteBufferFactory) throws IbsException, IllegalArgumentException,
            IbsIOException {

        final ByteBuffer data1 = byteBufferFactory.newByteBuffer(DATA1);
        final ByteBuffer data2 = byteBufferFactory.newByteBuffer(DATA2);

        // put some data
        Assert.assertTrue(ibs.put(KEY1, data1));

        // replace data in a transaction
        boolean commit = false;
        {
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertTrue(ibs.replace(txId, KEY1, KEY2, data2, 0, data2.capacity()));

                // Set data2 position to the end to compare contents with get requests
                data2.position(data2.capacity());

                // KEY1 data should be found, but not the KEY2 data

                // get data and compare
                {
                    final ByteBuffer result = byteBufferFactory.newByteBuffer(256);
                    ibs.get(KEY1, result);
                    ByteBuffers.assertEqualsByteBuffers(data1, result);

                    result.rewind();
                    try {
                        ibs.get(KEY2, result);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                    }
                }

                ibs.commit(txId);
                commit = true;

                // KEY2 data should be found, but not the KEY1 data

                // get data and compare
                checkKey1Replaced(data1, data2);

            }
            finally {
                if (!commit) {
                    ibs.rollback(txId);
                }
            }
        }

        // Check KEY1 already known on put()
        {
            final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
            Assert.assertFalse(ibs.put(KEY2, data3, 0, data3.capacity()));

            checkKey1Replaced(data1, data2);
        }
        // Check KEY2 already known on replace()
        {
            final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
            Assert.assertFalse(ibs.replace(KEY3, KEY2, data3, 0, data3.capacity()));

            checkKey1Replaced(data1, data2);
        }
        // Check KEY2 already known on putTx()
        {
            final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertFalse(ibs.put(txId, KEY2, data3, 0, data3.capacity()));
            }
            finally {
                ibs.rollback(txId);
            }

            checkKey1Replaced(data1, data2);
        }
        // Check KEY2 already known on replaceTx()
        {
            final ByteBuffer data3 = byteBufferFactory.newByteBuffer(DATA3);
            final int txId = ibs.createTransaction();
            try {
                Assert.assertTrue(txId > 0);
                Assert.assertFalse(ibs.replace(txId, KEY3, KEY2, data3, 0, data3.capacity()));
            }
            finally {
                ibs.rollback(txId);
            }

            checkKey1Replaced(data1, data2);
        }
    }

    private final void checkKey1Replaced(final ByteBuffer data1, final ByteBuffer data2) throws IbsIOException,
            IbsException {
        // Should not find KEY1 and find KEY2
        final ByteBuffer result = FACTORY_BYTE_DIRECT.newByteBuffer(256);
        if (ibs.isHotDataEnabled()) {
            try {
                ibs.get(KEY1, result);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsIOException e) {
                Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
            }
        }
        else {
            // Replace did not replaced
            ibs.get(KEY1, result);
            ByteBuffers.assertEqualsByteBuffers(data1, result);
            result.rewind();
        }
        ibs.get(KEY2, result);
        Assert.assertEquals(data2.position(), data2.capacity());
        ByteBuffers.assertEqualsByteBuffers(data2, result);
    }
}
