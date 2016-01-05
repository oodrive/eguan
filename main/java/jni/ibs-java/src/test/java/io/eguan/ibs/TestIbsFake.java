package io.eguan.ibs;

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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

/**
 * Simple unit tests for {@link IbsFake}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestIbsFake {

    private static final int TEST_BUF_LEN = 56;

    @Test
    public void testIbsFakeBasic() throws IbsIOException {
        final File ibsFile = new File("fake");
        Ibs ibs = IbsFactory.createIbs(ibsFile, IbsType.FAKE);
        try {
            ibs.close();
            Assert.assertTrue(ibs.isClosed());
            Assert.assertFalse(ibs.isStarted());

            // Open IBS and put/get key-values
            ibs = IbsFactory.openIbs(ibsFile, IbsType.FAKE);
            ibs.start();
            try {

                Assert.assertTrue(ibs.isHotDataEnabled());
                Assert.assertTrue(ibs.isStarted());

                final Random random = new Random();
                final byte[] key1 = new byte[TEST_BUF_LEN];
                final byte[] key2 = new byte[TEST_BUF_LEN];
                final byte[] key3 = new byte[TEST_BUF_LEN];
                random.nextBytes(key1);
                random.nextBytes(key2);
                random.nextBytes(key3);
                final ByteBuffer bytebuf1 = ByteBuffer.wrap(key1);
                final ByteBuffer bytebuf2 = ByteBuffer.wrap(key2);
                final ByteBuffer bytebuf3 = ByteBuffer.wrap(key3);

                Assert.assertTrue(ibs.put(key1, bytebuf1));
                Assert.assertTrue(ibs.put(key2, bytebuf2));
                Assert.assertTrue(ibs.put(key3, bytebuf3));

                { // Read key1
                    final ByteBuffer readBuf = ibs.get(key1, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf1.rewind();
                    Assert.assertEquals(bytebuf1, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }
                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }
                { // Read key3
                    final ByteBuffer readBuf = ibs.get(key3, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf3.rewind();
                    Assert.assertEquals(bytebuf3, readBuf);
                    Assert.assertFalse(bytebuf2.equals(readBuf));
                }

                // Replace key1
                Assert.assertFalse(ibs.replace(key1, key3, bytebuf2));

                { // Read key1: replace() replaces
                    try {
                        ibs.get(key1, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                    }
                }
                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }
                { // Read key3
                    final ByteBuffer readBuf = ibs.get(key3, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf3.rewind();
                    Assert.assertFalse(bytebuf2.equals(readBuf));
                    Assert.assertEquals(bytebuf3, readBuf);
                }

                // Remove key1
                ibs.del(key1);

                { // Read key1
                    try {
                        ibs.get(key1, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                    }
                }
                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }
                { // Read key3
                    final ByteBuffer readBuf = ibs.get(key3, TEST_BUF_LEN, false);
                    readBuf.rewind();
                    bytebuf3.rewind();
                    Assert.assertFalse(bytebuf2.equals(readBuf));
                    Assert.assertEquals(bytebuf3, readBuf);
                }

            }
            finally {
                ibs.close();
            }
        }
        finally {
            ibs.destroy();
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenNotExist() {
        try {
            final File ibsFile = new File("fake");
            IbsFactory.openIbs(ibsFile, IbsType.FAKE);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.INIT_FROM_EMPTY_DIR, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateExist() throws IbsIOException {
        Ibs ibs = null;
        try {
            final File ibsFile = new File("fake");
            ibs = IbsFactory.createIbs(ibsFile, IbsType.FAKE);
            IbsFactory.createIbs(ibsFile, IbsType.FAKE);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.CREATE_IN_NON_EMPTY_DIR, e.getErrorCode());
            throw e;
        }
        finally {
            if (ibs != null) {
                ibs.destroy();
            }
        }
    }

    /**
     * Fail every 3 put and 2 get.
     * 
     * @throws IbsIOException
     */
    @Test
    public void testIbsFakeErrors3put2get() throws IbsIOException {
        final File ibsFile = new File(Ibs.UNIT_TEST_IBS_HEADER + "3:2");
        Ibs ibs = IbsFactory.createIbs(ibsFile);
        try {
            ibs.close();
            Assert.assertTrue(ibs instanceof IbsFake);
            Assert.assertTrue(ibs.isClosed());
            Assert.assertFalse(ibs.isStarted());

            // Open IBS and put/get key-values
            ibs = IbsFactory.openIbs(ibsFile);
            ibs.start();
            try {

                Assert.assertTrue(ibs.isHotDataEnabled());
                Assert.assertTrue(ibs.isStarted());

                final Random random = new Random();
                final byte[] key1 = new byte[TEST_BUF_LEN];
                final byte[] key2 = new byte[TEST_BUF_LEN];
                final byte[] key3 = new byte[TEST_BUF_LEN];
                random.nextBytes(key1);
                random.nextBytes(key2);
                random.nextBytes(key3);
                final ByteBuffer bytebuf1 = ByteBuffer.wrap(key1);
                final ByteBuffer bytebuf2 = ByteBuffer.wrap(key2);
                final ByteBuffer bytebuf3 = ByteBuffer.wrap(key3);

                // 3 put
                Assert.assertTrue(ibs.put(key1, bytebuf1));
                Assert.assertTrue(ibs.put(key2, bytebuf2));
                try {
                    ibs.put(key3, bytebuf3);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsIOException e) {
                    // OK
                }

                // 2 get

                { // Read key3: not written
                    try {
                        ibs.get(key3, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                    }
                }

                { // Read key2: should fail
                    try {
                        ibs.get(key2, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.UNKNOW_ERROR, e.getErrorCode());
                    }
                }

                // put/get/replace mix
                Assert.assertTrue(ibs.put(key3, bytebuf3));
                bytebuf3.rewind();
                Assert.assertFalse(ibs.put(key2, bytebuf3));
                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    bytebuf3.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

                try {
                    bytebuf3.rewind();
                    ibs.replace(key3, key1, bytebuf3);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsIOException e) {
                    // OK
                }

                { // Read key1: second get, fails
                    try {
                        ibs.get(key1, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.UNKNOW_ERROR, e.getErrorCode());
                    }
                }

                { // Read key1: get again
                    final ByteBuffer readBuf = ibs.get(key1, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf1.rewind();
                    Assert.assertEquals(bytebuf1, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }
            }
            finally {
                ibs.close();
            }
        }
        finally {
            ibs.destroy();
        }
    }

    /**
     * Fail every 3 put and never fail on get.
     * 
     * @throws IbsIOException
     */
    @Test
    public void testIbsFakeErrors3put() throws IbsIOException {
        final File ibsFile = new File(Ibs.UNIT_TEST_IBS_HEADER + "3");
        Ibs ibs = IbsFactory.createIbs(ibsFile);
        try {
            ibs.close();
            Assert.assertTrue(ibs instanceof IbsFake);
            Assert.assertTrue(ibs.isClosed());
            Assert.assertFalse(ibs.isStarted());

            // Open IBS and put/get key-values
            ibs = IbsFactory.openIbs(ibsFile);
            ibs.start();
            try {

                Assert.assertTrue(ibs.isHotDataEnabled());
                Assert.assertTrue(ibs.isStarted());

                final Random random = new Random();
                final byte[] key1 = new byte[TEST_BUF_LEN];
                final byte[] key2 = new byte[TEST_BUF_LEN];
                final byte[] key3 = new byte[TEST_BUF_LEN];
                random.nextBytes(key1);
                random.nextBytes(key2);
                random.nextBytes(key3);
                final ByteBuffer bytebuf1 = ByteBuffer.wrap(key1);
                final ByteBuffer bytebuf2 = ByteBuffer.wrap(key2);
                final ByteBuffer bytebuf3 = ByteBuffer.wrap(key3);

                // 3 put
                Assert.assertTrue(ibs.put(key1, bytebuf1));
                Assert.assertTrue(ibs.put(key2, bytebuf2));
                try {
                    ibs.put(key3, bytebuf3);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsIOException e) {
                    // OK
                }

                // 4 get

                { // Read key1
                    final ByteBuffer readBuf = ibs.get(key1, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf1.rewind();
                    Assert.assertEquals(bytebuf1, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

                { // Read key3: not written
                    try {
                        ibs.get(key3, TEST_BUF_LEN, true);
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsIOException e) {
                        Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
                    }
                }

                { // Read key2 again
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

                // put/get/replace mix
                Assert.assertTrue(ibs.put(key3, bytebuf3));
                bytebuf3.rewind();
                Assert.assertFalse(ibs.put(key2, bytebuf3));
                { // Read key2
                    final ByteBuffer readBuf = ibs.get(key2, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf2.rewind();
                    bytebuf3.rewind();
                    Assert.assertEquals(bytebuf2, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

                // replace fails
                try {
                    bytebuf3.rewind();
                    ibs.replace(key3, key1, bytebuf3);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsIOException e) {
                    // OK
                }

                { // Read key1: not replaced
                    final ByteBuffer readBuf = ibs.get(key1, TEST_BUF_LEN, true);
                    readBuf.rewind();
                    bytebuf1.rewind();
                    Assert.assertEquals(bytebuf1, readBuf);
                    Assert.assertFalse(bytebuf3.equals(readBuf));
                }

            }
            finally {
                ibs.close();
            }
        }
        finally {
            ibs.destroy();
        }
    }

}
