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

import static io.eguan.ibs.IbsTestDefinitions.TEMP_PREFIX;
import static io.eguan.ibs.IbsTestDefinitions.TEMP_SUFFIX;
import io.eguan.ibs.TestIbs.IbsInitHelper;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

public class TestIbsFilesDB {

    private static final int TEST_BUF_LEN = 56;

    @Test
    public void testIbsFilesDBBasic() throws IOException {
        final File ibsDir = Files.createTempDirectory(TEMP_PREFIX).toFile();
        try {
            Ibs ibs = IbsFactory.createIbs(ibsDir, IbsType.FS);
            try {
                ibs.close();
                Assert.assertTrue(ibs.isClosed());
                Assert.assertFalse(ibs.isStarted());

                // Open IBS and put/get key-values
                ibs = IbsFactory.openIbs(ibsDir, IbsType.FS);
                ibs.start();
                try {

                    Assert.assertFalse(ibs.isHotDataEnabled());
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

                    { // Read key1: replace() does not replace
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

                    // Restart then read again
                    ibs.stop();
                    ibs.start();

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
        finally {
            io.eguan.utils.Files.deleteRecursive(ibsDir.toPath());
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenNotExist() {
        try {
            final File ibsDir = new File("/tmp/notexists");
            Assert.assertFalse(ibsDir.exists());
            IbsFactory.openIbs(ibsDir, IbsType.FS);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.INVALID_IBS_ID, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenFile() throws IOException {
        final File ibsDir = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            IbsFactory.openIbs(ibsDir, IbsType.FS);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.INVALID_IBS_ID, e.getErrorCode());
            throw e;
        }
        finally {
            Assert.assertTrue(ibsDir.delete());
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenEmptyDir() throws IOException {
        final File ibsDir = Files.createTempDirectory(TEMP_PREFIX).toFile();
        try {
            IbsFactory.openIbs(ibsDir, IbsType.FS);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.INIT_FROM_EMPTY_DIR, e.getErrorCode());
            throw e;
        }
        finally {
            io.eguan.utils.Files.deleteRecursive(ibsDir.toPath());
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateExist() throws IOException {
        final File ibsDir = Files.createTempDirectory(TEMP_PREFIX).toFile();
        try {
            Ibs ibs = null;
            try {
                ibs = IbsFactory.createIbs(ibsDir, IbsType.FS);
                IbsFactory.createIbs(ibsDir, IbsType.FS);
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
        finally {
            io.eguan.utils.Files.deleteRecursive(ibsDir.toPath());
        }
    }

    @Test
    public void testLevelDBConfig() throws Exception {
        final IbsInitHelper ibsInitHelper = new IbsInitHelper();
        ibsInitHelper.initIbs(IbsType.LEVELDB, null, false);
        try {
            final File tempFileConfig = ibsInitHelper.getTempFileConfig();
            Ibs ibs = IbsFactory.createIbs(tempFileConfig, IbsType.FS);
            ibs.close();

            // Check that a file have been created in the ibp directory
            final File tempDirIbp = ibsInitHelper.getTempDirIbp().toFile();
            Assert.assertTrue(new File(tempDirIbp, "created").isFile());

            ibs = IbsFactory.openIbs(tempFileConfig, IbsType.FS);
            ibs.close();
        }
        finally {
            ibsInitHelper.finiIbs();
        }
    }

    @Test(expected = IbsException.class)
    public void testLevelDBConfigIbpDeleted() throws Exception {
        final IbsInitHelper ibsInitHelper = new IbsInitHelper();
        ibsInitHelper.initIbs(IbsType.LEVELDB, null, false);
        try {
            final File tempFileConfig = ibsInitHelper.getTempFileConfig();
            final File tempDirIbp = ibsInitHelper.getTempDirIbp().toFile();
            Assert.assertTrue(tempDirIbp.delete());

            try {
                final Ibs ibs = IbsFactory.createIbs(tempFileConfig, IbsType.FS);
                ibs.close();
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.INVALID_IBS_ID, e.getErrorCode());
                throw e;
            }
        }
        finally {
            ibsInitHelper.finiIbs();
        }
    }

}
