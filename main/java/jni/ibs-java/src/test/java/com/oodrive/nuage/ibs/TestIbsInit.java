package com.oodrive.nuage.ibs;

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

import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_HOTDATA;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_HOTDATA_OFF;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_IBP;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_IBPGEN;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_OWNER;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_OWNER_ALT;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_UUID;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.CONF_UUID_ALT;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.TEMP_PREFIX;
import static com.oodrive.nuage.ibs.IbsTestDefinitions.TEMP_SUFFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.utils.ByteArrays;

/**
 * Test IBS initialization (success and failures).
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
public final class TestIbsInit {
    private static final int TST_KEY_LEN = 32;
    private static final int TST_VALUE_LEN = 4096;

    private static final int IDX_CONFIG_FILE = 0;
    private static final int IDX_IBPGEN_DIR = 1;
    private static final int IDX_IBP1_DIR = 2;
    private static final int IDX_IBP2_DIR = 3;

    // IBS internals
    private static final String IBP_CONFIG_FILE = "ibpid.conf";
    private static final String IBPGEN_CONFIG_FILE = "ibs.conf";

    private static final String IBP_CONFIG_OWNER_KEY = "owner";
    private static final String IBP_CONFIG_UUID_KEY = "uuid";

    enum HotDataDef {
        DEFAULT, ON, OFF;
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigNotExist() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempConfig.delete());
        try {
            IbsFactory.createIbs(tempConfig);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsException e) {
            Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigEmpty() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            try {
                IbsFactory.createIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigDirsEquals() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir = Files.createTempDirectory(TEMP_PREFIX);
            try {
                // Write config: same directory twice
                try (PrintStream config = new PrintStream(tempConfig)) {
                    final String dir = tempDir.toAbsolutePath().toString();
                    config.println(CONF_IBPGEN + dir);
                    config.println(CONF_IBP + dir);
                    config.println(CONF_UUID);
                    config.println(CONF_OWNER);
                }

                try {
                    IbsFactory.createIbs(tempConfig);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsException e) {
                    Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                    throw e;
                }
            }
            finally {
                Files.delete(tempDir);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigDirIbpgenNotExists() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            Assert.assertTrue(Files.deleteIfExists(tempDir1));
            final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
            try {

                // Write config: directory
                try (PrintStream config = new PrintStream(tempConfig)) {
                    config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                    config.println(CONF_IBP + tempDir2.toAbsolutePath());
                    config.println(CONF_UUID);
                    config.println(CONF_OWNER);
                }

                try {
                    IbsFactory.createIbs(tempConfig);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsException e) {
                    Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                    throw e;
                }
            }
            finally {
                Files.delete(tempDir2);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigDirIbpNotExists() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
            Assert.assertTrue(Files.deleteIfExists(tempDir2));
            try {

                // Write config: directory
                try (PrintStream config = new PrintStream(tempConfig)) {
                    config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                    config.println(CONF_IBP + tempDir2.toAbsolutePath());
                    config.println(CONF_UUID);
                    config.println(CONF_OWNER);
                }

                try {
                    IbsFactory.createIbs(tempConfig);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IbsException e) {
                    Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                    throw e;
                }
            }
            finally {
                Files.delete(tempDir1);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test
    public void testCreateConfigHotDataDefault() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            try {
                final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
                try {

                    // Write config: directory
                    try (PrintStream config = new PrintStream(tempConfig)) {
                        config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                        config.println(CONF_IBP + tempDir2.toAbsolutePath());
                        config.println(CONF_UUID);
                        config.println(CONF_OWNER);
                    }

                    final Ibs ibs = IbsFactory.createIbs(tempConfig);
                    try {
                        // Hot data enabled by default
                        Assert.assertTrue(ibs.isHotDataEnabled());
                    }
                    finally {
                        ibs.close();
                    }
                }
                finally {
                    com.oodrive.nuage.utils.Files.deleteRecursive(tempDir2);
                }
            }
            finally {
                com.oodrive.nuage.utils.Files.deleteRecursive(tempDir1);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test
    public void testCreateConfigHotDataOn() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            try {
                final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
                try {

                    // Write config: directory
                    try (PrintStream config = new PrintStream(tempConfig)) {
                        config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                        config.println(CONF_IBP + tempDir2.toAbsolutePath());
                        config.println(CONF_HOTDATA);
                        config.println(CONF_UUID);
                        config.println(CONF_OWNER);
                    }

                    final Ibs ibs = IbsFactory.createIbs(tempConfig);
                    try {
                        Assert.assertTrue(ibs.isHotDataEnabled());
                    }
                    finally {
                        ibs.close();
                    }
                }
                finally {
                    com.oodrive.nuage.utils.Files.deleteRecursive(tempDir2);
                }
            }
            finally {
                com.oodrive.nuage.utils.Files.deleteRecursive(tempDir1);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test
    public void testCreateConfigHotDataOff() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            try {
                final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
                try {

                    // Write config: directory
                    try (PrintStream config = new PrintStream(tempConfig)) {
                        config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                        config.println(CONF_IBP + tempDir2.toAbsolutePath());
                        config.println(CONF_HOTDATA_OFF);
                        config.println(CONF_UUID);
                        config.println(CONF_OWNER);
                    }

                    final Ibs ibs = IbsFactory.createIbs(tempConfig);
                    try {
                        Assert.assertFalse(ibs.isHotDataEnabled());
                    }
                    finally {
                        ibs.close();
                    }
                }
                finally {
                    com.oodrive.nuage.utils.Files.deleteRecursive(tempDir2);
                }
            }
            finally {
                com.oodrive.nuage.utils.Files.deleteRecursive(tempDir1);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test(expected = IbsException.class)
    public void testCreateConfigHotDataClosed() throws IOException {
        final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempDir1 = Files.createTempDirectory(TEMP_PREFIX);
            try {
                final Path tempDir2 = Files.createTempDirectory(TEMP_PREFIX);
                try {

                    // Write config: directory
                    try (PrintStream config = new PrintStream(tempConfig)) {
                        config.println(CONF_IBPGEN + tempDir1.toAbsolutePath());
                        config.println(CONF_IBP + tempDir2.toAbsolutePath());
                        config.println(CONF_HOTDATA_OFF);
                        config.println(CONF_UUID);
                        config.println(CONF_OWNER);
                    }

                    final Ibs ibs = IbsFactory.createIbs(tempConfig);
                    ibs.close();
                    try {
                        ibs.isHotDataEnabled();
                        throw new AssertionFailedError("Should not be reached");
                    }
                    catch (final IbsException e) {
                        Assert.assertEquals(IbsErrorCode.INVALID_IBS_ID, e.getErrorCode());
                        throw e;
                    }
                }
                finally {
                    com.oodrive.nuage.utils.Files.deleteRecursive(tempDir2);
                }
            }
            finally {
                com.oodrive.nuage.utils.Files.deleteRecursive(tempDir1);
            }
        }
        finally {
            tempConfig.delete();
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigNotExist() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            Assert.assertTrue(tempConfig.delete());
            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigEmpty() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            Assert.assertTrue(tempConfig.delete());
            Assert.assertTrue(tempConfig.createNewFile());
            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirsEquals() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempGenDir = files.get(IDX_IBPGEN_DIR);

            // Write config: same directory twice
            try (PrintStream config = new PrintStream(tempConfig)) {
                final String dir = tempGenDir.getAbsolutePath();
                config.println(CONF_IBPGEN + dir);
                config.println(CONF_IBP + dir);
                config.println(CONF_UUID);
                config.println(CONF_OWNER);
            }

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpgenNotExists() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempGenDir = files.get(IDX_IBPGEN_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempGenDir.toPath());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpgenEmpty() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempGenDir = files.get(IDX_IBPGEN_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempGenDir.toPath());
            Assert.assertTrue(tempGenDir.mkdir());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.INIT_FROM_EMPTY_DIR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigDirIbpgenMoved() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempGenDir = files.get(IDX_IBPGEN_DIR);

            // Rename gendir and re-write config file
            final Path tempGenDirNewPath = Files.createTempDirectory(TEMP_PREFIX);
            Files.delete(tempGenDirNewPath);
            Files.move(tempGenDir.toPath(), tempGenDirNewPath);
            files.set(IDX_IBPGEN_DIR, tempGenDirNewPath.toFile());
            writeIbsConfig(HotDataDef.DEFAULT, files);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            ibs.close();
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpgenChangedUuid() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Change UUID in IBPGEN config file
            final File ibpConf = new File(files.get(IDX_IBPGEN_DIR), IBPGEN_CONFIG_FILE);
            changeIbpConfigKey(ibpConf, IBP_CONFIG_UUID_KEY, UUID.randomUUID().toString());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpgenChangedOwner() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Change owner in IBPGEN config file
            final File ibpConf = new File(files.get(IDX_IBPGEN_DIR), IBPGEN_CONFIG_FILE);
            changeIbpConfigKey(ibpConf, IBP_CONFIG_OWNER_KEY, UUID.randomUUID().toString());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpNotExists1() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempPDir1 = files.get(IDX_IBP1_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempPDir1.toPath());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpEmpty1() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempPDir1 = files.get(IDX_IBP1_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempPDir1.toPath());
            Assert.assertTrue(tempPDir1.mkdir());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.INIT_FROM_EMPTY_DIR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpNotExists2() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempPDir2 = files.get(IDX_IBP2_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempPDir2.toPath());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpEmpty2() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            final File tempPDir2 = files.get(IDX_IBP2_DIR);
            com.oodrive.nuage.utils.Files.deleteRecursive(tempPDir2.toPath());
            Assert.assertTrue(tempPDir2.mkdir());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.INIT_FROM_EMPTY_DIR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedRemove1() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        File toDel = null;
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            toDel = files.remove(IDX_IBP1_DIR);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
            if (toDel != null) {
                com.oodrive.nuage.utils.Files.deleteRecursive(toDel.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedRemove2() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        File toDel = null;
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            toDel = files.remove(IDX_IBP2_DIR);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
            if (toDel != null) {
                com.oodrive.nuage.utils.Files.deleteRecursive(toDel.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedUuid() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Change owner in IBP config file
            final File ibpConf = new File(files.get(IDX_IBP2_DIR), IBP_CONFIG_FILE);
            changeIbpConfigKey(ibpConf, IBP_CONFIG_UUID_KEY, UUID.randomUUID().toString());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedOwner() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Change owner in IBP config file
            final File ibpConf = new File(files.get(IDX_IBP2_DIR), IBP_CONFIG_FILE);
            changeIbpConfigKey(ibpConf, IBP_CONFIG_OWNER_KEY, UUID.randomUUID().toString());

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigDirIbpChangedSwitchIbp() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Write some values
            final Map<byte[], byte[]> values = writeValues(tempConfig);

            // Switch ibp
            final File file = files.remove(IDX_IBP1_DIR);
            files.add(file);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                ibs.start();
                try {
                    for (final Iterator<Entry<byte[], byte[]>> entries = values.entrySet().iterator(); entries
                            .hasNext();) {
                        final Entry<byte[], byte[]> entry = entries.next();
                        final ByteBuffer valueBuf = ibs.get(entry.getKey(), TST_VALUE_LEN, false);
                        ByteArrays.assertEqualsByteArrays(entry.getValue(), valueBuf.array());
                    }
                }
                finally {
                    ibs.stop();
                }
            }
            finally {
                ibs.close();
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigDirIbpChangedMoveIbp1() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Write some values
            final Map<byte[], byte[]> values = writeValues(tempConfig);

            // Rename ibp1 and re-write config file
            final File tempPDir1 = files.get(IDX_IBP1_DIR);
            final Path tempPDir1NewPath = Files.createTempDirectory(TEMP_PREFIX);
            Files.delete(tempPDir1NewPath);
            Files.move(tempPDir1.toPath(), tempPDir1NewPath);
            files.set(IDX_IBP1_DIR, tempPDir1NewPath.toFile());
            writeIbsConfig(HotDataDef.DEFAULT, files);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                ibs.start();
                try {
                    for (final Iterator<Entry<byte[], byte[]>> entries = values.entrySet().iterator(); entries
                            .hasNext();) {
                        final Entry<byte[], byte[]> entry = entries.next();
                        final ByteBuffer valueBuf = ibs.get(entry.getKey(), TST_VALUE_LEN, false);
                        ByteArrays.assertEqualsByteArrays(entry.getValue(), valueBuf.array());
                    }
                }
                finally {
                    ibs.stop();
                }
            }
            finally {
                ibs.close();
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigDirIbpChangedMoveIbp2() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Write some values
            final Map<byte[], byte[]> values = writeValues(tempConfig);

            // Rename ibp2 and re-write config file
            final File tempPDir2 = files.get(IDX_IBP2_DIR);
            final Path tempPDir2NewPath = Files.createTempDirectory(TEMP_PREFIX);
            Files.delete(tempPDir2NewPath);
            Files.move(tempPDir2.toPath(), tempPDir2NewPath);
            files.set(IDX_IBP2_DIR, tempPDir2NewPath.toFile());
            writeIbsConfig(HotDataDef.DEFAULT, files);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                ibs.start();
                try {
                    for (final Iterator<Entry<byte[], byte[]>> entries = values.entrySet().iterator(); entries
                            .hasNext();) {
                        final Entry<byte[], byte[]> entry = entries.next();
                        final ByteBuffer valueBuf = ibs.get(entry.getKey(), TST_VALUE_LEN, false);
                        ByteArrays.assertEqualsByteArrays(entry.getValue(), valueBuf.array());
                    }
                }
                finally {
                    ibs.stop();
                }
            }
            finally {
                ibs.close();
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedIbp1Twice() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Add ibp2 again and re-write config file
            final File tempPDir1 = files.get(IDX_IBP1_DIR);
            files.add(tempPDir1);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedIbp2Twice() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Add ibp2 again and re-write config file
            final File tempPDir2 = files.get(IDX_IBP2_DIR);
            files.add(tempPDir2);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirIbpChangedSwitchIbpgen() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            // Switch ibpgen and ibp1
            final File file = files.remove(IDX_IBPGEN_DIR);
            files.add(file);
            writeIbsConfig(HotDataDef.DEFAULT, files);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirUuidChanged() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        File toDel = null;
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            toDel = files.remove(IDX_IBP1_DIR);
            writeIbsConfig(HotDataDef.DEFAULT, files, true, false);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
            if (toDel != null) {
                com.oodrive.nuage.utils.Files.deleteRecursive(toDel.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigDirOwnerChanged() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        File toDel = null;
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);
            toDel = files.remove(IDX_IBP1_DIR);
            writeIbsConfig(HotDataDef.DEFAULT, files, false, true);

            try {
                IbsFactory.openIbs(tempConfig);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.CONFIG_ERROR, e.getErrorCode());
                throw e;
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
            if (toDel != null) {
                com.oodrive.nuage.utils.Files.deleteRecursive(toDel.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigHotDataDefault() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.DEFAULT);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                // Hot data enabled by default
                Assert.assertTrue(ibs.isHotDataEnabled());
            }
            finally {
                ibs.close();
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigHotDataOn() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.ON);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                Assert.assertTrue(ibs.isHotDataEnabled());
            }
            finally {
                ibs.close();
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigHotDataOnOff() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.ON);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            {
                final Ibs ibs = IbsFactory.openIbs(tempConfig);
                try {
                    Assert.assertTrue(ibs.isHotDataEnabled());
                }
                finally {
                    ibs.close();
                }
            }

            // Switch to OFF
            writeIbsConfig(HotDataDef.OFF, files);

            {
                final Ibs ibs = IbsFactory.openIbs(tempConfig);
                try {
                    Assert.assertFalse(ibs.isHotDataEnabled());
                }
                finally {
                    ibs.close();
                }
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigHotDataOff() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.OFF);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            try {
                Assert.assertFalse(ibs.isHotDataEnabled());
            }
            finally {
                ibs.close();
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test
    public void testOpenConfigHotDataOffOn() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.OFF);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            {
                final Ibs ibs = IbsFactory.openIbs(tempConfig);
                try {
                    Assert.assertFalse(ibs.isHotDataEnabled());
                }
                finally {
                    ibs.close();
                }
            }

            // Switch to ON
            writeIbsConfig(HotDataDef.ON, files);

            {
                final Ibs ibs = IbsFactory.openIbs(tempConfig);
                try {
                    Assert.assertTrue(ibs.isHotDataEnabled());
                }
                finally {
                    ibs.close();
                }
            }

        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    @Test(expected = IbsException.class)
    public void testOpenConfigHotDataClosed() throws IOException {
        final ArrayList<File> files = createIbs(HotDataDef.ON);
        try {
            final File tempConfig = files.get(IDX_CONFIG_FILE);

            final Ibs ibs = IbsFactory.openIbs(tempConfig);
            ibs.close();
            try {
                ibs.isHotDataEnabled();
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IbsException e) {
                Assert.assertEquals(IbsErrorCode.INVALID_IBS_ID, e.getErrorCode());
                throw e;
            }
        }
        finally {
            for (final File file : files) {
                com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
            }
        }
    }

    /**
     * Base Ibs operations for multi-thread stress test.
     * 
     * 
     */
    static class IbsOpenClose implements Callable<Boolean> {

        final AtomicBoolean goOn = new AtomicBoolean(true);

        @Override
        public Boolean call() throws Exception {
            final ArrayList<File> files = createIbs(HotDataDef.ON);
            try {
                final File tempConfig = files.get(IDX_CONFIG_FILE);

                while (goOn.get()) {
                    final Ibs ibs = IbsFactory.openIbs(tempConfig);
                    try {
                        ibs.start();
                        ibs.stop();
                    }
                    finally {
                        ibs.close();
                    }
                }
            }
            finally {
                for (final File file : files) {
                    com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
                }
            }
            return Boolean.TRUE;
        }

    }

    @Test
    public void testOpenIbsMultiThread() throws InterruptedException, ExecutionException {
        final int threadCount = Runtime.getRuntime().availableProcessors();
        final ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        final List<IbsOpenClose> tasks = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            tasks.add(new IbsOpenClose());
        }

        // Start tasks
        final List<Future<Boolean>> threads = new ArrayList<>(threadCount);
        for (final IbsOpenClose task : tasks) {
            threads.add(exec.submit(task));
        }
        Thread.sleep(15 * 1000);

        // Shutdown tasks
        for (final IbsOpenClose task : tasks) {
            task.goOn.set(false);
        }

        // Check tasks run
        for (final Future<Boolean> thread : threads) {
            Assert.assertTrue(thread.get().booleanValue());
        }
    }

    /**
     * Create a IBS for init tests.
     * 
     * @return the first file is the config file.
     * @throws IOException
     */
    private static final ArrayList<File> createIbs(final HotDataDef hotDataDef) throws IOException {
        final ArrayList<File> result = new ArrayList<>();
        boolean done = false;
        try {
            final File tempConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
            result.add(tempConfig);
            final Path tempDirGen = Files.createTempDirectory(TEMP_PREFIX);
            result.add(tempDirGen.toFile());
            final Path tempDirP1 = Files.createTempDirectory(TEMP_PREFIX);
            result.add(tempDirP1.toFile());
            final Path tempDirP2 = Files.createTempDirectory(TEMP_PREFIX);
            result.add(tempDirP2.toFile());

            // Write config
            writeIbsConfig(hotDataDef, result);

            final Ibs ibs = IbsFactory.createIbs(tempConfig);
            ibs.close();
            done = true;
        }
        finally {
            if (!done) {
                for (final File file : result) {
                    com.oodrive.nuage.utils.Files.deleteRecursive(file.toPath());
                }
                result.clear();
            }
        }
        return result;
    }

    private static final void writeIbsConfig(final HotDataDef hotDataDef, final ArrayList<File> configFiles)
            throws IOException {
        writeIbsConfig(hotDataDef, configFiles, false, false);
    }

    private static final void writeIbsConfig(final HotDataDef hotDataDef, final ArrayList<File> configFiles,
            final boolean altUuid, final boolean altOwner) throws IOException {
        // Write config
        try (PrintStream config = new PrintStream(configFiles.get(IDX_CONFIG_FILE))) {
            config.println(CONF_IBPGEN + configFiles.get(IDX_IBPGEN_DIR).getAbsolutePath());
            config.print(CONF_IBP);
            for (int i = 2; i < configFiles.size(); i++) {
                final File ibpDir = configFiles.get(i);
                config.print(ibpDir.getAbsolutePath());
                if (i != (configFiles.size() - 1)) {
                    config.print(',');
                }
            }
            config.println();

            if (altUuid)
                config.println(CONF_UUID_ALT);
            else
                config.println(CONF_UUID);

            if (altOwner)
                config.println(CONF_OWNER_ALT);
            else
                config.println(CONF_OWNER);

            // HotData config
            if (hotDataDef == HotDataDef.DEFAULT) {
                // nop
            }
            else if (hotDataDef == HotDataDef.ON) {
                config.println(CONF_HOTDATA);
            }
            else if (hotDataDef == HotDataDef.OFF) {
                config.println(CONF_HOTDATA_OFF);
            }
            else {
                throw new AssertionFailedError("Should not be reached");
            }
        }
    }

    /**
     * Change the value of a key in a IBP config file.
     * 
     * @param ibpConfigFile
     * @param key
     * @param newValue
     * @throws IOException
     */
    private static final void changeIbpConfigKey(final File ibpConfigFile, final String key, final String newValue)
            throws IOException {
        // Load config
        final Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream(ibpConfigFile)) {
            config.load(fis);
        }
        // Check that the value is defined
        Assert.assertNotNull(config.get(key));
        config.setProperty(key, newValue);

        // Write config
        try (PrintStream ps = new PrintStream(ibpConfigFile)) {
            final Iterator<Entry<Object, Object>> ite = config.entrySet().iterator();
            while (ite.hasNext()) {
                final Map.Entry<Object, Object> entry = ite.next();
                ps.print(entry.getKey());
                ps.print('=');
                ps.println(entry.getValue());
            }
        }
    }

    /**
     * Write random key/value pairs in the IBS.
     * 
     * @param configFile
     *            IBS config file
     * @return the key/value pairs put in the IBS
     * @throws NullPointerException
     * @throws IbsIOException
     * @throws IbsException
     */
    private static final Map<byte[], byte[]> writeValues(final File configFile) throws IbsException, IbsIOException,
            NullPointerException {

        // Compute random values
        final Random random = new SecureRandom();
        final Map<byte[], byte[]> values = new HashMap<>();
        for (int i = 0; i < 32; i++) {
            final byte[] key = new byte[TST_KEY_LEN];
            random.nextBytes(key);
            final byte[] value = new byte[TST_VALUE_LEN];
            random.nextBytes(value);
            values.put(key, value);
        }

        // Write values to IBS
        final Ibs ibs = IbsFactory.openIbs(configFile);
        try {
            ibs.start();
            try {
                for (final Iterator<Entry<byte[], byte[]>> entries = values.entrySet().iterator(); entries.hasNext();) {
                    final Entry<byte[], byte[]> entry = entries.next();
                    final ByteBuffer value = ByteBuffer.wrap(entry.getValue());
                    ibs.put(entry.getKey(), value);
                }
            }
            finally {
                ibs.stop();
            }
        }
        finally {
            ibs.close();
        }

        return values;
    }
}
