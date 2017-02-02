package io.eguan.nrs;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.nrs.BlkCacheDirectoryConfigKey;
import io.eguan.nrs.ImagesFileDirectoryConfigKey;
import io.eguan.nrs.NrsClusterSizeConfigKey;
import io.eguan.nrs.NrsConfigurationContext;
import io.eguan.nrs.NrsStorageConfigKey;
import io.eguan.nrs.RemainingSpaceCreateLimitConfigKey;
import io.eguan.utils.RunCmdUtils;
import io.eguan.utils.unix.UnixFsFile;
import io.eguan.utils.unix.UnixMount;
import io.eguan.utils.unix.UnixUser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.InitializationError;

/**
 * 
 * Test configuration using a locally mounted file system.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
@RunWith(value = Parameterized.class)
public final class TestValidNrsConfigurationMountedContext extends TestAbstractNrsConfigurationContext {

    @Parameters
    public static Collection<Object[]> getMountConfig() {
        final UnixUser user = UnixUser.getCurrentUser();
        final Object[][] mountConfigs = new Object[][] {
        // vfat mounted read/write: does not support FS attributes
        { "vfat", "utf8,uid=" + user.getUid() + ",gid=" + user.getGid() } };
        return Arrays.asList(mountConfigs);
    }

    public TestValidNrsConfigurationMountedContext(final String helpersNrsFsType, final String helpersNrsMntOptions) {
        super(new ContextTestHelper<NrsConfigurationContext>(NrsConfigurationContext.getInstance()) {

            private static final int FS_SIZE = 8 * 1024 * 1024;

            private File testNrsBaseDir;
            private File mountPoint;
            // private File tmpNrsBaseDir;
            private UnixMount unixMount;
            private boolean mounted;

            @Override
            public Properties getConfig() {
                final Properties result = new Properties();
                result.setProperty(getPropertyKey(NrsStorageConfigKey.getInstance()), mountPoint.getAbsolutePath());
                result.setProperty(getPropertyKey(BlkCacheDirectoryConfigKey.getInstance()), "bbckcache");
                result.setProperty(getPropertyKey(ImagesFileDirectoryConfigKey.getInstance()), "iimages");
                result.setProperty(getPropertyKey(RemainingSpaceCreateLimitConfigKey.getInstance()), "1");
                result.setProperty(getPropertyKey(NrsClusterSizeConfigKey.getInstance()), "8192");
                return result;
            }

            @Override
            public void setUp() throws InitializationError {
                try {
                    // Base temporary directory
                    testNrsBaseDir = Files.createTempDirectory(NRS_TMPDIR_PREFIX).toFile();

                    // File containing the file system
                    final File tmpFsFile = File.createTempFile("nrsTst", ".fs", testNrsBaseDir);
                    final UnixFsFile unixFsFile = new UnixFsFile(tmpFsFile, FS_SIZE, helpersNrsFsType,
                            helpersNrsMntOptions);
                    unixFsFile.create();

                    mountPoint = Files.createTempDirectory(testNrsBaseDir.toPath(), "mnt").toFile();
                    unixMount = unixFsFile.newUnixMount(mountPoint);
                    unixMount.mount();
                    mounted = true;

                    // Give access to all
                    final String[] chmodArray = new String[] { "sudo", "chmod", "777", mountPoint.getAbsolutePath() };
                    RunCmdUtils.runCmd(chmodArray, this);
                }
                catch (final IOException e) {
                    throw new InitializationError(e);
                }
            }

            @Override
            public void tearDown() throws InitializationError {
                try {
                    if (mounted) {
                        unixMount.umount();
                        mounted = false;
                    }
                    io.eguan.utils.Files.deleteRecursive(testNrsBaseDir.toPath());
                }
                catch (final IOException e) {
                    throw new InitializationError(e);
                }
            }
        });
    }

}
