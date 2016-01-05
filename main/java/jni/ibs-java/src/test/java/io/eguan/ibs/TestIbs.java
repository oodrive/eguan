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

import static io.eguan.ibs.IbsTestDefinitions.COMPRESSION_KEYWORD;
import static io.eguan.ibs.IbsTestDefinitions.CONF_DEBUG_LEVEL;
import static io.eguan.ibs.IbsTestDefinitions.CONF_HOTDATA;
import static io.eguan.ibs.IbsTestDefinitions.CONF_IBP;
import static io.eguan.ibs.IbsTestDefinitions.CONF_IBPGEN;
import static io.eguan.ibs.IbsTestDefinitions.CONF_OWNER;
import static io.eguan.ibs.IbsTestDefinitions.CONF_UUID;
import static io.eguan.ibs.IbsTestDefinitions.TEMP_PREFIX;
import static io.eguan.ibs.IbsTestDefinitions.TEMP_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.slf4j.LoggerFactory;

/**
 * Parent class to run tests on an initialized IBS.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public abstract class TestIbs {

    static class IbsInitHelper {
        /** IBS configuration file */
        private File tempFileConfig = null;
        /** IBPGEN directory */
        private Path tempDirIbpgen = null;
        /** IBP directory */
        private Path tempDirIbp = null;
        /** IBS identifier */
        private Ibs ibs = null;

        /**
         * @param compression
         *            optional compression mode (no, front, back)
         * @throws IOException
         */
        void initIbs(final IbsType ibsType, final String compression) throws IOException {
            initIbs(ibsType, compression, true);
        }

        void initIbs(final IbsType ibsType, final String compression, final boolean create) throws IOException {
            this.tempFileConfig = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);

            if (ibsType == IbsType.LEVELDB) {
                // Fill config file
                this.tempDirIbpgen = Files.createTempDirectory(TEMP_PREFIX);
                this.tempDirIbp = Files.createTempDirectory(TEMP_PREFIX);

                // Write config
                try (PrintStream config = new PrintStream(this.tempFileConfig)) {
                    config.println(CONF_IBPGEN + this.tempDirIbpgen.toAbsolutePath());
                    config.println(CONF_IBP + this.tempDirIbp.toAbsolutePath());
                    config.println(CONF_HOTDATA);
                    config.println(CONF_UUID);
                    config.println(CONF_OWNER);
                    config.println(CONF_DEBUG_LEVEL);
                    if (compression != null) {
                        config.println(COMPRESSION_KEYWORD + compression);
                    }
                }
            }
            else if (ibsType == IbsType.FS) {
                // Needs an empty directory
                Assert.assertTrue(this.tempFileConfig.delete());
                Assert.assertTrue(this.tempFileConfig.mkdir());
            }
            else if (ibsType == IbsType.FAKE) {
                // Nothing to do
            }
            else {
                throw new AssertionError(ibsType);
            }

            // Create / close / init IBS
            if (create) {
                this.ibs = IbsFactory.createIbs(this.tempFileConfig, ibsType);
                this.ibs.close();
                this.ibs = IbsFactory.openIbs(this.tempFileConfig, ibsType);
            }
        }

        /**
         * Cleanup IBS. Remove as much elements as possible.
         * 
         * @throws Exception
         */
        void finiIbs() throws Exception {

            Exception lastException = null;

            if (this.ibs != null) {
                this.ibs.close();
                this.ibs = null;
            }

            if (this.tempFileConfig != null) {
                if (this.tempFileConfig.isFile()) {
                    this.tempFileConfig.delete();
                }
                else {
                    io.eguan.utils.Files.deleteRecursive(this.tempFileConfig.toPath());
                }
                this.tempFileConfig = null;
            }

            if (this.tempDirIbpgen != null) {
                try {
                    io.eguan.utils.Files.deleteRecursive(this.tempDirIbpgen);
                }
                catch (final IOException e) {
                    LoggerFactory.getLogger(getClass()).warn("Failed to delete IBPGEN path: " + this.tempDirIbpgen, e);
                    lastException = e;
                }
                this.tempDirIbpgen = null;
            }

            if (this.tempDirIbp != null) {
                try {
                    io.eguan.utils.Files.deleteRecursive(this.tempDirIbp);
                }
                catch (final IOException e) {
                    LoggerFactory.getLogger(getClass()).warn("Failed to delete IBP path: " + this.tempDirIbp, e);
                    lastException = e;
                }
                this.tempDirIbp = null;
            }

            // Test failure if something happened
            if (lastException != null) {
                throw lastException;
            }
        }

        final File getTempFileConfig() {
            return this.tempFileConfig;
        }

        final Path getTempDirIbpgen() {
            return this.tempDirIbpgen;
        }

        final Path getTempDirIbp() {
            return this.tempDirIbp;
        }

        final Ibs getIbs() {
            return this.ibs;
        }

    }
}
