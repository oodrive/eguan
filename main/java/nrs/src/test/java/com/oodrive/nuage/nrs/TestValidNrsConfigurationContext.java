package com.oodrive.nuage.nrs;

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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import org.junit.runners.model.InitializationError;

import com.oodrive.nuage.configuration.ValidConfigurationContext;

/**
 * {@link ValidConfigurationContext} implementation with {@link NrsConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public final class TestValidNrsConfigurationContext extends TestAbstractNrsConfigurationContext {

    public TestValidNrsConfigurationContext() {
        super(new ContextTestHelper<NrsConfigurationContext>(NrsConfigurationContext.getInstance()) {

            private File tmpNrsBaseDir;

            @Override
            public Properties getConfig() {
                final Properties result = new Properties();
                result.setProperty(getPropertyKey(NrsStorageConfigKey.getInstance()), tmpNrsBaseDir.getAbsolutePath());
                result.setProperty(getPropertyKey(BlkCacheDirectoryConfigKey.getInstance()), "bbckcache");
                result.setProperty(getPropertyKey(ImagesFileDirectoryConfigKey.getInstance()), "iimages");
                result.setProperty(getPropertyKey(RemainingSpaceCreateLimitConfigKey.getInstance()), "5");
                result.setProperty(getPropertyKey(NrsClusterSizeConfigKey.getInstance()), "8192");
                return result;
            }

            @Override
            public void setUp() throws InitializationError {
                try {
                    tmpNrsBaseDir = Files.createTempDirectory(NRS_TMPDIR_PREFIX).toFile();
                }
                catch (final IOException e) {
                    throw new InitializationError(e);
                }
            }

            @Override
            public void tearDown() throws InitializationError {
                try {
                    com.oodrive.nuage.utils.Files.deleteRecursive(tmpNrsBaseDir.toPath());
                }
                catch (final IOException e) {
                    throw new InitializationError(e);
                }
            }
        });
    }

}
