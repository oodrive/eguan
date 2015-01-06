package com.oodrive.nuage.vvr.persistence.configuration;

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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;

import com.oodrive.nuage.configuration.ValidConfigurationContext;
import com.oodrive.nuage.vvr.configuration.PersistenceConfigurationContext;
import com.oodrive.nuage.vvr.configuration.keys.DeviceFileDirectoryConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.SnapshotFileDirectoryConfigKey;

/**
 * {@link ValidConfigurationContext} implementation with {@link PersistenceConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class TestValidPersistenceConfigurationContext extends ValidConfigurationContext {

    private static ContextTestHelper<PersistenceConfigurationContext> testHelper = new ContextTestHelper<PersistenceConfigurationContext>(
            PersistenceConfigurationContext.getInstance()) {

        private File tmpNrsBaseDir;

        @Override
        public Properties getConfig() {
            final Properties result = new Properties();
            result.setProperty(getPropertyKey(DeviceFileDirectoryConfigKey.getInstance()), "deevices");
            result.setProperty(getPropertyKey(SnapshotFileDirectoryConfigKey.getInstance()), "snaapshots");
            return result;
        }

        @Override
        public void setUp() throws InitializationError {
            try {
                tmpNrsBaseDir = Files.createTempDirectory("vvrNrsStorage").toFile();
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
    };

    @BeforeClass
    public static final void setUpClass() throws InitializationError {
        testHelper.setUp();
    }

    @AfterClass
    public static final void tearDownClass() throws InitializationError {
        testHelper.tearDown();
    }

    @Override
    public final ContextTestHelper<?> getTestHelper() {
        return testHelper;
    }

}
