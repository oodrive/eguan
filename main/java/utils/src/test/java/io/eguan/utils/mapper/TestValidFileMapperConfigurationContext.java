package io.eguan.utils.mapper;

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

import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.utils.mapper.DirPrefixLengthConfigKey;
import io.eguan.utils.mapper.DirStructureDepthConfigKey;
import io.eguan.utils.mapper.FileMapper;
import io.eguan.utils.mapper.FileMapperConfigKey;
import io.eguan.utils.mapper.FileMapperConfigurationContext;

import java.io.IOException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

/**
 * {@link ValidConfigurationContext} implementation with {@link FileMapperConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestValidFileMapperConfigurationContext extends ValidConfigurationContext {

    private static ContextTestHelper<FileMapperConfigurationContext> testHelper = new ContextTestHelper<FileMapperConfigurationContext>(
            FileMapperConfigurationContext.getInstance()) {

        @Override
        public Properties getConfig() {
            final Properties result = new Properties();
            result.setProperty(getPropertyKey(DirPrefixLengthConfigKey.getInstance()), "3");
            result.setProperty(getPropertyKey(DirStructureDepthConfigKey.getInstance()), "3");
            result.setProperty(getPropertyKey(FileMapperConfigKey.getInstance()), FileMapper.Type.FLAT.toString());
            return result;
        }

        @Override
        public void setUp() throws InitializationError {
            // Nop
        }

        @Override
        public void tearDown() throws InitializationError {
            // Nop
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

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link FileMapperConfigurationContext} as context due to
     * {@link DirPrefixLengthConfigKey} and {@link DirStructureDepthConfigKey} multiplying to more than 31.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationPersistenceConfigurationContextFailDirPrefixStructure()
            throws RuntimeException, IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();
        config.setProperty(testHelper.getPropertyKey(DirPrefixLengthConfigKey.getInstance()), "4");
        config.setProperty(testHelper.getPropertyKey(DirStructureDepthConfigKey.getInstance()), "8");

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                FileMapperConfigurationContext.getInstance());
    }

}
