package io.eguan.vvr.configuration;

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

import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.hash.HashAlgorithm;
import io.eguan.vvr.configuration.CommonConfigurationContext;
import io.eguan.vvr.configuration.keys.BlockSizeConfigKey;
import io.eguan.vvr.configuration.keys.DeletedConfigKey;
import io.eguan.vvr.configuration.keys.DescriptionConfigkey;
import io.eguan.vvr.configuration.keys.HashAlgorithmConfigKey;
import io.eguan.vvr.configuration.keys.NameConfigKey;
import io.eguan.vvr.configuration.keys.NodeConfigKey;
import io.eguan.vvr.configuration.keys.StartedConfigKey;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.junit.Test;

/**
 * {@link ValidConfigurationContext} implementation with {@link CommonConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestValidCommonConfigurationContext extends ValidConfigurationContext {

    private static final ContextTestHelper<CommonConfigurationContext> testHelper = new ContextTestHelper<CommonConfigurationContext>(
            CommonConfigurationContext.getInstance()) {

        @Override
        public void setUp() {
        }

        @Override
        public void tearDown() {
        }

        @Override
        public Properties getConfig() {
            final Properties result = new Properties();
            result.setProperty(getPropertyKey(NameConfigKey.getInstance()), "VvrName");
            result.setProperty(getPropertyKey(DescriptionConfigkey.getInstance()), "VvrDescription");
            result.setProperty(getPropertyKey(BlockSizeConfigKey.getInstance()), "8192");
            result.setProperty(getPropertyKey(HashAlgorithmConfigKey.getInstance()), "TIGER");
            result.setProperty(getPropertyKey(NodeConfigKey.getInstance()), UUID.randomUUID().toString());
            result.setProperty(getPropertyKey(StartedConfigKey.getInstance()), Boolean.TRUE.toString());
            result.setProperty(getPropertyKey(DeletedConfigKey.getInstance()), Boolean.FALSE.toString());
            return result;
        }
    };

    /**
     * Tests failure to create a {@link MetaConfiguration} with {@link CommonConfigurationContext} as context due to a
     * too low {@link BlockSizeConfigKey} value.
     * 
     * @throws RuntimeException
     *             if creation fails. Not part of this test.
     * @throws IOException
     *             if reading the {@link InputStream} fails. Not part of this test.
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationCommonConfigurationContextFailBlockSizeTooLow() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();
        config.setProperty(testHelper.getPropertyKey(BlockSizeConfigKey.getInstance()), "511");

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                CommonConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link CommonConfigurationContext} as context due to a
     * too high {@link BlockSizeConfigKey} value.
     * 
     * @throws RuntimeException
     *             if creation fails. Not part of this test.
     * @throws IOException
     *             if reading the {@link InputStream} fails. Not part of this test.
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationCommonConfigurationContextFailBlockSizeTooHigh() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();
        config.setProperty(testHelper.getPropertyKey(BlockSizeConfigKey.getInstance()), "65537");

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                CommonConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link CommonConfigurationContext} as context due to a
     * value {@link HashAlgorithmConfigKey} which is not a {@link HashAlgorithm} constant.
     * 
     * @throws IOException
     *             if reading the {@link InputStream} fails. Not part of this test.
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testMetaConfigurationCommonConfigurationContextFailBadHashAlgorithm() throws IOException,
            ConfigValidationException {
        final Properties config = testHelper.getConfig();
        config.setProperty(testHelper.getPropertyKey(HashAlgorithmConfigKey.getInstance()), "FOOBAR");

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                CommonConfigurationContext.getInstance());
    }

    @Override
    public final ContextTestHelper<?> getTestHelper() {
        return testHelper;
    }

}
