package com.oodrive.nuage.rest.container;

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

import com.oodrive.nuage.configuration.ConfigValidationException;
import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.configuration.ValidConfigurationContext;
import com.oodrive.nuage.configuration.ValidationError;

/**
 * {@link ValidConfigurationContext} implementation with {@link JettyConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class TestJettyConfigurationContext extends ValidConfigurationContext {

    private static final ContextTestHelper<JettyConfigurationContext> testHelper = new ContextTestHelper<JettyConfigurationContext>(
            JettyConfigurationContext.getInstance()) {

        private Path resourceBaseFile;
        private URL resourceBaseUrl;

        @Override
        public final void setUp() throws InitializationError {
            try {
                resourceBaseFile = Files.createTempDirectory(TestJettyConfigurationContext.class.getSimpleName());
                resourceBaseUrl = resourceBaseFile.toUri().toURL();
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
        }

        @Override
        public final void tearDown() throws InitializationError {
            try {
                com.oodrive.nuage.utils.Files.deleteRecursive(resourceBaseFile);
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
        }

        @Override
        public final Properties getConfig() {
            final Properties result = new Properties();
            result.setProperty(getPropertyKey(ServerAddressConfigKey.getInstance()), "127.0.0.1");
            result.setProperty(getPropertyKey(ServerPortConfigKey.getInstance()), "8088");
            result.setProperty(getPropertyKey(JettyStopAtShutdownConfigKey.getInstance()), "false");
            result.setProperty(getPropertyKey(RestContextPathConfigKey.getInstance()), "/webapp");
            result.setProperty(getPropertyKey(RestResourceBaseConfigKey.getInstance()),
                    resourceBaseUrl.toExternalForm());
            result.setProperty(getPropertyKey(WebUiContextPathConfigKey.getInstance()), "/admin");
            result.setProperty(getPropertyKey(WebUiWarNameConfigKey.getInstance()), "");
            return result;
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
    public final ContextTestHelper<JettyConfigurationContext> getTestHelper() {
        return testHelper;
    }

    /**
     * Tests creating a {@link MetaConfiguration} after removing the explicitly configured web application resource
     * base.
     * 
     * @throws IllegalArgumentException
     *             if the configuration fails to parse, considered a test failure
     * @throws IOException
     *             if the configuration cannot be read, not part of this test
     * @throws ConfigValidationException
     *             if the configuration is invalid, considered a test failure
     */
    @Test
    public void testCreateConfigurationWithDefaultResourceBase() throws IllegalArgumentException, IOException,
            ConfigValidationException {
        final ContextTestHelper<JettyConfigurationContext> testHelper = getTestHelper();

        final JettyConfigurationContext context = testHelper.getContext();

        final RestResourceBaseConfigKey resourceBaseKey = RestResourceBaseConfigKey.getInstance();

        final Properties testConfig = testHelper.getConfig();
        testConfig.remove(context.getPropertyKey(resourceBaseKey));

        final MetaConfiguration config = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(testConfig), context);

        final List<ValidationError> result = context.validateConfiguration(config);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        assertNotNull(resourceBaseKey.getTypedValue(config));

    }

}
