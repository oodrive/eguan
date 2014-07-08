package com.oodrive.nuage.configuration;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.junit.Test;
import org.junit.runners.model.InitializationError;

/**
 * Class for validation test common to all implementations of {@link AbstractConfigurationContext}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author llambert
 * 
 */
public abstract class ValidConfigurationContext {

    /**
     * Helper class including utility methods and common methods to test subclasses of
     * {@link AbstractConfigurationContext}.
     * 
     * 
     * @param <T>
     *            the {@link AbstractConfigurationContext} subclass this instance helps to test
     */
    public static abstract class ContextTestHelper<T extends AbstractConfigurationContext> {

        /**
         * Proxy helper method to get a given {@link Properties} instance as {@link InputStream}.
         * 
         * Calls {@link ConfigTestHelper#getPropertiesAsInputStream(Properties)}.
         * 
         * @param properties
         *            the non-{@code null} {@link Properties} instance to convert
         * @return an {@link InputStream} providing the exact content of the argument
         * @throws IOException
         *             if storing fails
         * @see ConfigTestHelper#getPropertiesAsInputStream(Properties)
         */
        public static InputStream getPropertiesAsInputStream(final Properties properties) throws IOException {
            return ConfigTestHelper.getPropertiesAsInputStream(properties);
        }

        /**
         * Utility method to allow tests to get values directly parsed by a given {@link AbstractConfigKey}.
         * 
         * @param key
         *            the {@link AbstractConfigKey} to parse the value for
         * @param value
         *            a raw String value to be parsed
         * @return the correctly typed value
         * @see AbstractConfigKey#parseValue(String)
         */
        public static Object getParsedValue(final AbstractConfigKey key, final String value) {
            return key.parseValue(value);
        }

        /**
         * Utility method to get a {@link String} representation of an {@link AbstractConfigKey}'s value in a given
         * {@link MetaConfiguration}.
         * 
         * @param config
         *            the {@link MetaConfiguration} from which to extract the value
         * @param key
         *            the {@link AbstractConfigKey} managed by the configuration for which to get the value
         * @return a {@link String} as provided by {@link AbstractConfigKey#valueToString(Object)}
         */
        public static String getStringValue(final MetaConfiguration config, final AbstractConfigKey key) {
            return key.valueToString(key.getTypedValue(config));
        }

        /**
         * Utility method to produce a complete property key mapping used by tests.
         * 
         * @param context
         *            the {@link AbstractConfigurationContext} to produce the mapping for
         * @return a complete mapping with all keys returned by {@link AbstractConfigurationContext#getConfigKeys()}
         */
        protected static Map<AbstractConfigKey, String> getPropertyKeyMapping(final AbstractConfigurationContext context) {
            final HashMap<AbstractConfigKey, String> propertyKeys = new HashMap<AbstractConfigKey, String>();
            for (final AbstractConfigKey currKey : context.getConfigKeys()) {
                propertyKeys.put(currKey, context.getPropertyKey(currKey));
            }
            return propertyKeys;
        }

        /**
         * The target {@link AbstractConfigurationContext context}.
         */
        private final T context;

        /**
         * Map of all property keys to be found in the test property file content, indexed by {@link AbstractConfigKey}.
         */
        private final Map<AbstractConfigKey, String> propertyKeys;

        /**
         * Constructs the instance, initializing the common context and property key functionalities.
         * 
         * @param context
         *            the {@link AbstractConfigurationContext} instance to be tested
         */
        public ContextTestHelper(final T context) {
            this.context = Objects.requireNonNull(context);
            this.propertyKeys = getPropertyKeyMapping(context);
        }

        /**
         * Gets the default config, i.e. the configuration where all keys with a defined default value are set to that
         * value.
         * 
         * Note: required keys without a default value are set to the value provided by {@link #getConfig()}
         * 
         * @return a complete and valid configuration with a maximum of default values for the tested context
         */
        public final Properties getDefaultConfig() {
            final Properties result = getConfig();
            for (final AbstractConfigKey currKey : context.getConfigKeys()) {
                if (currKey.hasDefaultValue()) {
                    result.setProperty(context.getPropertyKey(currKey),
                            currKey.valueToString(currKey.getDefaultValue()));
                }
            }
            return result;
        }

        /**
         * A method to be called by JUnit {@link org.junit.BeforeClass} or {@link org.junit.Before} methods to set up
         * the necessary fixture to test this configuration.
         * 
         * @throws InitializationError
         *             if setup fails
         */
        public abstract void setUp() throws InitializationError;

        /**
         * A method to be called by JUnit {@link org.junit.AfterClass} or {@link org.junit.After} methods to tear down
         * the fixture set up by {@link #setUp()}.
         * 
         * @throws InitializationError
         *             if tearing down fails
         */
        public abstract void tearDown() throws InitializationError;

        /**
         * Gets the valid configuration to test.
         * 
         * @return a complete and valid configuration for the tested context
         */
        public abstract Properties getConfig();

        /**
         * Utility method to get the property key of a given {@link AbstractConfigKey} as it appears in the
         * configuration input or output.
         * 
         * @param key
         *            the {@link AbstractConfigKey} for which to retrieve the property key
         * @return the property key or <code>null</code> if it's not managed by the target context
         */
        public final String getPropertyKey(final AbstractConfigKey key) {
            return propertyKeys.get(key);
        }

        /**
         * Gets the context to test.
         * 
         * @return the {@link AbstractConfigurationContext}
         */
        public final T getContext() {
            return this.context;
        }

    }

    /**
     * Gets the {@link ContextTestHelper} instance used in all tests of this class.
     * 
     * @return a functional instance of {@link ContextTestHelper}
     */
    public abstract ContextTestHelper<?> getTestHelper();

    /**
     * Tests successful validation of a {@link MetaConfiguration} with
     * {@link CommonConfigurationContext#validateConfiguration(MetaConfiguration)}.
     * 
     * @throws ConfigValidationException
     *             if the {@link ContextTestHelper#getConfig() configuration} is invalid. Considered a test failure.
     * @throws IOException
     *             if reading the {@link InputStream} fails. Not part of this test.
     * @throws RuntimeException
     *             if creation fails. Not part of this test.
     */
    @Test
    public final void testValidateConfiguration() throws RuntimeException, IOException, ConfigValidationException {

        final ContextTestHelper<?> testHelper = getTestHelper();

        final AbstractConfigurationContext context = testHelper.getContext();

        final MetaConfiguration config = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(testHelper.getConfig()), context);

        final List<ValidationError> result = context.validateConfiguration(config);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Tests successful validation of a minimal {@link MetaConfiguration} with
     * {@link CommonConfigurationContext#validateConfiguration(MetaConfiguration)}.
     * 
     * @throws ConfigValidationException
     *             if the {@link ContextTestHelper#getConfig() configuration} is invalid. Considered a test failure.
     * @throws IOException
     *             if reading the {@link InputStream} fails. Not part of this test.
     * @throws RuntimeException
     *             if creation fails. Not part of this test.
     */
    @Test
    public final void testValidateConfigurationMinimal() throws RuntimeException, IOException,
            ConfigValidationException {

        final ContextTestHelper<?> testHelper = getTestHelper();

        final Properties props = testHelper.getConfig();

        final AbstractConfigurationContext context = testHelper.getContext();

        // removes all keys that are not required or do have a default value
        for (final AbstractConfigKey currKey : context.getConfigKeys()) {
            if (!currKey.hasDefaultValue() || currKey.isRequired()) {
                continue;
            }
            props.remove(testHelper.getPropertyKey(currKey));
        }

        final InputStream emptyInput = ContextTestHelper.getPropertiesAsInputStream(props);
        final MetaConfiguration config = MetaConfiguration.newConfiguration(emptyInput, context);

        for (final AbstractConfigKey currKey : context.getConfigKeys()) {
            if (!currKey.hasDefaultValue() || currKey.isRequired()) {
                continue;
            }
            assertEquals(currKey.getDefaultValue(), currKey.getTypedValue(config));
        }

        final AbstractConfigurationContext target = context;

        final List<ValidationError> result = target.validateConfiguration(config);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    /**
     * Tests successful creation of a {@link MetaConfiguration} with the target {@link AbstractConfigurationContext} as
     * only context.
     * 
     * @throws RuntimeException
     *             if creation fails. Considered a test failure.
     * @throws IOException
     *             if reading the {@link InputStream} fails. Considered a test failure.
     * @throws ConfigValidationException
     *             if the {@link ContextTestHelper#getConfig() configuration} is invalid. Considered a test failure.
     */
    @Test
    public final void testCreateMetaConfigurationWithConfigurationContext() throws RuntimeException, IOException,
            ConfigValidationException {

        final ContextTestHelper<?> testHelper = getTestHelper();

        final Properties configProperties = testHelper.getConfig();

        final AbstractConfigurationContext target = testHelper.getContext();

        final MetaConfiguration config = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(configProperties), target);

        for (final AbstractConfigKey currKey : target.getConfigKeys()) {
            assertEquals(
                    ContextTestHelper.getParsedValue(currKey,
                            configProperties.getProperty(testHelper.getPropertyKey((currKey)))),
                    currKey.getTypedValue(config));
        }

    }

    /**
     * Test {@link AbstractConfigurationContext#toString()} for all implementing classes.
     */
    @Test
    public final void testToString() {

        final ContextTestHelper<?> testHelper = getTestHelper();

        final AbstractConfigurationContext target = testHelper.getContext();

        assertEquals("toString returns class' simple name", target.getClass().getSimpleName(), target.toString());
    }

}
