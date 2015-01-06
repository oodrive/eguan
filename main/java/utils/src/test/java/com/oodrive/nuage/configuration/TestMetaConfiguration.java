package com.oodrive.nuage.configuration;

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

import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_BOOLEAN_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_ENUM_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_FILE_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_POS_INT_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_UNDEFINED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.math.IntMath;
import com.oodrive.nuage.configuration.IntegerConfigKey.PositiveIntegerConfigKey;
import com.oodrive.nuage.configuration.ValidationError.ErrorType;
import com.oodrive.nuage.utils.Files;

/**
 * Test class for {@link MetaConfiguration}'s methods.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * @author ebredzinski
 */
public final class TestMetaConfiguration {

    /**
     * Tests successful creation of a {@link MetaConfiguration} instance using
     * {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testCreateMetaConfiguration() throws IOException, ConfigValidationException, RuntimeException {
        // initializes the configuration
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        // gets the configuration context
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration result = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(result);
        assertTrue("result has registered context keys", result.getConfigKeys()
                .containsAll(testContext.getConfigKeys()));

        for (final AbstractConfigKey currKey : testContext.getConfigKeys()) {
            if (currKey == ConfigTestContext.TEST_UNDEFINED_KEY) {
                continue;
            }
            assertEquals("Retrieved key has expected typed value; key=" + currKey.getClass().getSimpleName(),
                    currKey.parseValue(testConfiguration.getProperty(testContext.getPropertyKey(currKey))),
                    currKey.getTypedValue(result));
        }
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an {@link InputStream} that is {@code null}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws NullPointerException
     *             if the {@link InputStream} is {@code null}. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = NullPointerException.class)
    public final void testCreateMetaConfigurationFailInputNull() throws IOException, ConfigValidationException,
            NullPointerException, RuntimeException {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        MetaConfiguration.newConfiguration((InputStream) null, testContext);
    }

    @Test(expected = NullPointerException.class)
    public final void testCreateMetaConfigurationFailFileNull() throws IOException, ConfigValidationException,
            NullPointerException, RuntimeException {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        MetaConfiguration.newConfiguration((File) null, testContext);
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of a {@link ConfigurationContext}... argument set to {@code null}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if the passed {@link ConfigurationContext} is {@code null}. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMetaConfigurationFailContextListNull() throws IOException, ConfigValidationException,
            IllegalArgumentException, RuntimeException {

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(ConfigTestHelper
                .getDefaultTestConfiguration());

        MetaConfiguration.newConfiguration(inputStream, (AbstractConfigurationContext[]) null);
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of a {@link ConfigurationContext}... argument set to an empty list.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if the passed list of {@link ConfigurationContext}s is empty. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMetaConfigurationFailContextListEmpty() throws IOException, ConfigValidationException,
            IllegalArgumentException, RuntimeException {

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(ConfigTestHelper
                .getDefaultTestConfiguration());

        MetaConfiguration.newConfiguration(inputStream, new AbstractConfigurationContext[] {});
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of a {@link ConfigurationContext}... argument set to a list containing only {@code null} as an element.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if the passed {@link ConfigurationContext} contains {@code null}. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMetaConfigurationFailContextListContainsOnlyNull() throws IOException,
            ConfigValidationException, IllegalArgumentException, RuntimeException {

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(ConfigTestHelper
                .getDefaultTestConfiguration());

        MetaConfiguration.newConfiguration(inputStream, new AbstractConfigurationContext[] { null });
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. missing a required value passed through the inputStream
     * argument.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigRequiredMissing() throws IOException,
            ConfigValidationException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        testConfiguration.remove(testContext.getPropertyKey(TEST_POS_INT_KEY));

        MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. a value passed through the inputStream argument is out of range
     * (a positive integer < 0).
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigOutOfRange() throws IOException,
            ConfigValidationException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        testConfiguration
                .setProperty(testContext.getPropertyKey(TEST_POS_INT_KEY), Integer.toString(Integer.MIN_VALUE));

        MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. an enum value passed through the inputStream argument is not
     * part of the authorized constant values.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if one of the loaded enum key values is not valid. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigNotAnEnumConstant() throws IOException,
            ConfigValidationException, IllegalArgumentException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        testConfiguration.setProperty(testContext.getPropertyKey(TEST_ENUM_KEY), "TEST_VALUE_ENUM");

        try {
            MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration),
                    testContext);
        }
        catch (final IllegalArgumentException ie) {
            // classify the thrown exception further
            assertTrue("Exception thrown due to missing enum constant", ie.getMessage().startsWith("No enum constant"));
            throw ie;
        }

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. a string that cannot be parsed to a number in the inputStream.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if one of the loaded key values is not valid. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigUnparseableString() throws IOException,
            ConfigValidationException, IllegalArgumentException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        testConfiguration.setProperty(ConfigTestContext.getInstance()
                .getPropertyKey(ConfigTestContext.TEST_POS_INT_KEY), "negatory");

        MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. a string of spaces passed for a required key.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if one of the loaded key values is not valid. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigStringOnlySpaces() throws IOException,
            ConfigValidationException, IllegalArgumentException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        testConfiguration.setProperty(ConfigTestContext.getInstance()
                .getPropertyKey(ConfigTestContext.TEST_POS_INT_KEY), "    ");

        MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an unreadable {@link InputStream} passed as argument.
     * 
     * @throws IOException
     *             if reading the configuration fails. Expected for this test.
     */
    @Test(expected = IOException.class)
    public final void testCreateMetaConfigurationFailUnreadableInput() throws IOException, ConfigValidationException,
            IllegalArgumentException, RuntimeException {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final InputStream unreadableInputStream = new InputStream() {

            @Override
            public final int read() throws IOException {
                throw new IOException("unreadable stream");
            }

        };

        MetaConfiguration.newConfiguration(unreadableInputStream, testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an {@link InputStream} throwing an {@link IOException} when being closed.
     * 
     * @throws IOException
     *             if reading the configuration fails. Expected for this test.
     */
    @Test(expected = IOException.class)
    public final void testCreateMetaConfigurationFailIOExceptionUncloseable() throws IOException,
            ConfigValidationException, IllegalArgumentException, RuntimeException {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final InputStream unreadableInputStream = new InputStream() {

            @Override
            public final int read() throws IOException {
                return 0;
            }

            @Override
            public final void close() throws IOException {
                throw new IOException("uncloseable stream");
            }

        };

        MetaConfiguration.newConfiguration(unreadableInputStream, testContext);

    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of duplicate key values in the {@link InputStream} argument.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws IllegalArgumentException
     *             if one of the loaded key values is not valid. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailDuplicateKeysInInput() throws ConfigValidationException,
            IOException {

        final ConfigTestContext context = ConfigTestContext.getInstance();

        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output));) {

            writer.append("com.oodrive.nuage.test.key.one = foo");
            writer.newLine();
            writer.append("com.oodrive.nuage.test.key.two = bar");
            writer.newLine();
            writer.append("com.oodrive.nuage.test.key.two = bar2");
            writer.newLine();
            writer.append("com.oodrive.nuage.test.key.one = foo2");
            writer.newLine();
            writer.append("com.oodrive.nuage.test.key.one = foo3");
            writer.newLine();
            writer.flush();

            try {
                MetaConfiguration.newConfiguration(new ByteArrayInputStream(output.toByteArray()), context);
            }
            catch (final ConfigValidationException ve) {
                final List<ValidationError> report = ve.getValidationReport();
                assertNotNull(report);
                assertFalse("report is not empty", report.isEmpty());

                final List<ErrorType> typeList = ve.getErrorTypeList();
                // check numbers of errors by type
                assertEquals("1 error of type context name due to name collision", 2,
                        Collections.frequency(typeList, ErrorType.KEYS_NAME));

                throw ve;
            }

        }
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. passing a {@link TestMetaConfiguration.BadConfigContext} with
     * duplicate, badly named and null keys as well as a colliding name.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailInvalidConfigContext() throws IOException,
            ConfigValidationException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final BadConfigContext testBadContext = new BadConfigContext();

        try {
            MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration),
                    testContext, testBadContext);
        }
        catch (final ConfigValidationException ve) {
            final List<ValidationError> report = ve.getValidationReport();
            assertNotNull(report);
            assertFalse("report is not empty", report.isEmpty());

            final List<ErrorType> typeList = ve.getErrorTypeList();
            // check numbers of errors by type
            assertEquals("1 error of type context name due to name collision", 1,
                    Collections.frequency(typeList, ErrorType.CONTEXT_NAME));
            assertEquals("1 error of type context name gotten by type", 1, ve.getErrorsByType(ErrorType.CONTEXT_NAME)
                    .size());

            assertEquals("2 errors of type context keys due to null key and duplicate keys", 2,
                    Collections.frequency(typeList, ErrorType.CONTEXT_KEYS));
            assertEquals("2 errors of type context key gotten by type", 2, ve.getErrorsByType(ErrorType.CONTEXT_KEYS)
                    .size());

            assertEquals("2 errors of type key name (due to null and empty string as names)", 2,
                    Collections.frequency(typeList, ErrorType.KEYS_NAME));
            assertEquals("2 errors of type key name gotten by type", 2, ve.getErrorsByType(ErrorType.KEYS_NAME).size());

            throw ve;
        }
    }

    /**
     * Tests failure of the {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)} method
     * because of an invalid {@link Configuration}, i.e. passing a badly named {@link AbstractConfigurationContext} with
     * a duplicate key.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Expected for this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} otherwise fails. Considered a test failure.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCreateMetaConfigurationFailConfigContextName() throws IOException, ConfigValidationException,
            RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final AbstractConfigurationContext testBadlyNamedContext = new AbstractConfigurationContext(
                "com.oodrive.nuage.java.utils.test..bad", ConfigTestContext.TEST_ENUM_KEY) {
        };

        try {
            MetaConfiguration.newConfiguration(ConfigTestHelper.getPropertiesAsInputStream(testConfiguration),
                    testContext, testBadlyNamedContext);
        }
        catch (final ConfigValidationException ve) {
            final List<ValidationError> report = ve.getValidationReport();
            assertNotNull(report);
            assertFalse("report is not empty", report.isEmpty());

            final List<ErrorType> typeList = ve.getErrorTypeList();
            // check numbers of errors by type
            assertEquals("1 error of type context name due to bad name", 1,
                    Collections.frequency(typeList, ErrorType.CONTEXT_NAME));
            assertEquals("1 error of type context name gotten by type", 1, ve.getErrorsByType(ErrorType.CONTEXT_NAME)
                    .size());
            assertEquals("1 error of type context keys due to duplicate keys", 1,
                    Collections.frequency(typeList, ErrorType.CONTEXT_KEYS));
            assertEquals("1 error of type context key gotten by type", 1, ve.getErrorsByType(ErrorType.CONTEXT_KEYS)
                    .size());

            throw ve;
        }
    }

    /**
     * Tests successful creation of a {@link MetaConfiguration} instance using
     * {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)}, while setting each
     * (non-required) key to an empty value, one at a time.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testCreateMetaConfigurationEmptyValues() throws IOException, ConfigValidationException,
            RuntimeException {
        // gets the test configuration
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        for (final AbstractConfigKey currKey : testContext.getConfigKeys()) {
            if (currKey.isRequired() || (currKey == TEST_UNDEFINED_KEY)) {
                // cannot perform this test on required keys
                continue;
            }

            final Properties newConf = new Properties();
            newConf.putAll(testConfiguration);

            newConf.setProperty(testContext.getPropertyKey(currKey), "");

            final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(newConf);
            final MetaConfiguration result = MetaConfiguration.newConfiguration(inputStream, testContext);

            assertNotNull(result);
            assertTrue("result has registered context keys",
                    result.getConfigKeys().containsAll(testContext.getConfigKeys()));

            assertEquals(currKey.parseValue(""), currKey.getTypedValue(result));
        }
    }

    /**
     * Tests successful creation of a {@link MetaConfiguration} instance using
     * {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)}, while removing each
     * (non-required) key to verify substitution with the default value on construction.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testCreateMetaConfigurationDefaultValues() throws IOException, ConfigValidationException,
            RuntimeException {
        // gets the test configuration
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        for (final AbstractConfigKey currKey : testContext.getConfigKeys()) {
            if (currKey.isRequired() || (currKey == TEST_UNDEFINED_KEY)) {
                // cannot perform this test on required keys
                continue;
            }

            final Properties newConf = new Properties();
            newConf.putAll(testConfiguration);

            final String propertyKey = testContext.getPropertyKey(currKey);
            newConf.remove(propertyKey);
            assertNull(newConf.get(propertyKey));

            final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(newConf);
            final MetaConfiguration result = MetaConfiguration.newConfiguration(inputStream, testContext);

            assertNotNull(result);
            assertTrue("result has registered context keys",
                    result.getConfigKeys().containsAll(testContext.getConfigKeys()));

            assertEquals(currKey.getDefaultValue(), currKey.getTypedValue(result));
        }
    }

    /**
     * Tests successful retrieval of unmanaged configuration keys from a {@link MetaConfiguration}, using
     * {@link MetaConfiguration#getUnmanagedKeys()}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid. Considered a test failure.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testGetUnmanagedKeys() throws IOException, ConfigValidationException, RuntimeException {
        // initializes the configuration
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final String unmgtKey1 = "com.oodrive.nuage.java.utils.configuration.test.unmanaged1";
        final String unmgtKey2 = "com.oodrive.nuage.java.utils.configuration.test.unmanaged2";
        final String unmgtValue1 = "unmanaged value 1";
        final String unmgtValue2 = "unmanaged value 2";
        testConfiguration.setProperty(unmgtKey1, unmgtValue1);
        testConfiguration.setProperty(unmgtKey2, unmgtValue2);

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        // gets the configuration context
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);
        assertTrue("config has registered context keys", target.getConfigKeys()
                .containsAll(testContext.getConfigKeys()));

        final Properties result = target.getUnmanagedKeys();

        assertEquals("exactly 2 unmanaged keys", 2, result.size());
        assertTrue("unmanaged keys contains first key", result.containsKey(unmgtKey1));
        assertTrue("unmanaged keys contains second key", result.containsKey(unmgtKey2));
        assertEquals("unmanaged value for first key", unmgtValue1, result.getProperty(unmgtKey1));
        assertEquals("unmanaged value for second key", unmgtValue2, result.getProperty(unmgtKey2));

        // checks if short of being immutable, the read properties are preserved in the configuration
        result.setProperty(unmgtKey1, "");
        result.setProperty(unmgtKey2, "");

        final Properties newResult = target.getUnmanagedKeys();
        assertEquals("unmanaged value for first key", unmgtValue1, newResult.getProperty(unmgtKey1));
        assertEquals("unmanaged value for second key", unmgtValue2, newResult.getProperty(unmgtKey2));

    }

    /**
     * Tests successful copy with modifications using {@link MetaConfiguration#copyAndAlterConfiguration(java.util.Map)}
     * .
     * 
     * @throws ConfigValidationException
     *             if the copied configuration is invalid. Considered a test failure.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if copying the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testCopyAndAlterConfiguration() throws RuntimeException, IOException, ConfigValidationException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

        final HashMap<AbstractConfigKey, Object> modifyConfigMap = new HashMap<AbstractConfigKey, Object>();

        final Boolean oldBoolValue = TEST_BOOLEAN_KEY.getTypedValue(target);
        assertNotNull(oldBoolValue);
        final Boolean newBoolValue = new Boolean(!oldBoolValue.booleanValue());
        modifyConfigMap.put(TEST_BOOLEAN_KEY, newBoolValue);

        final EnumTestValue oldEnumValue = TEST_ENUM_KEY.getTypedValue(target);
        assertNotNull(oldEnumValue);
        final EnumTestValue newEnumValue = oldEnumValue.equals(EnumTestValue.TEST_VALUE_1) ? EnumTestValue.TEST_VALUE_2
                : EnumTestValue.TEST_VALUE_1;
        modifyConfigMap.put(TEST_ENUM_KEY, newEnumValue);

        final MetaConfiguration result = target.copyAndAlterConfiguration(modifyConfigMap);

        assertNotNull(result);
        assertEquals(newBoolValue, TEST_BOOLEAN_KEY.getTypedValue(result));
        assertEquals(newEnumValue, TEST_ENUM_KEY.getTypedValue(result));

        // check old configuration
        assertEquals(oldBoolValue, TEST_BOOLEAN_KEY.getTypedValue(target));
        assertEquals(oldEnumValue, TEST_ENUM_KEY.getTypedValue(target));

    }

    /**
     * Tests successful multiple copies with modifications using
     * {@link MetaConfiguration#copyAndAlterConfiguration(java.util.Map)} .
     * 
     * @throws ConfigValidationException
     *             if the copied configuration is invalid. Considered a test failure.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if copying the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testMultipleCopyAndAlterConfiguration() throws RuntimeException, IOException,
            ConfigValidationException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

        final Boolean oldBoolValue = TEST_BOOLEAN_KEY.getTypedValue(target);
        assertNotNull(oldBoolValue);
        final EnumTestValue oldEnumValue = TEST_ENUM_KEY.getTypedValue(target);
        assertNotNull(oldEnumValue);
        final File oldFileValue = TEST_FILE_KEY.getTypedValue(target);
        assertNotNull(oldFileValue);
        final Integer oldPosIntValue = TEST_POS_INT_KEY.getTypedValue(target);
        assertNotNull(oldPosIntValue);

        // construct the first alteration map, altering boolean and enum keys
        final HashMap<AbstractConfigKey, Object> firstModConfigMap = new HashMap<AbstractConfigKey, Object>();

        final Boolean newBoolValue = new Boolean(!oldBoolValue.booleanValue());
        firstModConfigMap.put(TEST_BOOLEAN_KEY, newBoolValue);

        final EnumTestValue newEnumValue = oldEnumValue.equals(EnumTestValue.TEST_VALUE_1) ? EnumTestValue.TEST_VALUE_2
                : EnumTestValue.TEST_VALUE_1;
        firstModConfigMap.put(TEST_ENUM_KEY, newEnumValue);

        // make a first copy and verify modifications
        final MetaConfiguration firstCopy = target.copyAndAlterConfiguration(firstModConfigMap);

        assertNotNull(firstCopy);
        assertEquals(newBoolValue, TEST_BOOLEAN_KEY.getTypedValue(firstCopy));
        assertEquals(newEnumValue, TEST_ENUM_KEY.getTypedValue(firstCopy));
        assertEquals(oldFileValue, TEST_FILE_KEY.getTypedValue(firstCopy));
        assertEquals(oldPosIntValue, TEST_POS_INT_KEY.getTypedValue(firstCopy));

        // check for side effects
        assertEquals(oldBoolValue, TEST_BOOLEAN_KEY.getTypedValue(target));
        assertEquals(oldEnumValue, TEST_ENUM_KEY.getTypedValue(target));
        assertEquals(oldFileValue, TEST_FILE_KEY.getTypedValue(target));
        assertEquals(oldPosIntValue, TEST_POS_INT_KEY.getTypedValue(target));

        // construct the second alteration map, altering file and positive integer keys
        final HashMap<AbstractConfigKey, Object> secondModConfigMap = new HashMap<AbstractConfigKey, Object>();

        final File newFileValue = new File(oldFileValue, "/newSubDir/");
        secondModConfigMap.put(TEST_FILE_KEY, newFileValue);

        final Integer newPosIntValue = Integer.valueOf(IntMath.checkedMultiply(
                Integer.signum(oldPosIntValue.intValue()), oldPosIntValue.intValue()));
        secondModConfigMap.put(TEST_POS_INT_KEY, newPosIntValue);

        // make a second copy and verify modfications
        final MetaConfiguration secondCopy = target.copyAndAlterConfiguration(secondModConfigMap);

        assertNotNull(secondCopy);
        assertEquals(newFileValue, TEST_FILE_KEY.getTypedValue(secondCopy));
        assertEquals(newPosIntValue, TEST_POS_INT_KEY.getTypedValue(secondCopy));
        assertEquals(oldBoolValue, TEST_BOOLEAN_KEY.getTypedValue(secondCopy));
        assertEquals(oldEnumValue, TEST_ENUM_KEY.getTypedValue(secondCopy));

        // check for side effects
        assertEquals(oldBoolValue, TEST_BOOLEAN_KEY.getTypedValue(secondCopy));
        assertEquals(oldEnumValue, TEST_ENUM_KEY.getTypedValue(secondCopy));
        assertEquals(oldFileValue, TEST_FILE_KEY.getTypedValue(target));
        assertEquals(oldPosIntValue, TEST_POS_INT_KEY.getTypedValue(target));

    }

    /**
     * Tests failure of copying with modifications using
     * {@link MetaConfiguration#copyAndAlterConfiguration(java.util.Map)} due to an invalid new value in the key-value
     * mapping.
     * 
     * @throws ConfigValidationException
     *             if the copied configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testCopyAndAlterConfigurationFailInvalidConfig() throws RuntimeException, IOException,
            ConfigValidationException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

        final HashMap<AbstractConfigKey, Object> modifyConfigMap = new HashMap<AbstractConfigKey, Object>();

        assertNotNull(TEST_POS_INT_KEY.getTypedValue(target));

        modifyConfigMap.put(TEST_POS_INT_KEY, new Integer(Integer.MIN_VALUE));

        target.copyAndAlterConfiguration(modifyConfigMap);
    }

    /**
     * Tests failure of copying with modifications using
     * {@link MetaConfiguration#copyAndAlterConfiguration(java.util.Map)} due to a null modification map argument.
     * 
     * @throws NullPointerException
     *             Expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public final void testCopyAndAlterConfigurationFail() throws RuntimeException, IOException,
            ConfigValidationException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

        target.copyAndAlterConfiguration(null);
    }

    /**
     * Tests failure of copying with modifications using
     * {@link MetaConfiguration#copyAndAlterConfiguration(java.util.Map)} due to an invalid new value in the key-value
     * mapping.
     * 
     * @throws IllegalArgumentException
     *             Expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCopyAndAlterConfigurationFailInvalidConfigKey() throws RuntimeException, IOException,
            ConfigValidationException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final MetaConfiguration target = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(testConfiguration), testContext);

        final HashMap<AbstractConfigKey, Object> modifyConfigMap = new HashMap<AbstractConfigKey, Object>();

        modifyConfigMap.put(new PositiveIntegerConfigKey("unknown.key", 2, true) {
            @Override
            protected final Integer getDefaultValue() {
                return null;
            }
        }, new Integer(1));

        target.copyAndAlterConfiguration(modifyConfigMap);
    }

    /**
     * Tests successful storage of a {@link MetaConfiguration} configuration using
     * {@link MetaConfiguration#storeConfiguration(java.io.OutputStream)}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid on construction. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testStoreMetaConfiguration() throws IOException, ConfigValidationException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);

        final Properties unmanagedKeys = target.getUnmanagedKeys();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        target.storeConfiguration(outputStream);

        assertTrue("output stream length greater than 0", outputStream.size() > 0);

        final Properties readBackConfig = new Properties();

        readBackConfig.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (final String currKey : testConfiguration.stringPropertyNames()) {
            final String refValue = testConfiguration.getProperty(currKey);
            assertTrue("key " + currKey + " contained in read back output", readBackConfig.containsKey(currKey));
            assertTrue("value " + refValue + " for key " + currKey + " contained in read back output",
                    readBackConfig.containsValue(refValue));
        }

        for (final String unmgtKey : unmanagedKeys.stringPropertyNames()) {
            final String refValue = testConfiguration.getProperty(unmgtKey);
            assertTrue("key " + unmgtKey + " contained in read back output", readBackConfig.containsKey(unmgtKey));
            assertTrue("value " + refValue + " for key " + unmgtKey + " contained in read back output",
                    readBackConfig.containsValue(refValue));
        }
    }

    /**
     * Tests failure to store a {@link MetaConfiguration} configuration using
     * {@link MetaConfiguration#storeConfiguration(java.io.OutputStream)}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid on construction. Not part of this test.
     * @throws IOException
     *             if writing the configuration fails. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test(expected = IOException.class)
    public final void testStoreMetaConfigurationFailIOException() throws IOException, ConfigValidationException,
            RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);

        final OutputStream outputStream = new OutputStream() {

            @Override
            public final void write(final int b) throws IOException {
                throw new IOException();
            }

            @Override
            public final void write(final byte[] b, final int off, final int len) throws IOException {
                throw new IOException();
            }
        };

        target.storeConfiguration(outputStream);
    }

    /**
     * Tests successful storage of a complete {@link MetaConfiguration} configuration using
     * {@link MetaConfiguration#storeCompleteConfiguration(java.io.OutputStream)}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid on construction. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testStoreCompleteMetaConfiguration() throws IOException, ConfigValidationException,
            RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        // removes one key that is not required and has a default value
        final String removedPropertyKey = testContext.getPropertyKey(TEST_FILE_KEY);
        testConfiguration.remove(removedPropertyKey);

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);

        final Properties unmanagedKeys = target.getUnmanagedKeys();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        target.storeCompleteConfiguration(outputStream);

        assertTrue("output stream length greater than 0", outputStream.size() > 0);

        final ByteArrayInputStream newInputStream = new ByteArrayInputStream(outputStream.toByteArray());

        final Properties readProperties = new Properties();

        readProperties.load(newInputStream);

        for (final String currKey : testConfiguration.stringPropertyNames()) {
            final String readValue = readProperties.getProperty(currKey);
            assertNotNull(readValue);
            if (currKey.equals(removedPropertyKey)) {
                assertEquals(TEST_FILE_KEY.valueToString(TEST_FILE_KEY.getDefaultValue()),
                        readProperties.getProperty(currKey));
                continue;
            }
            assertEquals(testConfiguration.getProperty(currKey), readProperties.getProperty(currKey));
        }

        for (final String unmgtKey : unmanagedKeys.stringPropertyNames()) {
            final String readValue = readProperties.getProperty(unmgtKey);
            assertNotNull(readValue);
            assertEquals(testConfiguration.getProperty(unmgtKey), readProperties.getProperty(unmgtKey));
        }

    }

    /**
     * Tests failure to store a complete {@link MetaConfiguration} configuration using
     * {@link MetaConfiguration#storeCompleteConfiguration(java.io.OutputStream)}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid on construction. Not part of this test.
     * @throws IOException
     *             if writing the configuration fails. Expected for this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test(expected = IOException.class)
    public final void testStoreCompleteMetaConfigurationFailIOException() throws IOException,
            ConfigValidationException, RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);

        final OutputStream outputStream = new OutputStream() {

            @Override
            public final void write(final int b) throws IOException {
                throw new IOException();
            }

            @Override
            public final void write(final byte[] b, final int off, final int len) throws IOException {
                throw new IOException();
            }
        };

        target.storeCompleteConfiguration(outputStream);
    }

    /**
     * Tests successful conversion of a complete {@link MetaConfiguration} configuration as a {@link Properties}
     * instance using {@link MetaConfiguration#getCompleteConfigurationAsProperties()}.
     * 
     * @throws ConfigValidationException
     *             if the configuration is invalid on construction. Not part of this test.
     * @throws IOException
     *             if reading or writing the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if construction of the {@link MetaConfiguration} fails. Considered a test failure.
     */
    @Test
    public final void testGetCompleteMetaConfigurationAsProperties() throws IOException, ConfigValidationException,
            RuntimeException {
        final Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();

        final ConfigTestContext testContext = ConfigTestContext.getInstance();

        // removes one key that is not required and has a default value
        final String removedPropertyKey = testContext.getPropertyKey(TEST_FILE_KEY);
        testConfiguration.remove(removedPropertyKey);

        final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);

        final MetaConfiguration target = MetaConfiguration.newConfiguration(inputStream, testContext);

        assertNotNull(target);

        final Properties unmanagedKeys = target.getUnmanagedKeys();

        final Properties resultProperties = target.getCompleteConfigurationAsProperties();

        for (final String currKey : testConfiguration.stringPropertyNames()) {
            final String readValue = resultProperties.getProperty(currKey);
            assertNotNull(readValue);
            if (currKey.equals(removedPropertyKey)) {
                assertEquals(TEST_FILE_KEY.valueToString(TEST_FILE_KEY.getDefaultValue()),
                        resultProperties.getProperty(currKey));
                continue;
            }
            assertEquals(testConfiguration.getProperty(currKey), resultProperties.getProperty(currKey));
        }

        for (final String unmgtKey : unmanagedKeys.stringPropertyNames()) {
            final String readValue = resultProperties.getProperty(unmgtKey);
            assertNotNull(readValue);
            assertEquals(testConfiguration.getProperty(unmgtKey), resultProperties.getProperty(unmgtKey));
        }

    }

    /**
     * Straight forward write of a configuration in files.
     * 
     * @throws IOException
     * @throws ConfigValidationException
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    @Test
    public final void testStoreFileMetaConfiguration() throws IOException, NullPointerException,
            IllegalArgumentException, ConfigValidationException {
        // Create configuration
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final Collection<AbstractConfigKey> testKeys = testContext.getConfigKeys();
        Properties testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
        final MetaConfiguration config1;
        {
            final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);
            config1 = MetaConfiguration.newConfiguration(inputStream, testContext);
            assertNotNull(config1);
        }

        // Allocate file names
        final File configFile = File.createTempFile("tst-metacfg", ".tmp");
        try {
            final File configFilePrev = File.createTempFile("tst-metacfg", ".bak.tmp");
            try {
                Assert.assertTrue(configFile.delete());
                Assert.assertTrue(configFilePrev.delete());

                // Write to first file
                config1.storeConfiguration(configFile, configFilePrev, true);
                Assert.assertTrue(configFile.isFile());
                Assert.assertFalse(configFilePrev.exists());

                // Check contents
                {
                    final MetaConfiguration readConfiguration = MetaConfiguration.newConfiguration(configFile,
                            testContext);
                    for (final AbstractConfigKey testKey : testKeys) {
                        Assert.assertEquals(testKey.getTypedValue(config1), testKey.getTypedValue(readConfiguration));
                    }
                }

                // Removes one key that is not required and has a default value
                testConfiguration.remove(testContext.getPropertyKey(TEST_FILE_KEY));
                final MetaConfiguration config2;
                {
                    final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);
                    config2 = MetaConfiguration.newConfiguration(inputStream, testContext);
                    assertNotNull(config2);
                }

                // Re-write configuration
                config2.storeConfiguration(configFile, configFilePrev, true);
                Assert.assertTrue(configFile.isFile());
                Assert.assertTrue(configFilePrev.isFile());

                // Check contents
                {// Current
                    final MetaConfiguration readConfiguration = MetaConfiguration.newConfiguration(configFile,
                            testContext);
                    for (final AbstractConfigKey testKey : testKeys) {
                        Assert.assertEquals(testKey.getTypedValue(config2), testKey.getTypedValue(readConfiguration));
                    }
                }
                {// Prev
                    final MetaConfiguration readConfiguration = MetaConfiguration.newConfiguration(configFilePrev,
                            testContext);
                    for (final AbstractConfigKey testKey : testKeys) {
                        Assert.assertEquals(testKey.getTypedValue(config1), testKey.getTypedValue(readConfiguration));
                    }
                }

                // Check cleanup of prev file: create a directory
                Assert.assertTrue(configFilePrev.delete());
                Assert.assertTrue(configFilePrev.mkdir());
                File.createTempFile("tst", ".tmp", configFilePrev);

                // Get another default configuration
                testConfiguration = ConfigTestHelper.getDefaultTestConfiguration();
                final MetaConfiguration config3;
                {
                    final InputStream inputStream = ConfigTestHelper.getPropertiesAsInputStream(testConfiguration);
                    config3 = MetaConfiguration.newConfiguration(inputStream, testContext);
                    assertNotNull(config3);
                }
                config3.storeConfiguration(configFile, configFilePrev, true);
                Assert.assertTrue(configFile.isFile());
                Assert.assertTrue(configFilePrev.isFile());

                // Check contents
                {// Current
                    final MetaConfiguration readConfiguration = MetaConfiguration.newConfiguration(configFile,
                            testContext);
                    for (final AbstractConfigKey testKey : testKeys) {
                        Assert.assertEquals(testKey.getTypedValue(config3), testKey.getTypedValue(readConfiguration));
                    }
                }
                {// Prev
                    final MetaConfiguration readConfiguration = MetaConfiguration.newConfiguration(configFilePrev,
                            testContext);
                    for (final AbstractConfigKey testKey : testKeys) {
                        Assert.assertEquals(testKey.getTypedValue(config2), testKey.getTypedValue(readConfiguration));
                    }
                }

            }
            finally {
                Files.deleteRecursive(configFilePrev.toPath());
            }
        }
        finally {
            configFile.delete();
        }
    }

    /**
     * Bad configuration context for testing against {@link ConfigTestContext}.
     * 
     * This context has
     * <ul>
     * <li>a {@link #getName() name} colliding with {@link ConfigTestContext}'s,</li>
     * <li>two bad keys with either null or empty {@link AbstractConfigKey#getName() names},</li>
     * <li>a common key {@link ConfigTestContext#TEST_ENUM_KEY} with {@link ConfigTestContext} and</li>
     * <li>a {@code null} key.</li>
     * </ul>
     * 
     * 
     * @see TestMetaConfiguration#testCreateMetaConfigurationFailInvalidConfigContext()
     * 
     */
    private static final class BadConfigContext extends AbstractConfigurationContext {

        private static final String BADLY_CHOSEN_NAME = "com.oodrive.nuage.java.utils.configuration.test.bad.choice";

        /**
         * Default constructor creating the faulty instance without violating constraints.
         */
        protected BadConfigContext() {
            super(BADLY_CHOSEN_NAME, new AbstractConfigKey[] { null, ConfigTestContext.TEST_ENUM_KEY,
                    badConfigKeyEmptyName, badConfigKeyNullName });
        }

    }

    private static final PositiveIntegerConfigKey badConfigKeyEmptyName = new PositiveIntegerConfigKey("",
            Integer.MAX_VALUE, true) {

        @Override
        public final Integer getDefaultValue() {
            return null;
        }
    };
    private static final PositiveIntegerConfigKey badConfigKeyNullName = new PositiveIntegerConfigKey(null,
            Integer.MAX_VALUE, true) {

        @Override
        public final Integer getDefaultValue() {
            return null;
        }
    };

}
