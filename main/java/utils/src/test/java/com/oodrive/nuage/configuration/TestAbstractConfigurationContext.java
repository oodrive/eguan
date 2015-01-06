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
import static com.oodrive.nuage.configuration.EnumTestValue.TEST_VALUE_1;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Tests methods of {@link AbstractConfigurationContext} not covered by {@link TestMetaConfiguration}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestAbstractConfigurationContext {

    /**
     * Valid {@link AbstractConfigurationContext} names to test with
     * {@link TestAbstractConfigurationContext#testIsValidName()}.
     */
    private static final String[] VALID_CONTEXT_NAMES = new String[] { "c", "com", "com.oodrive", "com.oodrive.nuage",
            "com.oodrive.nuage.c.configuration", "com.1oodrive.2nuage" };

    /**
     * Invalid {@link AbstractConfigurationContext} names to test with
     * {@link TestAbstractConfigurationContext#testIsValidNameFailValidation()}.
     */
    private static final String[] INVALID_CONTEXT_NAMES = new String[] { null, "", " ", ".", "COM",
            "com..oodrive.nuage", "com.oodrive.nuage.", "com.oodrive.nuage..", "com.OODRIVE.nuage",
            "com.oodrive?.nuage" };

    /**
     * Tests the failure on creating an {@link AbstractConfigurationContext} with an empty name.
     * 
     * @throws IllegalArgumentException
     *             if the name is {@code null} or empty. Expected for this test.
     * @throws NullPointerException
     *             if the context list is {@code null}. Not part of this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateAbstractConfigurationContextFailNameless() throws IllegalArgumentException,
            NullPointerException {
        /*
         * calls the constructor with bad arguments, the config key list being intentionally null, so there'd be a
         * NullPointerException before an IllegalArgumentException that could be mistaken for the expected exception.
         */
        new AbstractConfigurationContext("", (AbstractConfigKey) null) {
        };
    }

    /**
     * Tests the failure on creating an {@link AbstractConfigurationContext} with an empty context list.
     * 
     * @throws IllegalArgumentException
     *             if the context list is empty. Expected for this test.
     * @throws NullPointerException
     *             if the context list is {@code null}. Not part of this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateAbstractConfigurationContextFailNoContext() throws IllegalArgumentException,
            NullPointerException {
        new AbstractConfigurationContext("com.oodrive.nuage.java.utils.config.test", new AbstractConfigKey[] {}) {
        };
    }

    /**
     * Tests the failure on an {@link AbstractConfigurationContext} with an empty context list.
     * 
     * @throws IllegalArgumentException
     *             if the context list is empty. Expected for this test.
     * @throws NullPointerException
     *             if the context list is {@code null}. Not part of this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testGetPropertyKeyFailNotManaged() throws IllegalArgumentException, NullPointerException {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        testContext.getPropertyKey(new FileConfigKey("unknown.name", false, false, false) {

            @Override
            protected Object getDefaultValue() {
                return null;
            }
        });
    }

    /**
     * Test successful validation of names with {@link AbstractConfigurationContext#isNameValid(String)}.
     */
    @Test
    public final void testIsValidName() {
        for (final String currName : VALID_CONTEXT_NAMES) {
            assertTrue("Name is valid; name='" + currName + "'", AbstractConfigurationContext.isNameValid(currName));
        }
    }

    /**
     * Test validation failure calling {@link AbstractConfigurationContext#isNameValid(String)} with empty,{@code null}
     * or otherwise invalid names.
     */
    @Test
    public final void testIsValidNameFailValidation() {
        for (final String currName : INVALID_CONTEXT_NAMES) {
            assertFalse("Name is invalid; name='" + currName + "'", AbstractConfigurationContext.isNameValid(currName));
        }
    }

    /**
     * Tests overriding {@link AbstractConfigurationContext#validateConfiguration(MetaConfiguration)} for additional
     * constraints on values.
     * 
     * @throws ConfigValidationException
     *             if the overridden validation generates a {@link ValidationError}. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testOverrideValidateConfiguration() throws RuntimeException, IOException,
            ConfigValidationException {

        final AbstractConfigurationContext target = new AbstractConfigurationContext(ConfigTestContext.NAME,
                TEST_BOOLEAN_KEY, TEST_ENUM_KEY) {
            @Override
            public final List<ValidationError> validateConfiguration(final MetaConfiguration configuration) {
                final List<ValidationError> result = super.validateConfiguration(configuration);

                final Boolean boolValue = TEST_BOOLEAN_KEY.getTypedValue(configuration);
                final EnumTestValue enumValue = TEST_ENUM_KEY.getTypedValue(configuration);

                if (!boolValue.booleanValue() && enumValue.equals(TEST_VALUE_1)) {
                    result.add(new ValidationError(ErrorType.VALUE_INVALID, this, TEST_ENUM_KEY, enumValue,
                            "cannot be a " + TEST_VALUE_1 + " if " + TEST_BOOLEAN_KEY.getName() + " is false"));
                }
                return result;
            }
        };
        MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(ConfigTestHelper.getDefaultTestConfiguration()), target);
    }
}
