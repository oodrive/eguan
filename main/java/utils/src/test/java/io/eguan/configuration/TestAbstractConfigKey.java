package io.eguan.configuration;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.ValidationError.ErrorType;

import java.io.IOException;
import java.util.AbstractList;

import org.junit.Test;

/**
 * Tests the methods directly implemented by {@link AbstractConfigKey} not covered by {@link TestMetaConfiguration}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestAbstractConfigKey {

    /**
     * Valid {@link AbstractConfigKey} names to test with {@link TestAbstractConfigKey#testIsValidName()}.
     */
    private static final String[] VALID_CONFIG_KEY_NAMES = new String[] { "k", "key", "test.key", "test.key.eguan",
            "test.key.eguan.k.valid", "test.Key.EGUAN", "test.Key2.EGUAN3" };

    /**
     * Invalid {@link AbstractConfigKey} names to test with
     * {@link TestAbstractConfigKey#testIsValidNameFailValidation()}.
     */
    private static final String[] INVALID_CONFIG_KEY_NAMES = new String[] { null, "", " ", ".", "test..key",
            "test.invalid.key.", "test.invalid.key..", "test.invalid.key.#" };

    /**
     * An abstract subclass of {@link AbstractConfigKey} implementing the methods not relevant to the tests.
     * 
     * 
     */
    private abstract static class TestableAbstractConfigKey extends AbstractConfigKey {

        /**
         * The default value to be returned by {@link #getDefaultValue()}.
         */
        private final Object defaultValue;

        /**
         * Default constructor with a fail-safe name value.
         * 
         * @param defaultValue
         *            the default value to be returned by {@link #getDefaultValue()}
         */
        public TestableAbstractConfigKey(final Object defaultValue) {
            super("testable.key");
            this.defaultValue = defaultValue;
        }

        @Override
        protected final Object getDefaultValue() {
            return defaultValue;
        }

        @Override
        public final Object getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
                ClassCastException, NullPointerException {
            return null;
        }

        @Override
        protected final Object parseValue(final String value) throws IllegalArgumentException, NullPointerException {
            return null;
        }

        @Override
        protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
            return null;
        }

        // intentionally non-final
        @Override
        protected ValidationError checkValue(final Object value) {
            return null;
        }
    }

    /**
     * {@link TestableAbstractConfigKey} implementation with an exact class-checking
     * {@link AbstractConfigKey#checkValue(Object)} implementation based on
     * {@link AbstractConfigKey#checkSameClass(Object, Class)}.
     * 
     * 
     */
    private static final class TestableClassCheckingConfigKey extends TestableAbstractConfigKey {

        private final Class<?> checkClass;

        public TestableClassCheckingConfigKey(final Class<?> classToCheckAgainst) {
            super(null);
            checkClass = classToCheckAgainst;
        }

        @Override
        protected final ValidationError checkValue(final Object value) {
            return checkSameClass(value, checkClass);
        }
    }

    /**
     * Tests consistency of the default implementation where a key is required if it has a default value.
     */
    @Test
    public final void testRequiredIfDefault() {
        final TestableAbstractConfigKey targetNoDefault = new TestableAbstractConfigKey(null) {
        };
        assertFalse("key claims to have no default value", targetNoDefault.hasDefaultValue());
        assertTrue("key is required", targetNoDefault.isRequired());

        final TestableAbstractConfigKey targetWithDefault = new TestableAbstractConfigKey("default value") {
        };
        assertTrue("key claims to have a default value", targetWithDefault.hasDefaultValue());
        assertFalse("key is not required", targetWithDefault.isRequired());
    }

    /**
     * Tests consistency of the default implementation with inverted required return value.
     */
    @Test
    public void testhasDefaultWithOverriddenRequired() {
        final TestableAbstractConfigKey targetDefaultRequired = new TestableAbstractConfigKey("default value") {
            @Override
            public boolean isRequired() {
                return !super.isRequired();
            }
        };
        assertFalse("key claims to have no default value", targetDefaultRequired.hasDefaultValue());
        assertTrue("key is required", targetDefaultRequired.isRequired());

        final TestableAbstractConfigKey targetNoDefaultNotRequired = new TestableAbstractConfigKey(null) {

            @Override
            public boolean isRequired() {
                return !super.isRequired();
            }

        };
        assertFalse("key claims to have no default value", targetNoDefaultNotRequired.hasDefaultValue());
        assertFalse("key is not required", targetNoDefaultNotRequired.isRequired());
    }

    /**
     * Tests failure when calling {@link AbstractConfigKey#checkConfigForKey(MetaConfiguration)} with a configuration
     * not managing the key.
     * 
     * @throws IllegalStateException
     *             the exception thrown on a key unknown to the configuration. Expected in this test.
     * @throws IOException
     *             if loading the configuration fails. Not part of this test.
     * @throws ConfigValidationException
     *             if validation of the configuration fails. Not part of this test.
     * @throws RuntimeException
     *             if parameters are {@code null} or construction otherwise fails. Not part of this test.
     */
    @Test(expected = IllegalStateException.class)
    public void testCheckConfigForKeyFail() throws IllegalStateException, IOException, ConfigValidationException,
            RuntimeException {
        final MetaConfiguration config = MetaConfiguration.newConfiguration(
                ConfigTestHelper.getPropertiesAsInputStream(ConfigTestHelper.getDefaultTestConfiguration()),
                ConfigTestContext.getInstance());
        final TestableAbstractConfigKey target = new TestableAbstractConfigKey(null) {
        };
        target.checkConfigForKey(config);
    }

    /**
     * Test successful validation of names with {@link AbstractConfigKey#isNameValid(String)}.
     */
    @Test
    public void testIsValidName() {
        for (final String currName : VALID_CONFIG_KEY_NAMES) {
            assertTrue("Name is valid; name='" + currName + "'", AbstractConfigKey.isNameValid(currName));
        }
    }

    /**
     * Test validation failure calling {@link AbstractConfigKey#isNameValid(String)} with empty,{@code null} or
     * otherwise invalid names.
     */
    @Test
    public void testIsValidNameFailValidation() {
        for (final String currName : INVALID_CONFIG_KEY_NAMES) {
            assertFalse("Name is invalid; name='" + currName + "'", AbstractConfigKey.isNameValid(currName));
        }
    }

    /**
     * Indirectly tests {@link AbstractConfigKey#checkSameClass(Object, Class)} through
     * {@link TestableClassCheckingConfigKey} given a subclass instance as argument.
     */
    @Test
    public void testCheckValueExactClassFailNotExactClass() {
        final TestableAbstractConfigKey target = new TestableClassCheckingConfigKey(AbstractList.class);

        final AbstractList<Object> listInstance = new AbstractList<Object>() {

            @Override
            public Object get(final int index) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }
        };

        final ValidationError report = target.checkValue(listInstance);

        assertEquals(ErrorType.VALUE_INVALID, report.getType());
    }

    /**
     * Indirectly tests {@link AbstractConfigKey#checkSameClass(Object, Class)} through
     * {@link TestableClassCheckingConfigKey} given an enum constant as argument.
     */
    @Test
    public void testCheckValueExactClassWithEnum() {
        final TestableAbstractConfigKey target = new TestableClassCheckingConfigKey(EnumTestValue.class);

        final ValidationError report = target.checkValue(EnumTestValue.TEST_VALUE_2);

        assertEquals(ValidationError.NO_ERROR, report);
    }

}
