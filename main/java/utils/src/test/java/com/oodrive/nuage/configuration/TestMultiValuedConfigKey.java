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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;

import com.oodrive.nuage.configuration.ValidConfigurationContext.ContextTestHelper;

/**
 * Tests for the method implemented by the abstract {@link MultiValuedConfigKey} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestMultiValuedConfigKey extends TestAbstractConfigKeys {

    /**
     * Basic implementation of {@link MultiValuedConfigKey} for testing.
     * 
     * 
     */
    private static final class TestableMultiValuedConfigKey extends MultiValuedConfigKey<ArrayList<String>, String> {

        private final boolean required;

        private final boolean hasDefault;

        /**
         * Utility constructor.
         * 
         * @param separator
         *            see {@link MultiValuedConfigKey#MultiValuedConfigKey(String, String, Class, Class)}
         * @param collectionType
         *            see {@link MultiValuedConfigKey#MultiValuedConfigKey(String, String, Class, Class)}
         * @param itemType
         *            see {@link MultiValuedConfigKey#MultiValuedConfigKey(String, String, Class, Class)}
         */
        public TestableMultiValuedConfigKey(final boolean required, final boolean hasDefault, final String separator,
                final Class<ArrayList<String>> collectionType, final Class<String> itemType) {
            super("test.multivalued.key", separator, collectionType, itemType);
            this.required = required;
            this.hasDefault = hasDefault;
        }

        @Override
        protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
            if (value == null) {
                return "";
            }
            if (value instanceof ArrayList) {
                final String stringList = String.valueOf(value);
                return stringList.substring(1, stringList.length() - 1).replace(", ", getSeparator());
            }
            else {
                throw new IllegalArgumentException("Not an ArrayList");
            }
        }

        @Override
        protected final ArrayList<String> getDefaultValue() {
            return hasDefault ? new ArrayList<String>(Arrays.asList(new String[] { "element1", "element2" })) : null;
        }

        @Override
        public final boolean isRequired() {
            return required;
        }

        @Override
        protected final ArrayList<String> getCollectionFromValueList(final ArrayList<String> values) {
            return values;
        }

        @Override
        protected final String getItemValueFromString(final String value) {
            return value;
        }

        @Override
        protected final ArrayList<String> makeDefensiveCopy(final ArrayList<String> value) {
            return new ArrayList<String>(value);
        }

        @Override
        protected final ValidationError performAdditionalValueChecks(final ArrayList<String> value) {
            return ValidationError.NO_ERROR;
        }

    };

    @SuppressWarnings("unchecked")
    private static final Class<ArrayList<String>> TEST_COLLECTION_CLASS = (Class<ArrayList<String>>) new ArrayList<String>()
            .getClass();

    /**
     * Test successful creation of a {@link MultiValuedConfigKey}.
     */
    @Test
    public final void testCreateMultiValuedConfigKey() {
        final TestableMultiValuedConfigKey result = new TestableMultiValuedConfigKey(false, false, ":",
                TEST_COLLECTION_CLASS, String.class);
        assertNotNull(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMultiValuedConfigKeyFailNullSeparator() {
        new TestableMultiValuedConfigKey(false, false, null, TEST_COLLECTION_CLASS, String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testCreateMultiValuedConfigKeyFailEmptySeparator() {
        new TestableMultiValuedConfigKey(false, false, "", TEST_COLLECTION_CLASS, String.class);
    }

    @Test(expected = NullPointerException.class)
    public final void testCreateMultiValuedConfigKeyFailNullCollectionClass() {
        new TestableMultiValuedConfigKey(false, false, ":", null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public final void testCreateMultiValuedConfigKeyFailNullItemClass() {
        new TestableMultiValuedConfigKey(false, false, ":", TEST_COLLECTION_CLASS, null);
    }

    /**
     * Test failure to modify a value within a {@link MetaConfiguration} for a {@link MultiValuedConfigKey}.
     */
    @Test
    public final void testImmutableConfigurationValue() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {
        final TestableMultiValuedConfigKey targetKey = new TestableMultiValuedConfigKey(false, true, ":",
                TEST_COLLECTION_CLASS, String.class);
        final AbstractConfigurationContext targetContext = new AbstractConfigurationContext("test.context", targetKey) {
        };

        final ArrayList<String> defaultValue = targetKey.getDefaultValue();
        final Properties configProps = new Properties();
        configProps.setProperty(targetContext.getPropertyKey(targetKey), targetKey.valueToString(defaultValue));

        final MetaConfiguration targetConfig = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(configProps), targetContext);

        final ArrayList<String> modifiedValue = targetKey.getTypedValue(targetConfig);

        // alters the value
        modifiedValue.clear();

        // re-reads the value for comparison
        final ArrayList<String> originalValue = targetKey.getTypedValue(targetConfig);

        for (final String currValue : defaultValue) {
            assertEquals(currValue, originalValue.get(defaultValue.indexOf(currValue)));
        }

    }

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        final TestableMultiValuedConfigKey result = new TestableMultiValuedConfigKey(required, hasDefault, ":",
                TEST_COLLECTION_CLASS, String.class);

        return result;
    }

}
