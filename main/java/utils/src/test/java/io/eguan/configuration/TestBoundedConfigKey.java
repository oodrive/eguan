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
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.BoundedConfigKey;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.ValidationError.ErrorType;

import java.util.Objects;

import org.junit.Test;

/**
 * Test class for methods implemented by the abstract {@link BoundedConfigKey}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestBoundedConfigKey extends TestAbstractConfigKeys {

    /**
     * {@link Comparable} implementation for testing.
     * 
     * 
     */
    private static final class TestComparable implements Comparable<TestComparable> {

        private final int value;

        /**
         * Constructs a {@link TestComparable}.
         * 
         * @param value
         *            the value to {@link #compareTo(TestComparable) compare} against.
         */
        private TestComparable(final int value) {
            this.value = value;
        }

        @Override
        public final int compareTo(final TestComparable o) {
            return Integer.compare(value, o.value);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof TestComparable)) {
                return false;
            }
            return compareTo((TestComparable) obj) == 0;
        }

        @Override
        public final String toString() {
            return String.valueOf(value);
        }
    };

    /**
     * Minimal implementation of {@link BoundedConfigKey} binding to {@link TestComparable} and getting some mandatory
     * method implementations out of the way.
     * 
     * 
     */
    private static class TestableBoundedConfigKey extends BoundedConfigKey<TestComparable> {

        private TestComparable defaultValue;

        private boolean required;

        /**
         * Constructs a {@link TestableBoundedConfigKey}.
         * 
         * @param minLimit
         *            the min limit to apply
         * @param maxLimit
         *            the max limit to apply
         * @throws IllegalArgumentException
         *             if the maximum limit is below the minimum limit
         */
        protected TestableBoundedConfigKey(final TestComparable minLimit, final TestComparable maxLimit)
                throws IllegalArgumentException {
            super("test.bounded.config.key", TestComparable.class, minLimit, maxLimit);
        }

        @Override
        public final TestComparable getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
                ClassCastException, NullPointerException {
            Objects.requireNonNull(configuration);
            checkConfigForKey(configuration);
            return (TestComparable) configuration.getValue(this);
        }

        @Override
        protected final TestComparable parseValue(final String value) throws IllegalArgumentException,
                NullPointerException {
            if (value.isEmpty()) {
                // Returns the default value or null when there is no default value
                final TestComparable defaultValue = (TestComparable) getDefaultValue();
                return defaultValue;
            }
            return new TestComparable(Integer.parseInt(Objects.requireNonNull(value)));
        }

        @Override
        protected final String valueToString(final Object value) throws IllegalArgumentException {
            if (value == null) {
                return "";
            }
            if (value instanceof TestComparable) {
                return ((TestComparable) value).toString();
            }
            else {
                throw new IllegalArgumentException("Not a TestComparable");
            }
        }

        @Override
        protected final TestComparable getDefaultValue() {
            return defaultValue;
        }

        @Override
        public final boolean isRequired() {
            return required;
        }

    };

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        final TestableBoundedConfigKey result = new TestableBoundedConfigKey(new TestComparable(Integer.MIN_VALUE),
                new TestComparable(Integer.MAX_VALUE));
        result.defaultValue = hasDefault ? new TestComparable(0) : null;
        result.required = required;
        return result;
    }

    /**
     * Tests failure of constructing a {@link BoundedConfigKey} due to invalid upper and lower limits.
     * 
     * 
     * @throws IllegalArgumentException
     *             if the lower limit is above the upper limit. Expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateBoundedConfigKeyFailMinBelowMax() throws IllegalArgumentException {
        new TestableBoundedConfigKey(new TestComparable(Integer.MAX_VALUE), new TestComparable(Integer.MIN_VALUE));
    }

    /**
     * Tests failure to validate a value given to {@link BoundedConfigKey#checkValue(Object)} due to values below the
     * lower or above the upper limit.
     */
    @Test
    public final void testCreateBoundedConfigKeyFailOutOfRange() {
        final TestableBoundedConfigKey target = new TestableBoundedConfigKey(new TestComparable(Integer.MIN_VALUE + 1),
                new TestComparable(Integer.MAX_VALUE - 1));

        final ValidationError resultLower = target.checkValue(new TestComparable(Integer.MIN_VALUE));
        assertEquals("validation error is of expected type", resultLower.getType(), ErrorType.VALUE_INVALID);
        assertTrue("validation error message is 'out of range'",
                resultLower.getErrorMessage().startsWith("out of range"));

        final ValidationError resultUpper = target.checkValue(new TestComparable(Integer.MAX_VALUE));
        assertEquals("validation error is of expected type", resultUpper.getType(), ErrorType.VALUE_INVALID);
        assertTrue("validation error message is 'out of range'",
                resultUpper.getErrorMessage().startsWith("out of range"));

    }

}
