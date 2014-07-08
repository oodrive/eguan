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

import org.junit.Test;

import com.oodrive.nuage.configuration.IntegerConfigKey.PositiveIntegerConfigKey;
import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Implementation of {@link TestAbstractConfigKeys} for testing {@link IntegerConfigKey}s.
 * 
 * This test suite adds some test specific to verifying upper and lower limit values.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestIntegerConfigKey extends TestAbstractConfigKeys {

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        return new IntegerConfigKey("test.integer.key", Integer.MIN_VALUE, Integer.MAX_VALUE) {

            @Override
            public Integer getDefaultValue() {
                return hasDefault ? Integer.valueOf(0) : null;
            }

            @Override
            public boolean isRequired() {
                return required;
            }
        };
    }

    /**
     * Tests failure to construct a {@link IntegerConfigKey.PositiveIntegerConfigKey} due to a negative upper limit.
     * 
     * @throws IllegalArgumentException
     *             if construction fails due to invalid limits. Expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreatePositiveIntegerConfigKeyFailNegativeUpperLimit() throws IllegalArgumentException {
        new PositiveIntegerConfigKey("test.positive.integer.key", -1, false) {

            @Override
            public final Integer getDefaultValue() {
                return null;
            }
        };
    }

    /**
     * Tests behavior of the derived {@link IntegerConfigKey.PositiveIntegerConfigKey#checkValue(Object)}, with and
     * without the strict parameter set to {@code true}.
     */
    @Test
    public final void testPositiveIntegerConfigKeyCheckValue() {
        final PositiveIntegerConfigKey strictTarget = new PositiveIntegerConfigKey("test.strict.positive.integer.key",
                Integer.MAX_VALUE, true) {

            @Override
            public final Integer getDefaultValue() {
                return null;
            }
        };
        assertEquals("key does accept positive values", ValidationError.NO_ERROR,
                strictTarget.checkValue(Integer.valueOf(1)));
        assertEquals("key does not accept negative values", ErrorType.VALUE_INVALID,
                strictTarget.checkValue(Integer.valueOf(-1)).getType());
        assertEquals("strict key does not accept zero", ErrorType.VALUE_INVALID,
                strictTarget.checkValue(Integer.valueOf(0)).getType());

        final PositiveIntegerConfigKey nonstrictTarget = new PositiveIntegerConfigKey(
                "test.nonstrict.positive.integer.key", Integer.MAX_VALUE, false) {

            @Override
            public final Integer getDefaultValue() {
                return null;
            }
        };
        assertEquals("key does accept positive values", ValidationError.NO_ERROR,
                nonstrictTarget.checkValue(Integer.valueOf(1)));
        assertEquals("key does not accept negative values", ErrorType.VALUE_INVALID,
                nonstrictTarget.checkValue(Integer.valueOf(-1)).getType());
        assertEquals("non strict key does accept zero", ValidationError.NO_ERROR,
                nonstrictTarget.checkValue(Integer.valueOf(0)));
    }

}
