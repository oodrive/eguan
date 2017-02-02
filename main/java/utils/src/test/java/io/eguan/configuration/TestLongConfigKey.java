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
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.LongConfigKey;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.LongConfigKey.PositiveLongConfigKey;
import io.eguan.configuration.ValidationError.ErrorType;

import org.junit.Test;

/**
 * Implementation of {@link TestAbstractConfigKeys} for testing {@link LongConfigKey}s.
 * 
 * This test suite adds some test specific to verifying upper and lower limit values.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestLongConfigKey extends TestAbstractConfigKeys {

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        return new LongConfigKey("test.long.key", Long.MIN_VALUE, Long.MAX_VALUE) {

            @Override
            public final Long getDefaultValue() {
                return hasDefault ? Long.valueOf(0) : null;
            }

            @Override
            public final boolean isRequired() {
                return required;
            }
        };
    }

    /**
     * Tests failure to construct a {@link LongConfigKey.PositiveLongConfigKey} due to a negative upper limit.
     * 
     * @throws IllegalArgumentException
     *             if construction fails due to invalid limits. Expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreatePositiveLongConfigKeyFailNegativeUpperLimit() throws IllegalArgumentException {
        new PositiveLongConfigKey("test.positive.long.key", -1, false) {

            @Override
            public final Long getDefaultValue() {
                return null;
            }
        };
    }

    /**
     * Tests behavior of the derived {@link LongConfigKey.PositiveLongConfigKey#checkValue(Object)}, with and without
     * the strict parameter set to {@code true}.
     */
    @Test
    public final void testPositiveLongConfigKeyCheckValue() {
        final PositiveLongConfigKey strictTarget = new PositiveLongConfigKey("test.strict.positive.long.key",
                Long.MAX_VALUE, true) {

            @Override
            public final Long getDefaultValue() {
                return null;
            }
        };
        assertEquals("key does accept positive values", ValidationError.NO_ERROR,
                strictTarget.checkValue(Long.valueOf(1)));
        assertEquals("key does not accept negative values", ErrorType.VALUE_INVALID,
                strictTarget.checkValue(Long.valueOf(-1)).getType());
        assertEquals("strict key does not accept zero", ErrorType.VALUE_INVALID,
                strictTarget.checkValue(Long.valueOf(0)).getType());

        final PositiveLongConfigKey nonstrictTarget = new PositiveLongConfigKey("test.nonstrict.positive.long.key",
                Long.MAX_VALUE, false) {

            @Override
            public final Long getDefaultValue() {
                return null;
            }
        };
        assertEquals("key does accept positive values", ValidationError.NO_ERROR,
                nonstrictTarget.checkValue(Long.valueOf(1)));
        assertEquals("key does not accept negative values", ErrorType.VALUE_INVALID,
                nonstrictTarget.checkValue(Long.valueOf(-1)).getType());
        assertEquals("non strict key does accept zero", ValidationError.NO_ERROR,
                nonstrictTarget.checkValue(Long.valueOf(0)));
    }

}
