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

import java.util.Objects;

/**
 * {@link BoundedConfigKey} implementation capable of handling values of {@link Integer} type.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
public abstract class IntegerConfigKey extends BoundedConfigKey<Integer> {

    /**
     * A subclass of {@link IntegerConfigKey} for positive integer keys.
     * 
     */
    public abstract static class PositiveIntegerConfigKey extends IntegerConfigKey {

        /**
         * Constructs an instance for positive integer values.
         * 
         * @param name
         *            the unique name to set
         * @param maxLimit
         *            the maximum limit, must be less or equal than {@link Integer#MAX_VALUE}
         * @param strict
         *            whether to exclude 0 (strict, i.e. {@code true}) or not (non-strict, i.e. {@code false})
         * @throws IllegalArgumentException
         *             if the maximum limit is negative or zero (depending on strict value)
         */
        protected PositiveIntegerConfigKey(final String name, final int maxLimit, final boolean strict)
                throws IllegalArgumentException {
            super(name, strict ? 1 : 0, maxLimit);
        };
    }

    /**
     * Constructs an instance which limits its possible values to a range between the provided lower and upper bounds.
     * 
     * The limits are enforced when calling the {@link #checkValue(Object)} method.
     * 
     * @param name
     *            the unique name, passed to {@link AbstractConfigKey#AbstractConfigKey(String)}
     * @param minLimit
     *            the lower (inclusive) limit for values
     * @param maxLimit
     *            the upper (inclusive) limit for values
     * @throws IllegalArgumentException
     *             if the lower limit is inferior to the upper limit
     */
    protected IntegerConfigKey(final String name, final int minLimit, final int maxLimit)
            throws IllegalArgumentException {
        super(name, Integer.class, Integer.valueOf(minLimit), Integer.valueOf(maxLimit));
    }

    @Override
    public final Integer getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return (Integer) configuration.getValue(this);
    }

    @Override
    public final Integer parseValue(final String value) {
        if (value.isEmpty()) {
            return null;
        }
        return Integer.valueOf(Objects.requireNonNull(value));
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }
        if (value instanceof Integer) {
            return ((Integer) value).toString();
        }
        else {
            throw new IllegalArgumentException("Not an Integer");
        }
    }
}
