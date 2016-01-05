package io.eguan.configuration;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.configuration.ValidationError.ErrorType;

import java.util.Objects;

/**
 * Abstract {@link AbstractConfigKey} implementation enforcing minimum and maximum limits on any {@link Comparable}
 * object.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 * @param <T>
 *            the {@link Comparable} object type to enforce bounds on
 */
public abstract class BoundedConfigKey<T extends Comparable<T>> extends AbstractConfigKey {

    /**
     * The minimum inclusive limit to enforce.
     */
    private final T minLimit;

    /**
     * The maximum inclusive limit to enforce.
     */
    private final T maxLimit;

    /**
     * The {@link Comparable} value type to accept.
     */
    private final Class<T> type;

    /**
     * Constructs a {@link BoundedConfigKey} applying limit to values of the given type.
     * 
     * @param name
     *            the {@link AbstractConfigKey#getName() name}
     * @param type
     *            the {@link Comparable} type to bind to
     * @param minLimit
     *            the minimum acceptable value (inclusive)
     * @param maxLimit
     *            the maximum acceptable value (inclusive)
     * @throws IllegalArgumentException
     *             if the lower limit it greater than the upper limit
     */
    protected BoundedConfigKey(final String name, final Class<T> type, final T minLimit, final T maxLimit)
            throws IllegalArgumentException {
        super(name);

        this.minLimit = Objects.requireNonNull(minLimit);
        this.maxLimit = Objects.requireNonNull(maxLimit);
        if (maxLimit.compareTo(minLimit) < 0) {
            throw new IllegalArgumentException("Max limit below min limit; max=" + maxLimit + " min=" + minLimit);
        }

        this.type = Objects.requireNonNull(type);
    }

    @Override
    protected final ValidationError checkValue(final Object value) {
        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }

        try {
            final Comparable<T> castValue = type.cast(value);
            if ((castValue.compareTo(minLimit) >= 0) && (castValue.compareTo(maxLimit) <= 0)) {
                return ValidationError.NO_ERROR;
            }
            else {
                return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, "out of range; min=" + minLimit
                        + ", max=" + maxLimit);
            }
        }
        catch (final ClassCastException ce) {
            return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, ce.getMessage());
        }
    }

}
