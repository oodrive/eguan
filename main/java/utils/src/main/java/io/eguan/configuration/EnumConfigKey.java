package io.eguan.configuration;

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

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * {@link ConfigKey} implementation taking the constants of one enum type as valid values.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 * @param <E>
 *            the {@link Enum} type whose constants are accepted as valid values
 */
public abstract class EnumConfigKey<E extends Enum<E>> extends AbstractConfigKey {

    /**
     * The target {@link Enum} class.
     */
    private final Class<E> enumClass;

    /**
     * Constructs a key capable of validating all constants of the bound enum type.
     * 
     * Providing the generic {@link Enum} type as argument is necessary as type erasure prevents runtime access to the
     * bound generic type.
     * 
     * @param name
     *            the unique name of the configuration key
     * @param enumType
     *            E's class object, never {@code null}
     * @throws NullPointerException
     *             if the provided enum type is {@code null}
     */
    protected EnumConfigKey(@Nonnull final String name, @Nonnull final Class<E> enumType) throws NullPointerException {
        super(name);
        this.enumClass = Objects.requireNonNull(enumType);
    }

    @Override
    public final E getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        // casts the result explicitly
        return enumClass.cast(configuration.getValue(this));
    }

    @Override
    protected final E parseValue(final String value) throws IllegalArgumentException, NullPointerException {
        if (value.isEmpty()) {
            return null;
        }
        return Enum.valueOf(this.enumClass, value);
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }
        try {
            return enumClass.cast(Objects.requireNonNull(value)).toString();
        }
        catch (final ClassCastException ce) {
            throw new IllegalArgumentException(ce);
        }
    }

    @Override
    protected final ValidationError checkValue(final Object value) {
        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }

        /*
         * tries to cast the value to the bound generic enum type. This is enough to validate the value as belonging to
         * the right class, as enums cannot subclass each other.
         */
        return checkSameClass(value, enumClass);
    }
}
