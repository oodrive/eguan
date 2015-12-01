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

import io.eguan.configuration.ValidationError.ErrorType;

import java.util.regex.Pattern;

import javax.annotation.Nonnull;

/**
 * Abstract implementation of the {@link ConfigKey} interface.
 *
 * This class provides additional protected methods for internal usage.
 *
 * @author oodrive
 * @author pwehrle
 *
 */
public abstract class AbstractConfigKey implements ConfigKey<MetaConfiguration> {

    /**
     * The regular expression against which to match the {@link #name}.
     */
    private static final String NAME_REGEX = "[a-zA-Z0-9]+(\\.[a-zA-Z0-9]+)*";

    /**
     * The {@link Pattern} to use for {@link #name} validation.
     */
    protected static final Pattern NAME_PATTERN = Pattern.compile(NAME_REGEX);

    /**
     * Validates a name against the {@link AbstractConfigKey}'s constraints on names.
     *
     * @param name
     *            the name to check against the mandatory syntax (i.e. {@value #NAME_REGEX})
     * @return {@code true} if the name is valid, {@code false} if the name is empty, {@code null} or otherwise invalid
     */
    protected static boolean isNameValid(final String name) {
        if (name == null) {
            return false;
        }
        return NAME_PATTERN.matcher(name).matches();
    }

    /**
     * The unique name returned provided to {@link #AbstractConfigKey(String)} and returned by {@link #getName()}.
     */
    private final String name;

    /**
     * Abstract constructor taking the mandatory name as argument.
     *
     * @param name
     *            the unique name identifying this key
     */
    protected AbstractConfigKey(final String name) {
        this.name = name;
    }

    @Override
    public final boolean hasDefaultValue() {
        // returns definitive value according to constraints
        return isRequired() ? false : getDefaultValue() != null;
    }

    @Override
    public boolean isRequired() {
        // defaults to required if there is no default value
        return getDefaultValue() == null;
    }

    /**
     * Gets the unique name to be used in .properties files for this key.
     *
     * The returned name uniquely identifies a configuration key within this context, following a syntax matching
     * {@value #NAME_REGEX}.
     *
     * @return the non-{@code null} unique name within the context
     */
    @Nonnull
    protected final String getName() {
        return name;
    }

    /**
     * Parses the given {@link String} value defined for the {@link ConfigKey}.
     *
     * This method returns a result of the accurate type if it can be parsed from the provided {@link String} value.
     *
     * If the method is passed an empty {@link String}, it can return either <code>null</code> or an empty String
     *
     * @param value
     *            the non-{@code null} value to parse as read from a .properties file.
     * @return the typed value, or, when the <code>value</code> is empty, the default value or <code>null</code> if
     *         there is no default value.
     * @throws IllegalArgumentException
     *             if no value can be parsed and/or conversion to the target type fails
     * @throws NullPointerException
     *             if the passed value is {@code null}
     */
    protected abstract Object parseValue(@Nonnull String value) throws IllegalArgumentException, NullPointerException;

    /**
     * Provides the inverse of {@link #parseValue(String)}, i.e. produces a {@link String} from the provided value that
     * can be parsed to the same (content) value by {@link #parseValue(String)}. Therefore, a <code>null</code>
     * parameter will be converted to an empty {@link String}.
     *
     * @param value
     *            the value of the same type as the one produced by {@link #parseValue(String)} or <code>null</code>
     * @return the {@link String} containing all the necessary information to be parsed back to the equivalent value
     * @throws IllegalArgumentException
     *             if the argument is of the wrong type or cannot be converted to the target type
     */
    protected abstract String valueToString(Object value) throws IllegalArgumentException;

    /**
     * Checks the given value for validity regarding constraints defined by this {@link ConfigKey}.
     *
     *
     * The value parameter must be of the type obtained from {@link #parseValue(String)}.
     * <ul>
     * <li>allows {@code null} as a valid value for undefined values</li>
     * <li>performs basic sanity checks (not negative, correct syntax, ...) and</li>
     * <li>performs advanced range and domain checks (min and max limits, valid enum constant, ...).</li>
     * </ul>
     *
     * @param value
     *            the value to check, possibly {@code null}
     * @return a specific {@link ValidationError} if the check produced an error, {@link ValidationError#NO_ERROR}
     *         otherwise
     */
    protected abstract ValidationError checkValue(Object value);

    /**
     * Gets the default value for this key.
     *
     * Keys marked as required cannot have a non-{@code null} default value.
     *
     * @return the default value or {@code null} if there is none
     */
    protected abstract Object getDefaultValue();

    /**
     * Utility method that checks if the given configuration key is managed by the configuration provided and throw an
     * {@link IllegalStateException} if this is not the case.
     *
     * @param config
     *            the {@link MetaConfiguration} to check
     * @throws IllegalStateException
     *             if the provided configuration does not contain this key
     */
    protected final void checkConfigForKey(@Nonnull final MetaConfiguration config) throws IllegalStateException {
        if (!config.getConfigKeys().contains(this)) {
            throw new IllegalStateException(String.format("Configuration does not include key; config='%s', key='%s'",
                    config, this));
        }
    }

    /**
     * Checks if the given value is {@code null} and/or required.
     *
     * @param value
     *            the value to check
     * @return <ul>
     *         <li>a {@link ValidationError} with {@link ValidationError.ErrorType#NONE} if value is neither
     *         {@code null} nor required</li>
     *         <li>a {@link ValidationError} with {@link ValidationError.ErrorType#VALUE_NULL} if value is {@code null}
     *         but not required</li>
     *         <li>a specific {@link ValidationError} with {@link ValidationError.ErrorType#VALUE_REQUIRED} if value is
     *         both {@code null} and required</li>
     *         </ul>
     */
    protected final ValidationError checkForNullAndRequired(final Object value) {
        ValidationError result = ValidationError.NO_ERROR;
        if (value == null) {
            result = new ValidationError(ErrorType.VALUE_NULL, null, this, value, "value is null");
            if (isRequired()) {
                result = new ValidationError(ErrorType.VALUE_REQUIRED, null, this, value, "is required and not set");
            }
        }
        return result;
    }

    /**
     * Checks if the given {@link Object} is of the exact {@link Class} provided as second argument.
     *
     * As member classes pass this test, enums constants pass.
     *
     * @param value
     *            the Object to check
     * @param clazz
     * @return a {@link ValidationError} with {@link ValidationError.ErrorType#VALUE_INVALID} if the provided value is
     *         not of the exact same type, {@link ValidationError.NO_ERROR} otherwise
     * @throws NullPointerException
     *             if either argument is <code>null</code>
     */
    protected final ValidationError checkSameClass(final Object value, final Class<?> clazz)
            throws NullPointerException {
        try {
            clazz.cast(value);
        }
        catch (final ClassCastException ce) {
            return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, ce.getMessage());
        }

        final Class<?> valueClass = value.getClass();

        // checks if clazz is the same or a superclass of value's class
        if (valueClass.isAssignableFrom(clazz)) {
            return ValidationError.NO_ERROR;
        }

        // treats the special case where an enum constant appears as member instance of the enclosing enum class
        if (valueClass.getEnclosingClass() == clazz) {
            return ValidationError.NO_ERROR;
        }

        return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, "is a subclass of "
                + clazz.getCanonicalName());

    }

    /**
     * Return the {@link String} representation of this object, i.e. it's {@link Class#getCanonicalName() canonical
     * name}.
     *
     * Classes extending {@link AbstractConfigKey} are expected to be singletons, so this method is just for prettily
     * printing the class name.
     */
    @Override
    public final String toString() {
        return getClass().getCanonicalName();
    }
}
