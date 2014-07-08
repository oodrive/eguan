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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Class for report items describing validation errors when throwing {@link ConfigValidationException}s.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
@Immutable
public final class ValidationError implements Serializable {

    private static final long serialVersionUID = -6015469385141576890L;

    /**
     * Static member to return when there is no validation error.
     */
    public static final ValidationError NO_ERROR = new ValidationError(ErrorType.NONE,
            new ConfigurationContext[] { null }, new ConfigKey[] { null }, null, "no error");

    /**
     * Enumeration of possible error types.
     * 
     * 
     */
    public enum ErrorType {
        /**
         * Returned when there is no error.
         */
        NONE,
        /**
         * Returned for empty, null or invalid context names.
         */
        CONTEXT_NAME,
        /**
         * Returned for null keys in contexts or duplicate keys between contexts.
         */
        CONTEXT_KEYS,
        /**
         * Returned for null or invalid key names.
         */
        KEYS_NAME,
        /**
         * Returned for null values.
         */
        VALUE_NULL,
        /**
         * Returned for invalid values according to a key's constraints.
         */
        VALUE_INVALID,
        /**
         * Returned on missing values for required keys.
         */
        VALUE_REQUIRED;
    }

    /**
     * Generates a multi-line error report from the given {@link ValidationError} instance.
     * 
     * @param error
     *            a non-<code>null</code> {@link ValidationError}
     * @return a multi-line error report including all information extracted from the argument, or {@value #NO_ERROR
     *         #getErrorMessage()} if the argument is {@link #NO_ERROR}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    // TODO: find a way to bind generic type arguments to avoid unchecked casts and raw types
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static final String getFormattedErrorReport(@Nonnull final ValidationError error)
            throws NullPointerException {
        if (NO_ERROR.equals(Objects.requireNonNull(error))) {
            return NO_ERROR.getErrorMessage();
        }

        final StringBuilder formattedContextList = new StringBuilder();
        final String separator = System.getProperty("line.separator");

        final ErrorType errType = error.getType();
        formattedContextList.append("type=" + errType.toString() + separator);
        final List<ConfigurationContext<?, ?>> contextList = Arrays.asList(error.getConfigurationContexts());
        final List<ConfigKey<?>> keyList = Arrays.asList(error.getConfigKeys());

        for (final ConfigurationContext<?, ?> currContext : contextList) {
            formattedContextList.append("context=" + currContext.getClass().getSimpleName());

            final ArrayList<ConfigKey<?>> contextKeyList = new ArrayList<ConfigKey<?>>(currContext.getConfigKeys());
            contextKeyList.retainAll(keyList);
            if (contextKeyList.isEmpty()) {
                continue;
            }
            formattedContextList.append("; key(s)=");
            for (final ConfigKey<?> currKey : contextKeyList) {
                formattedContextList.append(currContext.getPropertyKey((ConfigKey) currKey));
                if (contextKeyList.indexOf(currKey) < contextKeyList.size() - 1) {
                    formattedContextList.append(", ");
                }
            }
            formattedContextList.append(separator);
        }
        if (error.getValue() != null) {
            formattedContextList.append("Provided value: " + error.getValue() + separator);
        }
        formattedContextList.append("Error message: " + error.getErrorMessage() + separator);
        return formattedContextList.toString();
    }

    private final ErrorType type;
    private final HashSet<ConfigurationContext<?, ?>> contexts = new HashSet<ConfigurationContext<?, ?>>();
    private final ConfigKey<?>[] configKeys;
    private final Object value;
    private final String errorMessage;

    /**
     * Constructs an immutable instance.
     * 
     * @param type
     *            the {@link ErrorType} representing this error
     * @param contexts
     *            an array of affected {@link ConfigurationContext}, may be null
     * @param configKeys
     *            the optional array of affected {@link ConfigKey}s within the context
     * @param value
     *            the optional value affected to the configuration key
     * @param errorMessage
     *            an explicit free-text error message describing the problem
     */
    public ValidationError(@Nonnull final ErrorType type, final ConfigurationContext<?, ?>[] contexts,
            final ConfigKey<?>[] configKeys, final Object value, @Nonnull final String errorMessage) {
        super();
        this.type = Objects.requireNonNull(type);
        this.contexts.addAll(Arrays.asList(contexts));
        this.configKeys = configKeys;
        this.value = value;
        this.errorMessage = Objects.requireNonNull(errorMessage);
    }

    /**
     * Constructor taking only one context and config key argument.
     * 
     * @param type
     *            the {@link ErrorType} representing this error
     * @param context
     *            the affected {@link ConfigurationContext}, may be <code>null</code>
     * @param configKey
     *            the optional affected {@link ConfigKey}s within the context
     * @param value
     *            the optional value affected to the configuration key
     * @param errorMessage
     *            an explicit free-text error message describing the problem
     */
    public ValidationError(@Nonnull final ErrorType type, final ConfigurationContext<?, ?> context,
            final ConfigKey<?> configKey, final Object value, @Nonnull final String errorMessage) {
        this(type, (context == null ? new ConfigurationContext[0] : new ConfigurationContext[] { context }),
                (configKey == null ? new ConfigKey[0] : new ConfigKey[] { configKey }), value, errorMessage);
    }

    /**
     * Gets the {@link ErrorType} of this error.
     * 
     * @return the non-{@code null} error type
     */
    public final ErrorType getType() {
        return type;
    }

    /**
     * Sets the configuration context outside of construction.
     * 
     * @param context
     *            the affected, non-{@code null} {@link ConfigurationContext}
     */
    public final void addConfigurationContext(@Nonnull final ConfigurationContext<?, ?> context) {
        contexts.add(Objects.requireNonNull(context));
    }

    /**
     * Gets the {@link ConfigurationContext}s affected by this error.
     * 
     * @return an array of {@link ConfigurationContext}s which may be empty
     */
    public final ConfigurationContext<?, ?>[] getConfigurationContexts() {
        return contexts.toArray(new ConfigurationContext<?, ?>[contexts.size()]);
    }

    /**
     * Gets the optional affected {@link ConfigKey} within the {@link #getContext() context}.
     * 
     * @return the affected {@link ConfigKey}, or {@code null} if undefined
     */
    public final ConfigKey<?>[] getConfigKeys() {
        return configKeys;
    }

    /**
     * Gets the optional value affected to the {@link #getConfigKeys() configuration key}.
     * 
     * @return the set value, or {@code null} if either no configuration key is set or no value is defined
     */
    public final Object getValue() {
        return value;
    }

    /**
     * Gets an the error message describing the problem.
     * 
     * @return the error message
     */
    public final String getErrorMessage() {
        return errorMessage;
    }

}