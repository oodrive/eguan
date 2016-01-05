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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Abstract {@link AbstractConfigKey} implementation taking values of {@link Boolean} type.
 * 
 * This class accepts the values (case insensitive) "yes", "true" for {@link Boolean#TRUE} and "no", "false" for
 * {@link Boolean#FALSE}. Throws an exception for anything else.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public abstract class BooleanConfigKey extends AbstractConfigKey {

    /**
     * values (case-insensitive) that resolve to {@link Boolean#TRUE}.
     */
    static final List<String> VALUES_FOR_TRUE = Arrays.asList(new String[] { "yes", "true", "on" });

    /**
     * values (case-insensitive) that resolve to {@link Boolean#FALSE}.
     */
    static final List<String> VALUES_FOR_FALSE = Arrays.asList(new String[] { "no", "false", "off" });

    /**
     * Proxy constructor for subclasses.
     * 
     * @param name
     *            see {@link AbstractConfigKey#AbstractConfigKey(String)}
     */
    protected BooleanConfigKey(final String name) {
        super(name);
    }

    @Override
    public final Boolean getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return (Boolean) configuration.getValue(this);
    }

    @Override
    protected final Boolean parseValue(final String value) throws IllegalArgumentException, NullPointerException {
        if (value.isEmpty()) {
            // Returns the default value or null when there is no default value
            final Boolean defaultValue = (Boolean) getDefaultValue();
            return defaultValue;
        }

        final String cleanValue = value.trim().toLowerCase();

        if (VALUES_FOR_TRUE.contains(cleanValue)) {
            return Boolean.TRUE;
        }

        if (VALUES_FOR_FALSE.contains(cleanValue)) {
            return Boolean.FALSE;
        }

        throw new IllegalArgumentException("Not a boolean value");
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }
        if (Boolean.TRUE.equals(value)) {
            return VALUES_FOR_TRUE.get(0);
        }
        if (Boolean.FALSE.equals(value)) {
            return VALUES_FOR_FALSE.get(0);
        }
        throw new IllegalArgumentException("Not a Boolean");
    }

    @Override
    protected final ValidationError checkValue(final Object value) {

        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }
        return checkSameClass(value, Boolean.class);
    }

}
