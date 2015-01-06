package com.oodrive.nuage.configuration;

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

/**
 * {@link AbstractConfigKey} implementation capable of handling plain {@link String} values.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
public abstract class StringConfigKey extends AbstractConfigKey {

    /**
     * Proxy constructor for subclasses.
     * 
     * @param name
     *            see {@link AbstractConfigKey#AbstractConfigKey(String)}
     */
    protected StringConfigKey(final String name) {
        super(name);
    }

    @Override
    public final String getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return (String) configuration.getValue(this);
    }

    @Override
    protected final String parseValue(final String value) throws IllegalArgumentException, NullPointerException {
        if (value.isEmpty()) {
            return "";
        }
        return value;
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw new IllegalArgumentException("Not a String");
    }

    @Override
    protected final ValidationError checkValue(final Object value) {
        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }
        return checkSameClass(value, String.class);
    }

    @Override
    protected abstract String getDefaultValue();

}
