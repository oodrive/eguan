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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.regex.PatternSyntaxException;

import com.google.common.base.Strings;
import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Abstract generic class for any multi-valued configuration keys.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 * @param <T>
 *            the {@link Collection} type to support
 * @param <S>
 *            the {@link Object} type to support as part of the list
 */
public abstract class MultiValuedConfigKey<T extends Collection<S>, S> extends AbstractConfigKey {

    private final Class<T> collectionType;

    private final Class<S> itemType;

    private final String separator;

    /**
     * Internal constructor to be called by subclasses.
     * 
     * @param name
     *            see {@link AbstractConfigKey#AbstractConfigKey(String)}
     * @param separator
     *            a separator to use when {@link #parseValue(String) parsing} or {@link #valueToString(Object)
     *            serializing} values.
     * @param collectionType
     *            the collection type's {@link Class} instance representing {@link T}
     * @param itemType
     *            the item type's {@link Class} instance representing {@link S}
     */
    protected MultiValuedConfigKey(final String name, final String separator, final Class<T> collectionType,
            final Class<S> itemType) {
        super(name);
        if (Strings.isNullOrEmpty(separator)) {
            throw new IllegalArgumentException("Separator is null or empty");
        }
        this.separator = separator;
        this.itemType = Objects.requireNonNull(itemType);
        this.collectionType = Objects.requireNonNull(collectionType);
    }

    @Override
    public final T getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return makeDefensiveCopy(collectionType.cast(configuration.getValue(this)));
    }

    @Override
    protected final T parseValue(final String value) throws PatternSyntaxException, IllegalArgumentException,
            NullPointerException {

        if (value.isEmpty()) {
            return null;
        }

        final String[] splitValues = value.split(separator);
        final ArrayList<S> parsedList = new ArrayList<S>(splitValues.length);

        for (final String keyValue : splitValues) {
            parsedList.add(getItemValueFromString(keyValue));
        }

        return getCollectionFromValueList(parsedList);
    }

    @Override
    protected final ValidationError checkValue(final Object value) {
        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }

        try {
            final T valueCollection = collectionType.cast(value);
            for (final Object currObject : valueCollection) {
                itemType.cast(currObject);
            }
            return performAdditionalValueChecks(valueCollection);
        }
        catch (final ClassCastException ce) {
            return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, ce.getMessage());
        }
    }

    /**
     * Gets the {@link #separator}.
     * 
     * @return the immutable separator of collection elements for this instance.
     */
    protected final String getSeparator() {
        return this.separator;
    }

    /**
     * Utility method to provide {@link #parseValue(String)} with the correct {@link T collection type} to return
     * without resorting to reflection.
     * 
     * @param values
     *            an {@link ArrayList} of correctly typed values
     * @return a {@link Collection} of the right {@link T collection type} and with the same content as values to be
     *         returned by {@link #parseValue(String)}
     * @see T
     */
    protected abstract T getCollectionFromValueList(ArrayList<S> values);

    /**
     * Utility method to provide {@link #parseValue(String)} with correctly {@link S typed} items when parsing input
     * Strings. Again, this is to avoid resorting to reflection.
     * 
     * @param value
     *            an un-{@link String#trim() trimmed} token of the list to be parsed into a single value
     * @return the {@link S correctly typed} item
     * 
     * @see S
     */
    protected abstract S getItemValueFromString(String value);

    /**
     * Makes a defensive copy of the original value.
     * 
     * @param value
     *            the value to copy, may be <code>null</code>
     * @return an independent copy or <code>null</code> if the argument was <code>null</code>
     */
    protected abstract T makeDefensiveCopy(T value);

    /**
     * Performs additional value checks implemented by subclasses on the value already cast to the target type by
     * {@link #checkValue(Object)}.
     * 
     * This method is called by {@link #checkValue(Object)} after the collection value and its elements have been
     * successfully cast to the desired type.
     * 
     * @param value
     *            the value to check
     * @return a specific {@link ValidationError} if checks failed, {@link ValidationError#NO_ERROR} otherwise
     */
    protected abstract ValidationError performAdditionalValueChecks(T value);

}
