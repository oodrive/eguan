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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Strings;

/**
 * Abstract implementation of the {@link ConfigurationContext} interface.
 * 
 * This class provides configuration key list management for all subclasses.
 * 
 * @author oodrive
 * @author pwehrle
 */
@Immutable
public abstract class AbstractConfigurationContext implements
        ConfigurationContext<MetaConfiguration, AbstractConfigKey> {

    /**
     * The regular expression against which to match the {@link #name}.
     */
    private static final String NAME_REGEX = "[a-z0-9]+(\\.[a-z0-9]+)*";

    /**
     * The {@link Pattern} to use for {@link #name} validation.
     */
    protected static final Pattern NAME_PATTERN = Pattern.compile(NAME_REGEX);

    /**
     * Validates a name against the {@link AbstractConfigurationContext}'s constraints on names.
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
     * The unique context name of the form {@value #NAME_REGEX}.
     */
    private final String name;

    /**
     * The unmodifiable list of managed {@link ConfigKey}s.
     */
    private final List<AbstractConfigKey> configurationKeys;

    /**
     * Constructs an {@link AbstractConfigurationContext} instance with the given list of {@link AbstractConfigKey}s.
     * 
     * @param name
     *            the unique {@link #getName() name} of the context, must comply to the syntax {@value #NAME_REGEX}
     * @param configKeys
     *            the {@link AbstractConfigKey}s this context exclusively manages
     * @throws IllegalArgumentException
     *             if either argument is {@code null} or empty
     * @throws NullPointerException
     *             if the list is {@code null}
     */
    protected AbstractConfigurationContext(final String name, final AbstractConfigKey... configKeys)
            throws IllegalArgumentException, NullPointerException {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name=" + name);
        }
        this.name = name;

        if (configKeys.length == 0) {
            throw new IllegalArgumentException();
        }
        configurationKeys = Collections.unmodifiableList(Arrays.asList(configKeys));
    }

    @Override
    public final Collection<AbstractConfigKey> getConfigKeys() {
        return configurationKeys;
    }

    @Override
    public List<ValidationError> validateConfiguration(final MetaConfiguration configuration) {
        final ArrayList<ValidationError> result = new ArrayList<ValidationError>();

        for (final AbstractConfigKey currKey : getConfigKeys()) {
            final Object value = configuration.getValue(currKey);
            final ValidationError checkResult = currKey.checkValue(value);
            if (checkResult != ValidationError.NO_ERROR) {
                checkResult.addConfigurationContext(this);
                result.add(checkResult);
            }
        }

        return result;
    }

    @Override
    public final String getPropertyKey(final ConfigKey<MetaConfiguration> configKey) throws IllegalArgumentException,
            NullPointerException {
        if (!configurationKeys.contains(configKey)) {
            throw new IllegalArgumentException("Key is not managed by this context: key=" + configKey);
        }
        return getName() + "." + ((AbstractConfigKey) configKey).getName();
    }

    /**
     * Gets the unique name to add to all managed configuration keys in .properties files.
     * 
     * This name complies to the following syntax: {@value #NAME_REGEX}
     * 
     * @return the unique name for property keys
     */
    protected final String getName() {
        return name;
    }

    /**
     * Return the {@link String} representation of this object, i.e. it's {@link Class#getSimpleName() simple name}.
     * 
     * Classes extending {@link AbstractConfigurationContext} are expected to be singletons, so this method is just for
     * prettily printing the class name.
     * 
     * @return the context class' simple name
     */
    @Override
    public final String toString() {
        return getClass().getSimpleName();
    }

}
