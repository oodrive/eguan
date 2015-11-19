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
import io.eguan.utils.Files;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Complete implementation of the {@link Configuration} interface managing {@link AbstractConfigKey}s through
 * {@link AbstractConfigurationContext}s.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class MetaConfiguration implements Configuration<AbstractConfigKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetaConfiguration.class);

    /**
     * The property key pattern to check the entries found in configuration sources against.
     * 
     * This pattern is constructed to depend on the patterns defined in {@link AbstractConfigurationContext} and
     * {@link AbstractConfigKey}.
     * 
     * @see AbstractConfigurationContext#NAME_PATTERN
     * @see AbstractConfigKey#NAME_PATTERN
     */
    private static final Pattern KEY_PATTERN = Pattern.compile("^(" + AbstractConfigurationContext.NAME_PATTERN + "\\."
            + AbstractConfigKey.NAME_PATTERN + ")\\s*=.*");

    /**
     * Creates a {@link MetaConfiguration} reading {@link Properties} from an {@link InputStream} and validates against
     * at least one {@link ConfigurationContext}.
     * 
     * @param inputStream
     *            a non-{@code null} InputStream from which to load the configuration. The stream is explicitly closed
     *            when loading is finished.
     * @param configurationContexts
     *            the ConfigurationContexts to include for managing the keys and values of the configuration
     * @return a functional and valid {@link MetaConfiguration} instance
     * @throws ConfigValidationException
     *             <ul>
     *             <li>if the configuration loaded from the {@link InputStream} is invalid or</li>
     *             <li>a collision between ConfigurationContext names is detected</li>
     *             </ul>
     * @throws IOException
     *             if reading the {@link InputStream} fails
     * @throws NullPointerException
     *             if the {@link InputStream} is {@code null}, among others
     * @throws IllegalArgumentException
     *             if
     *             <ul>
     *             <li>not at least one {@link ConfigurationContext} is provided ({@code null} is not accepted) or</li>
     *             <li>a value provided in the configuration fails to parse</li>
     *             </ul>
     */
    public static final MetaConfiguration newConfiguration(@Nonnull final InputStream inputStream,
            final AbstractConfigurationContext... configurationContexts) throws IOException, ConfigValidationException,
            NullPointerException, IllegalArgumentException {

        if (inputStream == null) {
            throw new NullPointerException("InputStream to read from is null");
        }

        if (configurationContexts == null) {
            throw new IllegalArgumentException("Configuration context list is null");
        }

        final List<AbstractConfigurationContext> configContextList = new ArrayList<AbstractConfigurationContext>(
                Arrays.asList(configurationContexts));

        if (configContextList.contains(null)) {
            LOGGER.warn("Configuration context list contains null; list='{}'", Arrays.toString(configurationContexts));
            configContextList.remove(null);
        }

        if (configContextList.size() < 1) {
            throw new IllegalArgumentException("No configuration context given");
        }

        final List<ValidationError> globalReport = checkConfigurationContexts(configContextList);

        globalReport.addAll(checkForDuplicateConfigKeys(configContextList));

        globalReport.addAll(checkForNameCollisions(configContextList));

        if (!globalReport.isEmpty()) {
            throw new ConfigValidationException(globalReport);
        }

        // final Properties newProperties = checkAndLoadProperties(inputStream);
        final Properties newProperties = new Properties();

        globalReport.addAll(checkInputAndLoadProperties(inputStream, newProperties));

        final MetaConfiguration result = new MetaConfiguration(newProperties, configContextList);

        // validates the resulting state

        for (final AbstractConfigurationContext currContext : configContextList) {
            globalReport.addAll(currContext.validateConfiguration(result));
        }
        if (!globalReport.isEmpty()) {
            throw new ConfigValidationException(globalReport);
        }

        return result;

    }

    /**
     * Load a {@link Properties} file to create a new {@link MetaConfiguration}.
     * 
     * @param configurationFile
     *            file to read
     * @param configurationContexts
     *            the ConfigurationContexts to include for managing the keys and values of the configuration
     * @return a non-<code>null</code> {@link MetaConfiguration}
     * @throws IOException
     *             if reading the file fails
     * @throws ConfigValidationException
     *             if the configuration contained in the file is invalid
     * @throws NullPointerException
     *             if the given {@link File} is <code>null</code>
     * @throws IllegalArgumentException
     *             if
     *             <ul>
     *             <li>not at least one {@link ConfigurationContext} is provided ({@code null} is not accepted) or</li>
     *             <li>a value provided in the configuration fails to parse</li>
     *             </ul>
     * @see #newConfiguration(InputStream, AbstractConfigurationContext...)
     */
    public static final MetaConfiguration newConfiguration(@Nonnull final File configurationFile,
            final AbstractConfigurationContext... configurationContexts) throws IOException, ConfigValidationException,
            NullPointerException, IllegalArgumentException {
        // new FileInputStream throws NPE if the File is null
        try (FileInputStream fis = new FileInputStream(configurationFile)) {
            return MetaConfiguration.newConfiguration(fis, configurationContexts);
        }
    }

    /**
     * The properties object holding the complete configuration.
     */
    private final Properties configProperties;

    /**
     * Map to hold the managed part of the configuration, with keys and values of the corresponding type.
     */
    private final Map<AbstractConfigKey, Object> typedConfiguration;

    /**
     * Properties holding all key-value pairs not explicitly managed by this configuration.
     */
    private final Properties unmanagedProperties;

    /**
     * Set of {@link ConfigurationContext} managed by this configuration.
     */
    private final Set<AbstractConfigurationContext> configContexts;

    /**
     * Immutable list of managed {@link ConfigKey}s.
     */
    private final Collection<AbstractConfigKey> configKeys;

    /**
     * The comment string to insert when storing the configuration.
     * 
     * @see MetaConfiguration#storeConfiguration(OutputStream)
     * @see MetaConfiguration#storeCompleteConfiguration(OutputStream)
     */
    private final String propertiesFileComment;

    /**
     * Private constructor only to be called by
     * {@link MetaConfiguration#newConfiguration(InputStream, ConfigurationContext...)}.
     * 
     * @param properties
     *            the {@link Properties} object defining the state of this configuration
     * @param configContextList
     *            a list of {@link AbstractConfigurationContext}, excluding {@code null}
     * @throws IllegalArgumentException
     *             if one of the parameter values fails to parse correctly
     */
    private MetaConfiguration(final Properties properties, final List<AbstractConfigurationContext> configContextList)
            throws IllegalArgumentException {
        // creates context set without extra verification, as this has already happened in the factory method
        configContexts = Collections.unmodifiableSet(new HashSet<AbstractConfigurationContext>(configContextList));

        // reads the configuration from the input stream
        configProperties = properties;

        final ArrayList<AbstractConfigKey> configKeyList = new ArrayList<AbstractConfigKey>();

        for (final AbstractConfigurationContext currContext : configContexts) {
            configKeyList.addAll(currContext.getConfigKeys());
        }

        configKeys = Collections.unmodifiableList(configKeyList);

        // initializes the typed configuration map
        typedConfiguration = new HashMap<AbstractConfigKey, Object>(configKeys.size());

        // initializes the unmanaged Properties map to include mappings for all keys found
        unmanagedProperties = new Properties();
        unmanagedProperties.putAll(configProperties);

        /*
         * iterates over all managed keys to the typed map adding them to the typed map and removing them from the
         * unmanaged map
         */
        for (final AbstractConfigurationContext currContext : configContexts) {
            for (final AbstractConfigKey currKey : currContext.getConfigKeys()) {

                final String propertyKey = currContext.getPropertyKey(currKey);
                final String stringValue = configProperties.getProperty(propertyKey);

                unmanagedProperties.remove(propertyKey);

                if (stringValue == null) {
                    typedConfiguration.put(currKey, currKey.getDefaultValue());
                }
                else {
                    typedConfiguration.put(currKey, currKey.parseValue(stringValue.trim()));
                }
            }
        }

        // initializes the comment to include in saved properties files
        propertiesFileComment = String.format(" %s with configuration contexts: %s",
                MetaConfiguration.class.getSimpleName(), Arrays.toString(configContexts.toArray()));
    }

    @Override
    public final Collection<AbstractConfigKey> getConfigKeys() {
        return configKeys;
    }

    @Override
    public final Properties getUnmanagedKeys() {
        // returns the only defensive copy guaranteed to function
        final Properties result = new Properties();
        result.putAll(unmanagedProperties);
        return result;
    }

    @Override
    public MetaConfiguration copyAndAlterConfiguration(final Map<AbstractConfigKey, Object> newKeyValueMap)
            throws ConfigValidationException, NullPointerException, IllegalArgumentException, IOException {

        final Properties newProps = new Properties();

        newProps.putAll(configProperties);

        for (final AbstractConfigKey currKey : newKeyValueMap.keySet()) {
            final AbstractConfigurationContext context = getContextForKey(currKey);
            if (context == null) {
                throw new IllegalArgumentException("Key not in managed context; key=" + currKey);
            }
            newProps.setProperty(context.getPropertyKey(currKey), currKey.valueToString(newKeyValueMap.get(currKey)));
        }

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        newProps.store(outputStream, propertiesFileComment);

        return newConfiguration(new ByteArrayInputStream(outputStream.toByteArray()),
                configContexts.toArray(new AbstractConfigurationContext[configContexts.size()]));
    }

    @Override
    public final void storeConfiguration(final OutputStream outputStream) throws IOException {
        configProperties.store(outputStream, propertiesFileComment);
    }

    @Override
    public final void storeCompleteConfiguration(final OutputStream outputStream) throws IOException {

        getCompleteConfigurationAsProperties().store(outputStream, propertiesFileComment);

    }

    @Override
    public Properties getCompleteConfigurationAsProperties() {
        final Properties result = new Properties();

        result.putAll(unmanagedProperties);

        for (final AbstractConfigurationContext currContext : configContexts) {

            for (final AbstractConfigKey currKey : currContext.getConfigKeys()) {

                final Object rawValue = getValue(currKey);
                result.setProperty(currContext.getPropertyKey(currKey), currKey.valueToString(rawValue));
            }

        }

        return result;
    }

    @Override
    public final void storeConfiguration(final File configuration, final File prevConfiguration, final boolean complete)
            throws IOException {
        // Remove previous configuration, what ever it is
        Files.deleteRecursive(prevConfiguration.toPath());

        // Save previous configuration. Just rename, even if it's not a regular file
        final boolean renamed;
        if (configuration.exists()) {
            if (!configuration.renameTo(prevConfiguration)) {
                throw new IOException("Failed to rename '" + configuration + "'");
            }
            renamed = true;
        }
        else {
            renamed = false;
        }

        // Write new configuration
        boolean done = false;
        try {
            try (FileOutputStream fos = new FileOutputStream(configuration)) {
                if (complete) {
                    storeCompleteConfiguration(fos);
                }
                else {
                    storeConfiguration(fos);
                }
            }
            done = true;
        }
        finally {
            // Restore previous configuration on error
            if (!done) {
                configuration.delete();
                if (renamed) {
                    if (!prevConfiguration.renameTo(configuration)) {
                        throw new IOException("Failed to rename '" + prevConfiguration + "'");
                    }
                }
            }
        }
    }

    /**
     * Gets the untyped value associated with the given key for this {@link Configuration}.
     * 
     * This method gets the raw value directly from the underlying text representation, so there is no guarantee as to
     * the validity or type of the returned value unless the given key is part of the list returned by
     * {@link #getConfigKeys()}.
     * 
     * @param key
     *            the requested {@link ConfigKey key}
     * @return the value associated with the key, null if there is no value in this {@link Configuration}
     * @see #getConfigKeys()
     */
    protected final Object getValue(final AbstractConfigKey key) {
        return typedConfiguration.get(key);
    }

    /**
     * Checks for invalid {@link AbstractConfigurationContext#getName() names} and {@code null} or badly named keys in
     * {@link AbstractConfigurationContext}s.
     * 
     * As lists can contain {@code null} or the same object multiple times, this method does only compare different,
     * non-{@code null} contexts if they point to the same object.
     * 
     * @param configContextList
     *            the list of configurations to check, may contain the same element multiple times
     * @return a report stating all errors encountered
     */
    private static List<ValidationError> checkConfigurationContexts(
            final List<AbstractConfigurationContext> configContextList) {

        final ArrayList<ValidationError> report = new ArrayList<ValidationError>();

        for (final AbstractConfigurationContext currContext : configContextList) {

            // checks the context name
            final String contextName = currContext.getName();
            if (!AbstractConfigurationContext.isNameValid(contextName)) {
                report.add(new ValidationError(ErrorType.CONTEXT_NAME, currContext, null, null,
                        "invalid context name; context=" + currContext + " name=" + contextName));
            }

            final Collection<AbstractConfigKey> currKeys = currContext.getConfigKeys();
            // checks for null keys
            if (currKeys.contains(null)) {
                report.add(new ValidationError(ErrorType.CONTEXT_KEYS, currContext, null, null,
                        "key list contains null"));
            }
            // checks key names
            for (final AbstractConfigKey currKey : currKeys) {
                // skips null keys
                if (currKey == null) {
                    continue;
                }
                final String keyName = currKey.getName();
                if (Strings.isNullOrEmpty(keyName) || !AbstractConfigKey.isNameValid(keyName)) {
                    report.add(new ValidationError(ErrorType.KEYS_NAME, currContext, currKey, null,
                            "invalid key name; key=" + currKey + " name=" + keyName));
                }
            }
        }

        return report;
    }

    /**
     * Checks for duplicate {@link ConfigKey keys} in the provided list of configuration {@link ConfigurationContext}s.
     * 
     * As lists can contain {@code null} or the same object multiple times, this method does only compare different,
     * non-{@code null} contexts if they point to the same object.
     * 
     * @param configContextList
     *            the list of configurations to check, may contain the same element multiple times
     * @return a report stating all duplicate config keys with a message stating the two {@link ConfigurationContext} in
     *         which it was found
     */
    private static List<ValidationError> checkForDuplicateConfigKeys(
            final List<AbstractConfigurationContext> configContextList) {

        final ArrayList<ValidationError> report = new ArrayList<ValidationError>();

        final ArrayList<AbstractConfigKey> duplicateList = new ArrayList<AbstractConfigKey>();

        for (final AbstractConfigurationContext firstContext : configContextList) {
            final Collection<AbstractConfigKey> firstKeys = firstContext.getConfigKeys();
            for (final AbstractConfigurationContext secondContext : configContextList) {

                if (firstContext == secondContext) {
                    continue;
                }

                // checks for disjoint configuration key lists
                final HashSet<AbstractConfigKey> intersection = new HashSet<AbstractConfigKey>(
                        secondContext.getConfigKeys());
                intersection.retainAll(firstKeys);

                if (intersection.isEmpty() || duplicateList.containsAll(intersection)) {
                    continue;
                }

                duplicateList.addAll(intersection);
                final AbstractConfigurationContext[] contexts = new AbstractConfigurationContext[] { firstContext,
                        secondContext };
                final AbstractConfigKey[] keys = new AbstractConfigKey[intersection.size()];

                report.add(new ValidationError(ErrorType.CONTEXT_KEYS, contexts, intersection.toArray(keys), null,
                        "keys exist in both contexts"));
            }
        }
        return report;
    }

    /**
     * Checks for collisions between {@link ConfigurationContext context} {@link AbstractConfigurationContext#getName()
     * names}.
     * 
     * @param configContextList
     *            the list of configurations to check, may contain the same element multiple times
     * @return a report stating all config keys affected by context name collisions with a message stating the two
     *         {@link ConfigurationContext} in which it was found
     */
    private static List<ValidationError> checkForNameCollisions(
            final List<AbstractConfigurationContext> configContextList) {

        final ArrayList<ValidationError> report = new ArrayList<ValidationError>();

        for (final AbstractConfigurationContext firstContext : configContextList) {
            final String firstName = firstContext.getName();

            for (final AbstractConfigurationContext secondContext : configContextList) {
                if (firstContext == secondContext) {
                    continue;
                }

                final String secondName = secondContext.getName();

                if (firstName.startsWith(secondName)) {
                    final String errMsg = String.format(
                            "name collision between context=%s, name=%s and context=%s, name=%s", firstContext,
                            firstName, secondContext, secondName);
                    report.add(new ValidationError(ErrorType.CONTEXT_NAME, firstContext, null, null, errMsg));
                }
            }
        }
        return report;
    }

    /**
     * Checks the input stream for duplicate key entries while loading the content into the provided {@link Properties}
     * instance.
     * 
     * @param inputStream
     *            the {@link InputStream} to load from
     * @param targetProperties
     *            the {@link Properties} to load to
     * @return a report of {@link ValidationError}s relative to duplicate entries
     * @throws IOException
     *             if any of the I/O operations fail
     */
    private static List<ValidationError> checkInputAndLoadProperties(final InputStream inputStream,
            final Properties targetProperties) throws IOException {

        final ArrayList<ValidationError> result = new ArrayList<ValidationError>();
        final ArrayList<String> keyList = new ArrayList<String>();
        final String separator = System.getProperty("line.separator");
        final StringBuilder writer = new StringBuilder();

        /*
         * The following construction causes a massive memory leak if the inputStream throws an IOException upon being
         * closed. In consequence the InputStream must be closed explicitly separately in the finally clause.
         */
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = reader.readLine()) != null) {

                final Matcher match = KEY_PATTERN.matcher(line);
                if (match.matches()) {
                    keyList.add(match.group(1));
                }

                writer.append(line);
                writer.append(separator);
            }
        }
        finally {
            inputStream.close();
        }

        final ByteArrayInputStream newInputStream = new ByteArrayInputStream(writer.toString().getBytes());
        targetProperties.load(newInputStream);

        final AbstractConfigurationContext context = null;

        for (final String currKey : targetProperties.stringPropertyNames()) {
            final int keyFreq = Collections.frequency(keyList, currKey);
            if (keyFreq > 1) {
                result.add(new ValidationError(ErrorType.KEYS_NAME, context, null, null, String.format(
                        "key='%s' appears %d times in input", currKey, Integer.valueOf(keyFreq))));
            }
        }

        return result;
    }

    /**
     * Searches the {@link AbstractConfigurationContext} instance for a given key.
     * 
     * @param key
     *            the {@link AbstractConfigKey} for which to search
     * @return the first {@link AbstractConfigurationContext} registered that manages the given key, <code>null</code>
     *         if none is found
     */
    private AbstractConfigurationContext getContextForKey(final AbstractConfigKey key) {
        for (final AbstractConfigurationContext currContext : this.configContexts) {
            if (currContext.getConfigKeys().contains(key)) {
                return currContext;
            }
        }
        return null;
    }

}
