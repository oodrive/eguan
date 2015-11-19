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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.Immutable;

/**
 * Interface for all configuration implementations based on text configuration files.
 * 
 * Implementing classes provide a front for text-based configurations in {@link Properties} format and guarantee their
 * validity with respect to constraints implemented by a set of {@link ConfigKey}s.
 * 
 * @param <T>
 *            the {@link ConfigKey}s implementation this configuration manages
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
@Immutable
public interface Configuration<T extends ConfigKey<?>> {

    /**
     * Gets all configuration keys.
     * 
     * Keys returned by this method are provided by {@link ConfigurationContext}s associated to this
     * {@link Configuration}.
     * 
     * @return the complete list of {@link ConfigKey configuration keys} managed by this {@link Configuration}
     */
    Collection<T> getConfigKeys();

    /**
     * Gets the part of the configuration that is not managed by any {@link ConfigurationContext} associated to this
     * {@link Configuration}.
     * 
     * Implementing classes provide defensive copies of the result, as modifications to the returned value would violate
     * the immutable constraint on {@link Configuration}.
     * 
     * @return a {@link Properties} object (not backed by the actual instance) containing all key-value pairs not
     *         covered by one of the associated {@link ConfigurationContext}s
     */
    Properties getUnmanagedKeys();

    /**
     * Copies this {@link Configuration} while altering the values for keys given in the given key-value map.
     * 
     * Keys for which a new value is provided must be managed by the original instance.
     * 
     * @param newKeyValueMap
     *            a {@link Map} of keys with matching values to add or overwrite in the copy
     * @return an independent copy of the original {@link Configuration}, with the new values explicitly set
     * @throws ConfigValidationException
     *             if the copy is not valid
     * @throws IOException
     *             if reading the old or writing the new {@link Configuration} instance fails
     * @throws NullPointerException
     *             if the key-value map is <code>null</code>
     * @throws IllegalArgumentException
     *             if any of the keys is not managed by this {@link Configuration} or the associated value cannot be
     *             used
     */
    Configuration<T> copyAndAlterConfiguration(final Map<T, Object> newKeyValueMap) throws ConfigValidationException,
            IOException, NullPointerException, IllegalArgumentException;

    /**
     * Stores the configuration in the given destination.
     * 
     * This method stores all key-value pairs explicitly defined in the configuration loaded on creation, i.e. keys not
     * present, even if returned by {@link #getConfigKeys()}, remain undefined.
     * 
     * @param outputStream
     *            destination output stream.
     * @throws IOException
     *             if writing to the output stream fails
     */
    public void storeConfiguration(final OutputStream outputStream) throws IOException;

    /**
     * Stores the complete configuration including default values in given destination.
     * 
     * This method stores a key-value pair for each key returned by {@link #getConfigKeys()}, inserting default values
     * if no value is explicitly defined.
     * 
     * @param outputStream
     *            destination output stream.
     * @throws IOException
     *             if writing to the output stream fails
     */
    public void storeCompleteConfiguration(final OutputStream outputStream) throws IOException;

    /**
     * Gets the complete configuration as a {@link Properties} instance.
     * 
     * @return a non-<code>null</code> {@link Properties} instance
     * @see #storeCompleteConfiguration(OutputStream)
     */
    public Properties getCompleteConfigurationAsProperties();

    /**
     * Stores the configuration in the given file. If it exists, the current file is renamed to
     * <code>prevConfiguration</code>. On failure, the previous configuration is renamed back <code>configuration</code>
     * .<br>
     * Note: <b><i><code>prevConfiguration</code> is deleted before renaming the destination file</i></b>.
     * 
     * @param configuration
     *            the destination file
     * @param prevConfiguration
     *            new file name of the current configuration file
     * @param complete
     *            <code>true</code> to save the complete configuration (see
     *            {@link #storeCompleteConfiguration(OutputStream)}).
     * @throws IOException
     *             if writing the configuration to the file fails
     */
    public void storeConfiguration(final File configuration, final File prevConfiguration, final boolean complete)
            throws IOException;
}
