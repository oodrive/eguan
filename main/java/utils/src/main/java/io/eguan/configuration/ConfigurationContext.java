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

import java.util.Collection;
import java.util.List;

/**
 * Context interface for {@link ConfigKey} management.
 * 
 * @param <S>
 *            the {@link Configuration} type the context supports for {@link #validateConfiguration(Configuration)
 *            validation}
 * @param <T>
 *            the {@link ConfigKey} type the context manages, which must be compatible with the configuration type
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public interface ConfigurationContext<S extends Configuration<T>, T extends ConfigKey<S>> {

    /**
     * Gets the complete list of {@link ConfigKey configuration keys} this {@link ConfigurationContext} manages.
     * 
     * @return a non-{@code null} but possibly empty list of singleton {@link ConfigKey}s
     */
    Collection<T> getConfigKeys();

    /**
     * Validates the given {@link Configuration} in this context.
     * 
     * @param configuration
     *            the {@link Configuration} to validate
     * @return a {@link List} with configuration keys and validation error messages, which is empty if no error occurred
     */
    List<ValidationError> validateConfiguration(S configuration);

    /**
     * Gets the complete key for reading/writing from {@link java.util.Properties} objects.
     * 
     * @param configKey
     *            the {@link ConfigKey} for which to get the complete key
     * @return the property key in the form <context name>.<key name>
     * @throws IllegalArgumentException
     *             if the provided {@link ConfigKey} is not managed by this context
     * @throws NullPointerException
     *             if the provided key is {@code null}
     */
    String getPropertyKey(ConfigKey<S> configKey) throws IllegalArgumentException, NullPointerException;

}
