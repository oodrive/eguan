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

import javax.annotation.Nonnull;

/**
 * Main interface implemented by all configuration keys managed by {@link ConfigurationContext configuration contexts}.
 * 
 * @param <T>
 *            the {@link Configuration} type that can manage this type of key
 * 
 * @author oodrive
 * @author pwehrle
 */
public interface ConfigKey<T extends Configuration<?>> {

    /**
     * Gets the value for this key from the provided configuration.
     * 
     * Implementing classes override this method to return the type closest to the value type represented by the key. If
     * no value has been set within the provided {@link Configuration}, a default value is provided if one was defined.
     * 
     * The returned value is an alterable copy of the original value which is left unaltered in the configuration.
     * 
     * @param configuration
     *            the {@link Configuration} from which to retrieve the value
     * @return the correctly typed value or the default value if none was found or {@code null} if there are neither
     * @throws IllegalStateException
     *             if the key does not belong to one of the contexts used for creating the configuration
     * @throws ClassCastException
     *             if the value read from the configuration is of the wrong type
     * @throws NullPointerException
     *             if the configuration provided as argument is {@code null}
     */
    Object getTypedValue(@Nonnull T configuration) throws IllegalStateException, ClassCastException,
            NullPointerException;

    /**
     * Returns the fact that the implementing class has a default value.
     * 
     * If {@link #isRequired()} returns {@code true}, this method returns {@code false}.
     * 
     * @return {@code true} if a default value is defined and {@link #isRequired()} is {@code false}, {@code false}
     *         otherwise
     */
    boolean hasDefaultValue();

    /**
     * Returns the fact that this key's value must be explicitly set when creating a {@link Configuration} instance.
     * 
     * Required keys must have an explicitly set, non-{@code null} value for the application to run. Validation of a
     * configuration throws an exception if no value is set and this flag is set to {@code true}.
     * 
     * Keys that have a {@link #hasDefaultValue() default value} cannot be required. The inverse, i.e. that keys not
     * marked as required have a default value, does not hold.
     * 
     * @return {@code true} if this key must be set, {@code false} if it can be left empty
     */
    boolean isRequired();

}
