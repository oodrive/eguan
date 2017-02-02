package io.eguan.vvr.repository.core.api;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Basic interface to uniquely identified objects.
 * <p>
 * 
 * Objects managed by the VVR are required to have their own unique identifiers
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */

// TODO: find better ways to enforce unique IDs, possibly through annotations and/or AOP

public interface UniqueVvrObject {

    /**
     * Gets the universally unique identifier of the given object.
     * 
     * @return a non-null UUID for the object
     */
    @Nonnull
    UUID getUuid();

    /**
     * Gets the optional name of this instance.
     * <p>
     * 
     * The returned name may be null or non-unique. If the value is undefined, implementing classes must return null and
     * not throw an exception.
     * 
     * @return the name of the object or null if none was set
     */
    String getName();

    /**
     * Sets the optional name of this instance.
     * <p>
     * 
     * @param name
     *            the arbitrary value (null or empty are allowed) to which to set the name
     * @return a {@link FutureVoid} representing the task, <code>null</code> if there is no change
     */
    FutureVoid setName(String name);

    /**
     * Gets the optional description of this instance.
     * <p>
     * The returned value may be null or non-unique. If the value is undefined, implementing classes must return null
     * and not throw an exception.
     * 
     * @return the description of the object or null if none was set
     */
    String getDescription();

    /**
     * Sets the optional description of this instance.
     * <p>
     * 
     * @param description
     *            the arbitrary value (null or empty are allowed) to which to set the description
     * @return a {@link FutureVoid} representing the task, <code>null</code> if there is no change
     */
    FutureVoid setDescription(String description);

    /**
     * Top interface for object builders to implement.
     * <p>
     * 
     * This interface only requires the essential parts for a repository object:
     * <ul>
     * <li>a setter for the unique ID of the object which must be guaranteed immutable by the implementing class</li>
     * <li>the parameterless builder method</li>
     * </ul>
     * 
     * 
     */
    interface Builder {

        /**
         * Sets the unique ID.
         * <p>
         * 
         * <i>MANDATORY.</i>
         * <p>
         * 
         * @param uuid
         *            the non-null {@link UUID} object to be the immutable identifier of the object.
         * @return the modified builder instance
         */
        Builder uuid(@Nonnull UUID uuid);

        /**
         * Sets the optional name.
         * <p>
         * 
         * <i>OPTIONAL.</i>
         * <p>
         * 
         * @param name
         *            the name to assign the object
         * @return the modified builder instance
         */
        Builder name(String name);

        /**
         * Sets the optional description.
         * <p>
         * 
         * <i>OPTIONAL.</i>
         * <p>
         * 
         * @param description
         *            the description to assign the object
         * @return the modified builder instance
         */
        Builder description(String description);
    }

}
