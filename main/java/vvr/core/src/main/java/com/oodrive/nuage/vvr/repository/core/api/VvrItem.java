package com.oodrive.nuage.vvr.repository.core.api;

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

import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Common interface for items contained by a VVR.
 *
 * @author oodrive
 * @author pwehrle
 * @author llambert
 *
 */
public interface VvrItem extends UniqueVvrObject {

    /**
     * Gets this item's VVR.
     *
     * @return the {@link VersionedVolumeRepository VVR} to whom this item belongs, never <code>null</code>
     */
    @Nonnull
    VersionedVolumeRepository getVvr();

    /**
     * Gets this item's parent in the hierarchy.
     *
     * @return the unique parent of this item, or null if this is the root item
     */
    UUID getParent();

    /**
     * Gets the partial nature of this item.
     *
     * @return the partial characteristic, i.e. whether this item relies on its ancestors in the tree to provide (at
     *         least part of) its content (which corresponds to <code>true</code>) or not (tertium non datur)
     */
    @Nonnull
    boolean isPartial();

    /**
     * Gets the current size of this item.
     *
     * @return the current size in bytes
     */
    @Nonnegative
    long getSize();

    /**
     * Gets the block size used to divide the {@link Device} internally.
     *
     * @return the block size in bytes as defined by the configuration on startup
     */
    int getBlockSize();

    /**
     * Gets the size of the data allocated for this item.
     *
     * @return the allocated data size for this item in bytes
     */
    @Nonnegative
    long getDataSize();

    /**
     * Associate a user-defined name value pair to the item.
     *
     * @param keyValues
     *            list of key/value pairs
     */
    FutureVoid setUserProperties(@Nonnull String... keyValues);

    /**
     * Removes the given user property. Does nothing if the property is not set.
     *
     * @param keys
     *            keys to remove
     */
    FutureVoid unsetUserProperties(@Nonnull String... keys);

    /**
     * Gets the value of the given user property.
     *
     * @param name
     *            name of the property.
     * @return the value, <code>null</code> if the value is not set.
     */
    String getUserProperty(@Nonnull String name);

    /**
     * Get the current user-defined properties.
     *
     * @return read-only view of the properties, may be empty but not <code>null</code>
     */
    Map<String, String> getUserProperties();

}
