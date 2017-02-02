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

import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.naming.OperationNotSupportedException;

/**
 * An read-only view on a given storage volume.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public interface Snapshot extends VvrItem {

    /**
     * Gets the children snapshots of this snapshot.
     * 
     * @return the children snapshots
     */
    Collection<UUID> getChildrenSnapshotsUuid();

    /**
     * Gets the children devices of this snapshot.
     * 
     * @return the children devices
     */
    Collection<UUID> getSnapshotDevicesUuid();

    /**
     * Creates a named {@link Device} attached to this {@link Snapshot}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name);

    /**
     * Creates a named {@link Device} with the given size attached to this {@link Snapshot}. The device will be
     * identified by the given {@link UUID}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param uuid
     *            the {@link UUID} of the device to create.
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, @Nonnull UUID uuid);

    /**
     * Creates a named {@link Device} with the given size attached to this {@link Snapshot}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param size
     *            the size of the newly created {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, long size);

    /**
     * Creates a named {@link Device} with the given size attached to this {@link Snapshot}. The device will be
     * identified by the given {@link UUID}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param uuid
     *            the {@link UUID} of the device to create.
     * @param size
     *            the size of the newly created {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, @Nonnull UUID uuid, long size);

    /**
     * Creates a named {@link Device} with an optional description attached to this {@link Snapshot}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param description
     *            the description to associate to the {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, String description);

    /**
     * Creates a named {@link Device} with an optional description attached to this {@link Snapshot}. The device will be
     * identified by the given {@link UUID}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param description
     *            the description to associate to the {@link Device}
     * @param uuid
     *            the {@link UUID} of the device to create.
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, String description, @Nonnull UUID uuid);

    /**
     * Creates a named {@link Device} with an optional description attached to this {@link Snapshot}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param description
     *            the description to associate to the {@link Device}
     * @param size
     *            the size of the newly created {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, String description, long size);

    /**
     * Creates a named {@link Device} with an optional description attached to this {@link Snapshot}. The device will be
     * identified by the given {@link UUID}.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Device}
     * @param description
     *            the description to associate to the {@link Device}
     * @param uuid
     *            the {@link UUID} of the device to create.
     * @param size
     *            the size of the newly created {@link Device}
     * @return a {@link FutureDevice} to follow the creation of the device
     */
    FutureDevice createDevice(@Nonnull String name, String description, @Nonnull UUID uuid, long size);

    /**
     * Exports this snapshot to a binary format.
     * 
     * @return the binary representation of this snapshot
     * @throws OperationNotSupportedException
     *             if the snapshot implementation does not support exporting
     */
    byte[] export() throws OperationNotSupportedException;

    /**
     * Deletes this snapshot.
     * <p>
     * Deleting the root snapshot of any repository (obtained by {@link VersionedVolumeRepository#getRootSnapshot()}
     * must throw an {@link IllegalStateException}
     * 
     * @return a {@link Future} to follow the deletion of the device
     */
    FutureVoid delete();

    // end official API

    /**
     * Gets the creation time of the snapshot.
     * 
     * @return the official time this snapshot was created
     */
    Date getSnapshotTime();
}
