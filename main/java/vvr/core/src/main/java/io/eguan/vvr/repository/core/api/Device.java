package io.eguan.vvr.repository.core.api;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Future;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Class representing an active device accepting both read and write requests.
 * 
 * Devices are tied to a parent snapshot and store blocks in a copy-on-write manner.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * @author ebredzinski
 * 
 */

public interface Device extends VvrItem {

    /**
     * Sets the size of this device.
     * <p>
     * 
     * For size increases, implementing classes must check if the containing {@link VersionedVolumeRepository VVR} has
     * enough (theoretical) capacity left to allow allocation of the potential extra capacity.
     * <p>
     * Decreasing the size requires checking the allocation state of the device and not accepting anything below the
     * offset of the last allocated data block.
     * <p>
     * 
     * @param size
     *            the new, non-zero and non-negative size in bytes
     */
    FutureVoid setSize(@Nonnull @Nonnegative long size);

    /**
     * Gets the active state of this device.
     * 
     * @return the activation state
     */
    boolean isActive();

    /**
     * Activates the device.
     * 
     * @return a {@link Future} to follow the activation progress
     */
    FutureVoid activate();

    /**
     * Deactivates the device.
     * 
     * @return a {@link Future} to follow the deactivation progress
     */
    FutureVoid deactivate();

    /**
     * Creates a {@link Snapshot} of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot();

    /**
     * Creates a named {@link Snapshot} of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Snapshot}
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot(@Nonnull String name);

    /**
     * Creates a named {@link Snapshot} of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param uuid
     *            UUID of the snapshot to create
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot(@Nonnull UUID uuid);

    /**
     * Creates a named {@link Snapshot} of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Snapshot}
     * @param uuid
     *            UUID of the snapshot to create
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot(@Nonnull String name, @Nonnull UUID uuid);

    /**
     * Creates a named {@link Snapshot} with an optional description of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Snapshot}
     * @param description
     *            the description to associate to the {@link Snapshot}
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot(@Nonnull String name, String description);

    /**
     * Creates a named {@link Snapshot} with an optional description of this device.
     * <p>
     * Implementing classes must throw exceptions should creation fail.
     * 
     * @param name
     *            the mandatory name to give the new {@link Snapshot}
     * @param description
     *            the description to associate to the {@link Snapshot}
     * @param uuid
     *            UUID of the snapshot to create
     * @return a {@link FutureSnapshot} to follow the creation of the snapshot
     */
    FutureSnapshot createSnapshot(@Nonnull String name, String description, @Nonnull UUID uuid);

    /**
     * Opens this device.
     * 
     * @param exclusive
     *            whether to request exclusive (write) access
     * @return a {@link ReadWriteHandle} instance representing the opened access
     */
    ReadWriteHandle open(boolean exclusive);

    /**
     * Deletes this device.
     * <p>
     * Deleting the device while it is active or opened for access must throw an {@link IllegalStateException}
     * 
     * @return a {@link Future} to follow the deletion of the device
     */
    FutureVoid delete();

    /**
     * Clone this device
     * 
     * @param name
     *            the name of the new device
     * 
     * @return a {@link Future} to follow the cloning of the device
     */
    FutureDevice clone(@Nonnull String name);

    /**
     * Clone this device
     * 
     * @param name
     *            the name of the new device
     * @param description
     *            the description of the new device
     * 
     * @return a {@link Future} to follow the cloning of the device
     */
    FutureDevice clone(@Nonnull String name, String description);

    /**
     * Clone this device
     * 
     * @param name
     *            the name of the new device
     * @param uuid
     *            the uuid of he new device
     * 
     * @return a {@link Future} to follow the cloning of the device
     */
    FutureDevice clone(@Nonnull String name, @Nonnull UUID uuid);

    /**
     * Clone this device
     * 
     * @param name
     *            the name of the new device
     * @param description
     *            the description of the new device
     * @param uuid
     *            the uuid of he new device
     * 
     * @return a {@link Future} to follow the cloning of the device
     */
    FutureDevice clone(@Nonnull String name, String description, @Nonnull UUID uuid);

    /**
     * Interface for read/write handles obtained through {@link Device#open(boolean)}.
     * 
     * 
     */
    interface ReadWriteHandle extends AutoCloseable {

        /**
         * Gets the current size of the item.
         * 
         * @return the current size in bytes
         */
        long getSize();

        /**
         * Gets the current block size of the item.
         * 
         * @return the current block size in bytes
         */
        int getBlockSize();

        /**
         * Reads bytes from storage to the passed {@link ByteBuffer}.
         * 
         * @param destination
         *            the {@link ByteBuffer} into which the data will be copied
         * @param destinationOffset
         *            the offset in bytes at which to write to the destination
         * @param length
         *            the number of bytes to copy
         * @param devOffset
         *            the position of the first byte on the device to be read
         * @throws IOException
         *             if the operation cannot be completed for any reason
         * @throws NullPointerException
         *             if <code>destination</code> is <code>null</code>
         */
        void read(@Nonnull ByteBuffer destination, @Nonnegative int destinationOffset, @Nonnegative int length,
                @Nonnegative long devOffset) throws IOException;

        /**
         * Writes part of the passed {@link ByteBuffer}'s content.
         * 
         * @param source
         *            the source of the data to be stored
         * @param sourceOffset
         *            the offset in bytes from which to read in the source
         * @param length
         *            the number of bytes to be copied
         * @param devOffset
         *            byte offset in the storage area
         * @throws IOException
         *             if the operation cannot be completed for any reason
         * @throws NullPointerException
         *             if <code>source</code> is <code>null</code>
         */
        void write(@Nonnull ByteBuffer source, @Nonnegative int sourceOffset, @Nonnegative int length,
                @Nonnegative long devOffset) throws IOException;

        /**
         * Trim bytes from storage.
         * 
         * @param length
         *            the number of bytes to trim
         * @param devOffset
         *            the offset in byte to trim in the destination
         */
        void trim(@Nonnegative long length, @Nonnegative long devOffset);

        /**
         * Closes the {@link ReadWriteHandle}.
         */
        @Override
        void close();

    }

}
