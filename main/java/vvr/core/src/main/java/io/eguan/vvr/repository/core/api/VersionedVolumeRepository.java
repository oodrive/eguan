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

import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.protobuf.MessageLite;

/**
 * Repository containing an arbitrary number of storage volumes.
 * <p>
 * 
 * Each repository has handles on the root volumes of all devices and snapshots.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author ebredzinski
 * @author jmcaba
 * 
 */
public interface VersionedVolumeRepository extends UniqueVvrObject, DtxResourceManager {

    /**
     * Gets the mandatory unique id of this repository's owner.
     * 
     * @return the owner's unique id, never null
     */
    UUID getOwnerId();

    /**
     * Gets the mandatory unique id of the current node.
     * 
     * @return unique id of the current node, never null
     */
    UUID getNodeId();

    /**
     * Initializes the {@link VersionedVolumeRepository} instance.
     * <p>
     * Initialized instances can be {@link #fini() shut down} to return to the uninitialized state
     */
    void init();

    /**
     * Gets the initialization status of the {@link VersionedVolumeRepository} instance.
     * 
     * @return {@code true} if this instance was successfully initialized, {@code false} otherwise
     */
    boolean isInitialized();

    /**
     * Shuts down the {@link VersionedVolumeRepository} instance.
     * <p>
     * <ul>
     * <li>{@link #isStarted() started} instances must be stopped by this method before proceeding with fini</li>
     * <li>shut down instances must be {@link #init() initialized} again before being operational</li>
     * </ul>
     */
    void fini();

    /**
     * Starts operation of the {@link VersionedVolumeRepository} instance.
     * 
     * Instances can only be started after having been successfully {@link #init() initialized}
     * 
     * @param saveState
     *            if <code>true</code>, the repository should save the state <code>started</code>.
     */
    void start(boolean saveState);

    /**
     * Gets the operational status of the {@link VersionedVolumeRepository} instance.
     * 
     * @return {@code true} if this instance was successfully started, {@code false} otherwise
     */
    boolean isStarted();

    /**
     * Stops operation of the {@link VersionedVolumeRepository} instance.
     * <p>
     * Only {@link #isInitialized() initialized} instances can successfully be stopped
     * 
     * @param saveState
     *            if <code>true</code>, the repository should save the state <code>stopped</code>.
     */
    void stop(boolean saveState);

    /**
     * Set the {@link VersionedVolumeRepository} deleted. Clears the content of this repository. Can be done only if the
     * VVR is stopped.
     */
    void delete();

    /**
     * Tells if the {@link VersionedVolumeRepository} is deleted (and should be ignored).
     * 
     * @return <code>true</code> if the {@link VersionedVolumeRepository} is deleted.
     */
    boolean isDeleted();

    // end official API

    /**
     * Gets the {@link Snapshot} designated by the given id.
     * 
     * @param snapshotId
     *            the non-null UUID of the requested snapshot
     * @return the {@link Snapshot} instance representing the requested snapshot or null if non could be found
     */
    Snapshot getSnapshot(@Nonnull UUID snapshotId);

    /**
     * Gets all {@link Snapshot}s from the {@link VersionedVolumeRepository}.
     * 
     * @return the complete list of {@link Snapshot} UUIDs registered with this instance or an empty list if there are
     *         none
     */
    Set<UUID> getSnapshots();

    /**
     * Gets the {@link Device} designated by the given id.
     * 
     * @param deviceId
     *            the non-null UUID of the requested device
     * @return the {@link Device} instance representing the requested device or null if non could be found
     */
    Device getDevice(@Nonnull UUID deviceId);

    /**
     * Gets all {@link Device}s from the {@link VersionedVolumeRepository}.
     * 
     * @return the complete list of {@link Device} UUIDs registered with this instance or an empty list if there are
     *         none
     */
    Set<UUID> getDevices();

    /**
     * Gets the {@link MetaConfiguration} instance this {@link VersionedVolumeRepository repository} uses to access
     * configuration values.
     * <p>
     * 
     * @return the non-{@code null} {@link MetaConfiguration} instance this {@link VersionedVolumeRepository 
     *         repository's} state is based on
     */
    MetaConfiguration getConfiguration();

    /**
     * Update the configuration with the provided key/value pairs.
     * 
     * @param newKeyValueMap
     */
    void updateConfiguration(final Map<AbstractConfigKey, Object> newKeyValueMap);

    /**
     * Gets the snapshot at the root of the repository item hierarchy.
     * 
     * This returns the first snapshot that was created in this repository.
     * 
     * @return the root snapshot or null if none exists yet
     */
    Snapshot getRootSnapshot();

    /**
     * Register a subscriber for VVR event bus. Subscriber will get {@link ItemEvent}s.
     * 
     * @param subscriber
     */
    void registerItemEvents(Object subscriber);

    /**
     * Unregister a subscriber from the event bus.
     * 
     * @param subscriber
     */
    void unregisterItemEvents(Object subscriber);

    /**
     * Handle a remote message on this VVR.
     * 
     * @param op
     * @return an optional reply or <code>null</code>
     */
    MessageLite handleMsg(RemoteOperation op);

    /**
     * Events on items.
     * 
     */
    public abstract class ItemEvent {
        private final VersionedVolumeRepository repository;

        ItemEvent(final VersionedVolumeRepository repository) {
            super();
            this.repository = repository;
        }

        /**
         * Related {@link VersionedVolumeRepository}.
         * 
         * @return the VVR where the event occurred.
         */
        public final VersionedVolumeRepository getRepository() {
            return repository;
        }

    }

    /**
     * Event sent when a new Item is created.
     * 
     */
    public final class ItemCreatedEvent extends ItemEvent {
        private final VvrItem item;

        public ItemCreatedEvent(final VersionedVolumeRepository repository, final VvrItem item) {
            super(repository);
            this.item = item;
        }

        /**
         * Item created
         * 
         * @return the created item.
         */
        public final VvrItem getItem() {
            return item;
        }

    }

    /**
     * Event sent when a new Item is deleted.
     * 
     */
    public final class ItemDeletedEvent extends ItemEvent {
        private final UUID itemUuid;
        private final Class<? extends VvrItem> clazz;

        public ItemDeletedEvent(final VersionedVolumeRepository repository, final UUID itemUuid,
                final Class<? extends VvrItem> clazz) {
            super(repository);
            this.itemUuid = itemUuid;
            this.clazz = clazz;
        }

        /**
         * UUID of the deleted item.
         * 
         * @return the identifier if the deleted item
         */
        public final UUID getItemUuid() {
            return itemUuid;
        }

        /**
         * Kind of deleted item.
         * 
         * @return the class of the deleted item.
         */
        public final Class<? extends VvrItem> getClazz() {
            return clazz;
        }

    }

    /**
     * Event sent when an item is changed.
     * 
     * @author
     */
    public final class ItemChangedEvent extends ItemEvent {
        private final UUID itemUuid;
        private final String oldValue;
        private final String newValue;
        private final VvrItemAttributeType type;

        public enum VvrItemAttributeType {
            NAME, DESCRIPTION;
        }

        public ItemChangedEvent(final VersionedVolumeRepository repository, final UUID itemUuid,
                final VvrItemAttributeType type, final String oldValue, final String newValue) {
            super(repository);
            this.itemUuid = itemUuid;
            this.type = type;
            this.oldValue = oldValue;
            this.newValue = newValue;

        }

        /**
         * Gets Item uuid.
         * 
         * @return the identifier of the item.
         */
        public final UUID getItemUuid() {
            return itemUuid;
        }

        /**
         * Gets the old value.
         * 
         * @return the old value of the attribute
         */
        public final String getOldValue() {
            return oldValue;
        }

        /**
         * Gets the new value.
         * 
         * @return the new value of the attribute
         */
        public final String getNewValue() {
            return newValue;
        }

        /**
         * Gets the attribute type.
         * 
         * @return the attribute type
         */
        public final VvrItemAttributeType getType() {
            return type;
        }

    }
}
