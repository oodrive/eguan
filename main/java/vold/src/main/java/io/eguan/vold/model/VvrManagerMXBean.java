package io.eguan.vold.model;

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

import io.eguan.vvr.repository.core.api.VersionedVolumeRepository;

import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * JMX methods exposed by the {@link VvrManager}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public interface VvrManagerMXBean {

    /**
     * Create a new {@link Vvr}. The {@link UUID} is randomly generated.
     * 
     * @param name
     *            name of the VVR.
     * @param description
     *            description of the VVR.
     * @return The {@link UUID} of the created VVR.
     * @throws VvrManagementException
     */
    String createVvr(final String name, final String description) throws VvrManagementException;

    /**
     * Create a new {@link Vvr}. Its {@link UUID} is the one given.
     * 
     * @param name
     *            name of the VVR.
     * @param description
     *            description of the VVR.
     * @param uuid
     *            {@link UUID} of the {@link Vvr}.
     * @throws VvrManagementException
     */
    void createVvr(final String name, final String description, final @Nonnull String uuid)
            throws VvrManagementException;

    /**
     * Create a new {@link Vvr}. The {@link UUID} is randomly generated.
     * 
     * @param name
     *            name of the VVR.
     * @param description
     *            description of the VVR.
     * @return The {@link UUID} of the task handling the creation of the VVR.
     * @throws VvrManagementException
     */
    String createVvrNoWait(final String name, final String description) throws VvrManagementException;

    /**
     * Create a new {@link Vvr}. The {@link UUID} is randomly generated.
     * 
     * @param name
     *            name of the VVR.
     * @param description
     *            description of the VVR.
     * @param uuid
     *            {@link UUID} of the {@link Vvr}.
     * @return The {@link UUID} of the task handling the creation of the VVR.
     * @throws VvrManagementException
     */
    String createVvrNoWait(final String name, final String description, final @Nonnull String uuid)
            throws VvrManagementException;

    /**
     * Delete the VVR instance, wipe all stored data.
     * 
     * @param uuid
     *            uuid of the {@link Vvr} to delete.
     * @throws IllegalArgumentException
     *             if <code>uuid</code> does not match a VVR.
     */
    void delete(@Nonnull final String uuid) throws IllegalArgumentException;

    /**
     * Delete the VVR instance, wipe all stored data.
     * 
     * @param uuid
     *            uuid of the {@link Vvr} to delete.
     * @throws IllegalArgumentException
     *             if <code>uuid</code> does not match a VVR.
     * @return The {@link UUID} of the task handling the creation of the VVR.
     */
    String deleteNoWait(@Nonnull final String uuid) throws IllegalArgumentException;

    /**
     * Gets the UUID of the owner of the {@link VersionedVolumeRepository}s.<br>
     * TODO <code>NOTE:</code> TO REMOVE, the launcher of the VOLD is supposed to know this information.
     * 
     * @return the string of the {@link UUID} of the owner
     */
    String getOwnerUuid();

    /**
     * Gets a task for VVR manager resource.
     * 
     * @param taskId
     *            UUID of the {@link VvrManagerTaskInfo} to get.
     * @return The {@link VvrManagerTask} of the task or null if no task is found for this resource manager.
     */
    public VvrManagerTask getVvrManagerTask(@Nonnull final String taskId);

    /**
     * Gets the list of tasks for VVR manager resource.
     * 
     * @param taskId
     *            UUID of the {@link VvrManagerTask} to get.
     * @return The {@link VvrManagerTask} of the task.
     */
    public VvrManagerTask[] getVvrManagerTasks();

    /**
     * Gets the Vvr configuration
     * 
     * @return a Map containing the configuration.
     */
    public Map<String, String> getVvrConfiguration();

}
