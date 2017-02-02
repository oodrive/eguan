package io.eguan.vold.model;

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

import io.eguan.vvr.persistence.repository.VvrTaskInfo;

/**
 * MXbean definition for {@link VvrOld}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public interface VvrMXBean {
    /**
     * Description setter.
     * 
     * @param description
     *            the new description of the VVR
     */
    void setDescription(final String description);

    /**
     * Description getter.
     * 
     * @return the description of the VVR
     */
    String getDescription();

    /**
     * Name setter.
     * 
     * @param name
     *            the new name of the VVR
     */
    void setName(final String name);

    /**
     * Name getter.
     * 
     * @return the name of the VVR
     */
    String getName();

    /**
     * Uuid getter.
     * 
     * @return the UUID of the VVR
     */
    String getUuid();

    /**
     * Uuid getter.
     * 
     * @return the UUID of the VVR
     */
    String getOwnerUuid();

    /**
     * Tells if the VVR is initialized.
     * 
     * @return <code>true</code> if initialized
     */
    boolean isInitialized();

    /**
     * Tells if the VVR is started.
     * 
     * @return <code>true</code> if started
     */
    boolean isStarted();

    /**
     * Start the VVR.
     */
    void start();

    /**
     * Start the VVR.
     * 
     * @return the UUID of the task that starts the VVR
     */
    String startNoWait();

    /**
     * Stop the VVR instance.
     */
    void stop();

    /**
     * Stop the VVR instance.
     * 
     * @return the UUID of the task that stops the VVR
     */
    String stopNoWait();

    /**
     * Gets a task for VVR resource.
     * 
     * @param taskId
     *            UUID of the {@link VvrTaskInfo} to get.
     * @return The {@link VvrTaskInfo} of the task or null if no task is found for this resource manager.
     */
    public VvrTask getVvrTask(final String taskId);

    /**
     * Gets a list task for VVR resource.
     * 
     * @param taskId
     *            UUID of the {@link VvrTask} to get.
     * @param resourceId
     *            UUID of the VVR.
     * @return The {@link VvrTask} of the task .
     */
    public VvrTask[] getVvrTasks();

}
