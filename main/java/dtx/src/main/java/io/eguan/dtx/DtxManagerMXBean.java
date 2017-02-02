package io.eguan.dtx;

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

/**
 * MXBean definitions for the {@link DtxManager}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * @author pwehrle
 * 
 */
public interface DtxManagerMXBean {

    /**
     * Gets the list of tasks.
     * 
     * @return a read-only view of some attributes of the tasks submitted on the server.
     */
    DtxTaskAdm[] getTasks();

    /**
     * Gets the list of registered resources managers.
     * 
     * @return a read-only view of the resource managers registered on the dtx manager.
     */
    DtxResourceManagerAdm[] getResourceManagers();

    /**
     * Gets information on the current request queue.
     * 
     * @return a read-only view of the request queue on the dtx manager.
     */
    DtxRequestQueueAdm getRequestQueue();

    /**
     * Gets the list of tasks for a given resource manager.
     * 
     * @param resourceId
     *            the requested resource manager's ID
     * 
     * @return a read-only view of some attributes of the tasks submitted on the server. if no task, returns an empty
     *         array.
     * 
     */
    DtxTaskAdm[] getResourceManagerTasks(final String resourceId);

    /**
     * Gets a task from its ID.
     * 
     * @param taskId
     *            the ID associated to the task
     * @return a read-only view of some attributes of the task submitted on the server.
     */
    DtxTaskAdm getTask(final String taskId);

    /**
     * Cancels a task.
     * 
     * @param taskId
     *            the ID associated to the task
     * @return <code>false</code> if the task could not be cancelled, <code>true</code> otherwise
     */
    boolean cancelTask(final String taskId);

    /**
     * Restart a {@link DtxManager}
     * 
     */
    void restart();

}
