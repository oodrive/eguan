package io.eguan.dtx;

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

import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Client interface for external request submission and monitoring.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author llambert
 * 
 */
public interface DtxTaskApi {

    /**
     * Submits an operation on the given resource.
     * 
     * @param resourceId
     *            the ID of the target resource
     * @param payload
     *            the operation in binary format to be executed by the {@link DtxResourceManager}
     * @return the ID of the {@link DtxTask} created to represent the execution of the submitted operation
     * @throws IllegalStateException
     *             if the underlying DTX cluster is in a state where it cannot accept submissions
     */
    public UUID submit(UUID resourceId, byte[] payload) throws IllegalStateException;

    /**
     * Create or update a new task.
     * 
     * @param taskId
     *            the non-<code>null</code> task's ID
     * @param txId
     *            the transaction ID
     * @param resourceId
     *            ID of the resource manager
     * @param status
     *            the non-<code>null</code> new status of the task
     * @param info
     *            the {@link DtxTaskInfo} associated to the task
     */
    public void setTask(@Nonnull UUID taskId, long txId, @Nonnull UUID resourceId, @Nonnull DtxTaskStatus status,
            DtxTaskInfo info);

    /**
     * Gets the {@link DtxTaskInfo} associated to the given ID.
     * 
     * @param taskId
     *            the requested task's ID
     * @return the dtxTaskInfo representing its current info, <code>null</code> otherwise
     */
    public DtxTaskInfo getDtxTaskInfo(UUID taskId);

    /**
     * Gets the {@link DtxTask} associated to the given ID.
     * 
     * @param taskId
     *            the requested task's ID
     * @return the immutable {@link DtxTask} instance representing its current state, <code>null</code> otherwise
     */
    public DtxTaskAdm getTask(UUID taskId);

    /**
     * Get the list of the known tasks.
     * 
     * @return the list of the known tasks.
     */
    public DtxTaskAdm[] getTasks();

    /**
     * Get the list of the known tasks for a given resource manager.
     * 
     * @param resourceId
     *            the requested resource manager's {@link UUID}
     * 
     * @return the list of the known tasks.
     */
    public DtxTaskAdm[] getResourceManagerTasks(UUID resourceId);

    /**
     * Requests the cancellation of a task.
     * 
     * @param taskId
     *            the ID of the task to cancel
     * @return <code>false</code> if the task could not be cancelled, <code>true</code> otherwise
     * @throws IllegalStateException
     *             if the undelying DTX cluster is in a state where it cannot process cancellation requests
     */
    public boolean cancel(UUID taskId) throws IllegalStateException;

}
