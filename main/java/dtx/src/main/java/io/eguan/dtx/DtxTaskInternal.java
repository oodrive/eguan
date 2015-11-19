package io.eguan.dtx;

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

import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Client interface for internal interaction with task keeper
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
interface DtxTaskInternal {

    /**
     * Start purge for the task keeper.
     * 
     */
    public void startPurgeTaskKeeper();

    /**
     * Stop purge for the task keeper.
     * 
     */
    public void stopPurgeTaskKeeper();

    /**
     * Load a new task.
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
     * @param timestamp
     *            the timestamp associated to the task
     */
    public void loadTask(@Nonnull UUID taskId, long txId, @Nonnull UUID resourceId, @Nonnull DtxTaskStatus status,
            DtxTaskInfo info, long timestamp);

    /**
     * Set the name and the description of the task. One or both may be <code>null</code>.
     * 
     * @param taskId
     *            the non-<code>null</code> requested task's ID
     * @param name
     *            name of the task, may be <code>null</code>
     * @param description
     *            description of the task, may be <code>null</code>
     */
    public void setTaskReadableId(@Nonnull UUID taskId, String name, String description);

    /**
     * Set the transaction id corresponding to the task ID
     * 
     * @param taskId
     *            the non-<code>null</code> requested task's ID
     * @param txId
     *            transaction ID of the task
     */
    public void setTaskTransactionId(@Nonnull UUID taskId, long txId);

    /**
     * Update status for a task.
     * 
     * @param taskId
     *            the requested task's ID
     * @param DtxTaskStatus
     *            status of the task
     */
    public void setTaskStatus(UUID taskId, DtxTaskStatus status);

    /**
     * Update status for a task.
     * 
     * @param transactionId
     *            ID the requested transaction ID
     * @param DtxTaskStatus
     *            status of the task
     */
    public void setTaskStatus(long transactionId, DtxTaskStatus status);

    /**
     * Sets the task info data.
     * 
     * @param taskId
     *            the requested task's ID
     * @param taskInfo
     *            some info coded in a {@link DtxTaskInfo}. May not be <code>null</code>
     */
    public void setDtxTaskInfo(UUID taskId, DtxTaskInfo taskInfo);

    /**
     * Return true if dtxTaskInfo is already set for a given task ID.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @return true is the dtxTaskInfo is not null, false otherwise.
     */
    public boolean isDtxTaskInfoSet(UUID taskId);

    /**
     * Gets the timestamp associated to the given ID.
     * 
     * @param taskId
     *            the requested task's ID
     * @return the current timestamp, <code>null</code> otherwise
     */
    public long getTaskTimestamp(UUID taskId);

}
