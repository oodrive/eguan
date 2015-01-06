package com.oodrive.nuage.vold.model;

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

import java.beans.ConstructorProperties;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.oodrive.nuage.dtx.DtxTaskStatus;
import com.oodrive.nuage.vvr.persistence.repository.VvrTaskInfo;

public final class VvrTask {

    private final String taskId;
    private final DtxTaskStatus status;
    private final VvrTaskInfo info;

    /**
     * Constructs an immutable instance.
     * 
     * @param taskId
     *            the globally unique ID of this task
     * @param status
     *            the current status of the underlying transaction
     * @param info
     *            extended parameter of the task
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */

    @ConstructorProperties({ "taskId", "status", "info" })
    public VvrTask(@Nonnull final String taskId, @Nonnull final DtxTaskStatus status, final VvrTaskInfo info) {
        this.taskId = Objects.requireNonNull(taskId);
        this.status = Objects.requireNonNull(status);
        this.info = info;
    }

    /**
     * Gets the ID for a task. May not be null.
     * 
     * @return a {@link String} representing the globally unique ID of this task
     */

    public final String getTaskId() {
        return taskId;
    }

    /**
     * Gets the status of the task.
     * 
     * @return the status of the task.
     */
    public final DtxTaskStatus getStatus() {
        return status;
    }

    /**
     * Gets the info of the task. May be null.
     * 
     * @return a {@link VvrTaskInfo} representing the additionnal info of this task
     */
    public final VvrTaskInfo getInfo() {
        return info;
    }

}
