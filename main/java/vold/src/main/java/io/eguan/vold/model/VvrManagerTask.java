package io.eguan.vold.model;

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

import io.eguan.dtx.DtxTaskStatus;

import java.beans.ConstructorProperties;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of a Vvr manager task submitted to a DTX cluster. This version is exported as a MXBean
 * attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class VvrManagerTask {

    private final String taskId;
    private final DtxTaskStatus status;
    private final VvrManagerTaskInfo info;

    /**
     * Constructs an immutable instance of a vvr manager task.
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
    public VvrManagerTask(@Nonnull final String taskId, @Nonnull final DtxTaskStatus status,
            final VvrManagerTaskInfo info) {
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
     * @return a {@link io.eguan.vvr.persistence.repository.VvrTaskInfo} representing the additional info of
     *         this task
     */
    public final VvrManagerTaskInfo getInfo() {
        return info;
    }

}
