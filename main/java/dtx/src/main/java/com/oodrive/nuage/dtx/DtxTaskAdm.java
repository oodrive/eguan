package com.oodrive.nuage.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of a task submitted to a DTX cluster. This version is exported as a MXBean attribute.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 * 
 */
@Immutable
public final class DtxTaskAdm {

    private final String taskId;
    private final String name;
    private final String description;
    private final String resourceId;
    private final DtxTaskStatus status;

    /**
     * Constructs an immutable instance.
     * 
     * @param taskId
     *            the globally unique ID of this task
     * @param name
     *            name of the task. May be <code>null</code>
     * @param description
     *            description of the task. May be <code>null</code>
     * @param resourceId
     *            the optional resource manager's {@link UUID}
     * @param status
     *            the current status of the underlying transaction
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */
    DtxTaskAdm(@Nonnull final UUID taskId, final String name, final String description, final UUID resourceId,
            @Nonnull final DtxTaskStatus status) {
        this(taskId.toString(), name, description, resourceId == null ? null : resourceId.toString(), status);
    }

    /**
     * Constructs an immutable instance.
     * 
     * @param taskId
     *            the globally unique ID of this task
     * @param name
     *            name of the task. May be <code>null</code>
     * @param description
     *            description of the task. May be <code>null</code>
     * @param resourceId
     *            the optional resource manager's ID
     * @param status
     *            the current status of the underlying transaction
     * @throws NullPointerException
     *             if any of the {@link Nonnull} parameters is <code>null</code>
     */
    @ConstructorProperties({ "taskId", "name", "description", "resourceId", "status" })
    public DtxTaskAdm(@Nonnull final String taskId, final String name, final String description,
            final String resourceId, @Nonnull final DtxTaskStatus status) {
        super();
        this.taskId = Objects.requireNonNull(taskId);
        // Convert null strings to empty strings
        this.name = name == null ? "" : name;
        this.description = description == null ? "" : description;
        this.status = Objects.requireNonNull(status);
        this.resourceId = resourceId == null ? "" : resourceId;
    }

    /**
     * Gets the globally unique ID of this instance.
     * 
     * @return the non-<code>null</code> {@link UUID} of this task
     */
    public final String getTaskId() {
        return taskId;
    }

    /**
     * Gets the name of the task. May be empty.
     * 
     * @return the name of the task
     */
    public final String getName() {
        return name;
    }

    /**
     * Gets the description of the task. May be empty.
     * 
     * @return the description of the task
     */
    public final String getDescription() {
        return description;
    }

    /**
     * Gets the resource identifier of the task. May be empty.
     * 
     * @return the resource ID of the task
     */
    public final String getResourceId() {
        return resourceId;
    }

    /**
     * Gets the current status of the task.
     * 
     * @return a {@link DtxTaskStatus} representing the last known state
     */
    public final DtxTaskStatus getStatus() {
        return this.status;
    }

}
