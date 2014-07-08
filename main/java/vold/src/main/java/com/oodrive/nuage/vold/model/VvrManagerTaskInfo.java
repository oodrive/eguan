package com.oodrive.nuage.vold.model;

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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.oodrive.nuage.dtx.DtxTaskInfo;

/**
 * Immutable representation of a Vvr manager task submitted to a DTX cluster. This version is exported as a MXBean
 * attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class VvrManagerTaskInfo extends DtxTaskInfo {

    private final VvrManagerTaskOperation operation;
    private final VvrManagerTargetType targetType;
    private final String targetId;

    /**
     * Constructs information for vvr manager task.
     * 
     * @param resourceId
     *            The globally unique ID of the resourceId
     * @param source
     *            The source node which instanciate the task
     * @param targetId
     *            The ID of the result target for the task
     * @param operation
     *            The operation for the task.
     * @param targetType
     *            The type of target which is manipulated by the task
     * @throws NullPointerException
     *             If any of the {@link Nonnull} parameters is <code>null</code>
     */

    @ConstructorProperties({ "source", "operation", "targetType", "targetId" })
    public VvrManagerTaskInfo(@Nonnull final String source, @Nonnull final VvrManagerTaskOperation operation,
            @Nonnull final VvrManagerTargetType targetType, final String targetId) {
        super(source);
        this.operation = Objects.requireNonNull(operation);
        this.targetType = Objects.requireNonNull(targetType);
        this.targetId = targetId;
    }

    /**
     * Gets the Operation of the task. May be not be null.
     * 
     * @return the operation of the task
     */
    public final VvrManagerTaskOperation getOperation() {
        return this.operation;
    }

    /**
     * Gets the target type of the task. May be not be null.
     * 
     * @return the type of the target
     */
    public final VvrManagerTargetType getTargetType() {
        return this.targetType;
    }

    /**
     * Gets the target ID of the task. May be not be null.
     * 
     * @return the ID of the target
     */
    public final String getTargetId() {
        return this.targetId;
    }
}
