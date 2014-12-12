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

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link ThreadSafe} record encapsulating a submitted request.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@ThreadSafe
final class Request {

    private final UUID resourceId;
    private final UUID taskId;
    private final byte[] payload;
    @GuardedBy("this")
    private volatile DtxTaskStatus currentTxStatus;

    /**
     * Constructs an immutable request instance.
     * 
     * @param resourceId
     *            the destination resource manager's ID
     * @param taskId
     *            the client-side task ID associated to this request
     * @param payload
     *            the opaque, non-<code>null</code> request payload
     * @throws NullPointerException
     *             if any of the arguments is <code>null</code>
     */
    @ParametersAreNonnullByDefault
    Request(final UUID resourceId, final UUID taskId, final byte[] payload) throws NullPointerException {
        super();
        this.resourceId = Objects.requireNonNull(resourceId);
        this.taskId = Objects.requireNonNull(taskId);
        this.payload = Arrays.copyOf(Objects.requireNonNull(payload), payload.length);
        this.currentTxStatus = DtxTaskStatus.PENDING;
    }

    /**
     * Gets the resource manager's ID.
     * 
     * @return a non-<code>null</code> {@link UUID}
     */
    final UUID getResourceId() {
        return resourceId;
    }

    /**
     * Gets the public task ID.
     * 
     * @return a non-<code>null</code> {@link UUID}
     */
    final UUID getTaskId() {
        return taskId;
    }

    /**
     * Gets the opaque payload.
     * 
     * @return a copy of the binary payload
     */
    final byte[] getPayload() {
        return Arrays.copyOf(payload, payload.length);
    }

    /**
     * Gets the task status associated with the request.
     * 
     * @return a {@link DtxTaskStatus}
     */
    final synchronized DtxTaskStatus getTaskStatus() {
        return currentTxStatus;
    }

    /**
     * Sets the task status.
     * 
     * @param newTxStatus
     *            a valid {@link DtxTaskStatus}
     */
    final synchronized void setTaskStatus(final DtxTaskStatus newTxStatus) {
        this.currentTxStatus = newTxStatus;
    }

    @Override
    public final String toString() {
        return toStringHelper(this).add("taskId", taskId).add("resourceId", resourceId).add("status", currentTxStatus)
                .toString();
    }
}
