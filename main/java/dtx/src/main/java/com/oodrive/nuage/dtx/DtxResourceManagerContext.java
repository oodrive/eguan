package com.oodrive.nuage.dtx;

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

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Interface for transaction context encapsulation.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public abstract class DtxResourceManagerContext {

    private final UUID resourceManagerId;
    private DtxTaskStatus txStatus;

    /**
     * Gets the unique ID of the {@link DtxResourceManager} this context belongs to.
     * 
     * @return a {@link UUID}
     */
    @Nonnull
    public final UUID getResourceManagerId() {
        return resourceManagerId;
    }

    /**
     * Gets the status of the transaction whose state is stored in this context.
     * 
     * Defaults to {@link DtxTaskStatus#PENDING} if none was set.
     * 
     * @return a {@link DtxTaskStatus} literal
     */
    @Nonnull
    public final DtxTaskStatus getTxStatus() {
        return this.txStatus;
    }

    /**
     * Sets the transaction status associated to this context.
     * 
     * @param status
     *            a non-<code>null</code> {@link DtxTaskStatus}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public final void setTxStatus(@Nonnull final DtxTaskStatus status) throws NullPointerException {
        this.txStatus = Objects.requireNonNull(status);
    }

    /**
     * Internal constructor adding the resource manager's ID to the context.
     * 
     * The status returned by {@link #getTxStatus()} defaults to {@link DtxTaskStatus#PENDING}.
     * 
     * @param resourceManagerId
     *            the non-<code>null</code> {@link UUID} of the resource manager
     * @throws NullPointerException
     *             if the resource manager ID parameter is <code>null</code>
     */
    protected DtxResourceManagerContext(@Nonnull final UUID resourceManagerId) throws NullPointerException {
        this(resourceManagerId, null);
    }

    /**
     * Internal constructor adding the resource manager's ID and initial state to the context.
     * 
     * @param resourceManagerId
     *            the non-<code>null</code> {@link UUID} of the resource manager
     * @param initialStatus
     *            the {@link DtxTaskStatus} to set, will default to {@link DtxTaskStatus#PENDING} if given
     *            <code>null</code>
     * @throws NullPointerException
     *             if the resource manager ID parameter is <code>null</code>
     */
    protected DtxResourceManagerContext(@Nonnull final UUID resourceManagerId, final DtxTaskStatus initialStatus)
            throws NullPointerException {
        this.resourceManagerId = Objects.requireNonNull(resourceManagerId);
        this.txStatus = initialStatus == null ? DtxTaskStatus.PENDING : initialStatus;
    }

}
