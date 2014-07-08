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

import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of the request queue. This version is exported as a MXBean attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class DtxRequestQueueAdm {

    private final int nbOfPendingRequests;
    private final String nextTaskID;
    private final String nextResourceManagerID;

    /**
     * Constructs an immutable instance.
     * 
     * @param nbOfPendingRequests
     *            the number of pending request in the request queue
     * @param request
     *            the next request to execute in the request queue
     */
    DtxRequestQueueAdm(final int nbOfPendingRequests, final Request request) {
        this(nbOfPendingRequests, request == null ? null : request.getTaskId().toString(), request == null ? null
                : request.getResourceId().toString());
    }

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this resource manager
     * @param name
     *            the current status of the resource manager
     * @param name
     *            the current status of the resource manager
     */
    @ConstructorProperties({ "nbOfPendingRequests", "nextTaskID", "nextResourceManagerID" })
    public DtxRequestQueueAdm(final int nbOfPendingRequests, final String nextTaskId, final String nextResourceManagerID) {
        this.nbOfPendingRequests = nbOfPendingRequests;
        this.nextTaskID = nextTaskId == null ? "" : nextTaskId;
        this.nextResourceManagerID = nextResourceManagerID == null ? "" : nextResourceManagerID;
    }

    /**
     * Gets the nb of pending request in the request queue.
     * 
     * @return the number of pending request
     */
    public final int getNbOfPendingRequests() {
        return nbOfPendingRequests;
    }

    /**
     * Gets the next task ID in the pending request
     * 
     * @return the uuid of the next pending task
     */
    public final String getNextTaskID() {
        return nextTaskID;
    }

    /**
     * Gets the resource manager ID of the next request.
     * 
     * @return the uuid of the resource manager
     */
    public final String getNextResourceManagerID() {
        return nextResourceManagerID;
    }

}
