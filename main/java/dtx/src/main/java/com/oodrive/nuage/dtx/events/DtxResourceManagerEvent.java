package com.oodrive.nuage.dtx.events;

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

import java.util.Objects;
import java.util.UUID;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import com.oodrive.nuage.dtx.DtxManager;
import com.oodrive.nuage.dtx.DtxResourceManagerState;

/**
 * Event class to encapsulate resource manager synchronization state transitions.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class DtxResourceManagerEvent extends DtxEvent<DtxManager> {

    private final UUID resId;
    private final DtxResourceManagerState previousState;
    private final DtxResourceManagerState newState;

    /**
     * Constructs an instance for a specific resource manager ID and a transition between states.
     * 
     * @param source
     *            the source {@link DtxManager}
     * @param resMgrId
     *            the non-<code>null</code> affected resource manager's ID
     * @param previousState
     *            the initial {@link DtxResourceManagerState}
     * @param newState
     *            the {@link DtxResourceManagerState} obtained at the end of the transition
     * @throws NullPointerException
     *             if any argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if previous and new states are equal
     */
    @ParametersAreNonnullByDefault
    public DtxResourceManagerEvent(final DtxManager source, final UUID resMgrId,
            final DtxResourceManagerState previousState, final DtxResourceManagerState newState)
            throws NullPointerException {
        super(Objects.requireNonNull(source), System.currentTimeMillis());
        this.resId = Objects.requireNonNull(resMgrId);
        if (previousState == newState) {
            throw new IllegalArgumentException("Not a state transition; previous=" + previousState + ", new="
                    + newState);
        }
        this.previousState = Objects.requireNonNull(previousState);
        this.newState = Objects.requireNonNull(newState);
    }

    /**
     * Gets the affected resource manager's ID.
     * 
     * @return a non-<code>null</code> {@link UUID}
     */
    public final UUID getResourceManagerId() {
        return resId;
    }

    /**
     * Gets the resource manager's initial state.
     * 
     * @return the previousState a valid {@link DtxResourceManagerState}
     */
    public final DtxResourceManagerState getPreviousState() {
        return previousState;
    }

    /**
     * Gets the resource manager's destination state.
     * 
     * @return the newState a valid {@link DtxResourceManagerState}
     */
    public final DtxResourceManagerState getNewState() {
        return newState;
    }

    @Override
    public final String toString() {
        return toStringHelper().add("resourceId", resId).add("previousState", previousState).add("newState", newState)
                .toString();
    }

}
