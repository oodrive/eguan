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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent.LifecycleState;

/**
 * Event related to the local node's membership in a Hazelcast cluster.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class HazelcastNodeEvent extends DtxEvent<HazelcastInstance> {

    private final LifecycleState previousState;
    private final LifecycleState newState;

    /**
     * Constructs a new event relative to a state change.
     * 
     * @param source
     *            the source {@link HazelcastInstance} of the event
     * @param previousState
     *            the {@link LifecycleState state} before the change, may be <code>null</code>
     * @param newState
     *            the new, non-<code>null</code> {@link LifecycleState state}
     */
    HazelcastNodeEvent(final HazelcastInstance source, final LifecycleState previousState,
            @Nonnull final LifecycleState newState) {
        super(source, System.currentTimeMillis());
        this.previousState = previousState;
        this.newState = Objects.requireNonNull(newState);
    }

    /**
     * Gets the starting state of the represented transition.
     * 
     * @return a valid {@link LifecycleState} or <code>null</code> if the previous state was undefined
     */
    public final LifecycleState getPreviousState() {
        return previousState;
    }

    /**
     * Gets the destination state of the represented transition.
     * 
     * @return a non-<code>null</code> {@link LifecycleState}
     */
    public final LifecycleState getNewState() {
        return newState;
    }

    @Override
    public final String toString() {
        return toStringHelper().add("previousState", previousState).add("newState", newState).toString();
    }

}
