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

import com.google.common.eventbus.EventBus;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;

/**
 * Custom {@link LifecycleListener} implementation to forward {@link LifecycleEvent}s from a given
 * {@link HazelcastInstance} to an {@link EventBus}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class HazelcastToEvtBusLifecycleConverter implements LifecycleListener {

    private final String nodeId;
    private final EventBus[] destBusses;
    private LifecycleState previousState;

    private HazelcastInstance hzInstance;

    /**
     * Constructs an instance relaying events from the source {@link HazelcastInstance} to the destination
     * {@link EventBus}.
     * 
     * @param nodeId
     *            the node ID to determine the source {@link HazelcastInstance}
     * @param destinations
     *            the target {@link EventBus}es
     * @throws NullPointerException
     *             if any argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the {@link HazelcastInstance} cannot be used to subscribe to its life cycle events
     */
    @ParametersAreNonnullByDefault
    public HazelcastToEvtBusLifecycleConverter(final UUID nodeId, final EventBus... destinations)
            throws NullPointerException, IllegalArgumentException {
        this.destBusses = Objects.requireNonNull(destinations);
        this.nodeId = nodeId.toString();
    }

    @Override
    public final void stateChanged(final LifecycleEvent event) {
        final LifecycleState newState = event.getState();
        if (newState == previousState) {
            return;
        }
        if (hzInstance == null) {
            hzInstance = Hazelcast.getHazelcastInstanceByName(nodeId);
        }

        final HazelcastNodeEvent newEvent = new HazelcastNodeEvent(hzInstance, previousState, newState);
        previousState = newState;

        for (final EventBus destBus : this.destBusses) {
            destBus.post(newEvent);
        }
    }
}
