package com.oodrive.nuage.dtx.events;

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

import javax.annotation.ParametersAreNonnullByDefault;

import com.oodrive.nuage.dtx.DtxManager;
import com.oodrive.nuage.dtx.DtxNode;

/**
 * An event relating to any DTX cluster node.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxClusterEvent extends DtxEvent<DtxManager> {

    /**
     * Cluster membership event types.
     * 
     * 
     */
    public enum DtxClusterEventType {
        /**
         * Type used whenever a new registered node comes online.
         */
        ADDED,
        /**
         * Type used when a registered node goes offline.
         */
        REMOVED;
    }

    private final DtxClusterEventType type;

    private final DtxNode node;

    /**
     * Constructs an event relating to any DTX cluster node.
     * 
     * @param source
     *            the {@link DtxManager} triggering the event
     * @param type
     *            the {@link DtxClusterEventType} describing the event
     * @param node
     *            the affected {@link DtxNode}
     * @param quorumOnline
     *            <code>true</code> if the quorum is online after the event occurred, <code>false</code> otherwise
     * @throws NullPointerException
     *             if any argument is <code>null</code>
     */
    @ParametersAreNonnullByDefault
    public DtxClusterEvent(final DtxManager source, final DtxClusterEventType type, final DtxNode node,
            final boolean quorumOnline) throws NullPointerException {
        super(Objects.requireNonNull(source), System.currentTimeMillis());
        this.type = Objects.requireNonNull(type);
        this.node = Objects.requireNonNull(node);
    }

    /**
     * Gets the type describing the event.
     * 
     * @return a {@link DtxClusterEventType}
     */
    public final DtxClusterEventType getType() {
        return type;
    }

    /**
     * Gets the node affected by the event.
     * 
     * @return a non-<code>null</code> {@link DtxNode}
     */
    public final DtxNode getNode() {
        return node;
    }

    @Override
    public final String toString() {
        return toStringHelper().add("type", type).add("node", node).toString();
    }

}
