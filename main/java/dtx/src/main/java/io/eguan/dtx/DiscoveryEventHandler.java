package io.eguan.dtx;

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

import static io.eguan.dtx.DtxNodeState.STARTED;
import static io.eguan.dtx.DtxResourceManagerState.UNDETERMINED;
import io.eguan.dtx.events.DtxClusterEvent;
import io.eguan.dtx.events.DtxNodeEvent;
import io.eguan.dtx.events.DtxResourceManagerEvent;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.annotation.Nonnull;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

/**
 * Handler for all events triggering discovery actions.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class DiscoveryEventHandler {

    /**
     * Handles any {@link DtxClusterEvent} and launches discovery where necessary.
     * 
     * @param event
     *            the input {@link DtxClusterEvent}
     * @throws InterruptedException
     *             if handling is interrupted while waiting on the {@link DtxManager#getStatusReadLock() status lock}
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void handleClusterEvent(@Nonnull final DtxClusterEvent event) throws InterruptedException {

        switch (event.getType()) {
        case ADDED:
            doAdded(event);
            break;
        case REMOVED:
            doRemoved(event);
            break;
        default:
            // nothing
        }
    }

    private final void doAdded(final DtxClusterEvent event) throws InterruptedException {
        // gets the context references from the source
        final DtxManager dtxManager = event.getSource();
        assert (dtxManager != null);

        final TransactionManager txMgr = dtxManager.getTxManager();
        final ReadLock readLock = dtxManager.getStatusReadLock();

        readLock.lockInterruptibly();
        try {
            if (!STARTED.equals(dtxManager.getStatus())) {
                // do nothing
                return;
            }

            // discover status of all local resource managers on newly added node
            final HashMap<UUID, Long> discoveryMap = new HashMap<UUID, Long>();
            for (final DtxResourceManager currResMgr : txMgr.getRegisteredResourceManagers()) {
                final UUID resId = currResMgr.getId();
                discoveryMap.put(resId, Long.valueOf(txMgr.getLastCompleteTxIdForResMgr(resId)));
            }
            dtxManager.discoverResMgrStatus(discoveryMap, event.getNode());

            // discover status of all undetermined resource managers on all online nodes
            dtxManager.discoverResMgrStatus(txMgr.getUndeterminedResourceManagers());
        }
        finally {
            readLock.unlock();
        }
    }

    private final void doRemoved(@Nonnull final DtxClusterEvent event) throws InterruptedException {
        // gets the context references from the source
        final DtxManager dtxManager = event.getSource();
        assert (dtxManager != null);

        final DtxNode node = event.getNode();
        final ReadLock readLock = dtxManager.getStatusReadLock();

        readLock.lockInterruptibly();
        try {
            if (!STARTED.equals(dtxManager.getStatus())) {
                // do nothing
                return;
            }
            dtxManager.removeClusterMapInfo(node);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Handles any {@link DtxResourceManagerEvent} leading to discovery operations including the target resource
     * manager.
     * 
     * @param event
     *            the input {@link DtxResourceManagerEvent}
     * @throws InterruptedException
     *             if handling is interrupted while waiting on the {@link DtxManager#getStatusReadLock() status lock}
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void handleResourceManagerEvent(@Nonnull final DtxResourceManagerEvent event)
            throws InterruptedException {

        final DtxResourceManagerState newState = event.getNewState();

        if (UNDETERMINED != newState) {
            // limit discover to undetermined state
            return;
        }

        final DtxManager dtxManager = event.getSource();
        assert (dtxManager != null);

        final TransactionManager txMgr = dtxManager.getTxManager();
        final ReadLock readLock = dtxManager.getStatusReadLock();

        readLock.lockInterruptibly();
        try {
            if (STARTED != dtxManager.getStatus()) {
                // do nothing if not started
                return;
            }

            // discover the new resource manager's status on all online nodes
            final UUID resId = event.getResourceManagerId();
            final HashMap<UUID, Long> discoveryMap = new HashMap<UUID, Long>();
            discoveryMap.put(resId, Long.valueOf(txMgr.getLastCompleteTxIdForResMgr(resId)));

            dtxManager.discoverResMgrStatus(discoveryMap);
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * Handles any {@link DtxNodeEvent} leading to discovery operations on the target node.
     * 
     * @param event
     *            the input {@link DtxNodeEvent}
     * @throws InterruptedException
     *             if handling is interrupted while waiting on the {@link DtxManager#getStatusReadLock() status lock}
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void handleDtxNodeEvent(@Nonnull final DtxNodeEvent event) throws InterruptedException {
        final DtxNodeState newState = event.getNewState();

        if (!STARTED.equals(newState)) {
            // do nothing
            return;
        }

        // gets the context references from the source
        final DtxManager dtxManager = event.getSource();
        assert (dtxManager != null);

        final TransactionManager txMgr = dtxManager.getTxManager();
        final ReadLock readLock = dtxManager.getStatusReadLock();

        readLock.lockInterruptibly();
        try {
            // goes on to discover synchronization states for all resource managers
            final HashMap<UUID, Long> discoveryMap = new HashMap<UUID, Long>();

            for (final DtxResourceManager currResMgr : txMgr.getRegisteredResourceManagers()) {
                final UUID resId = currResMgr.getId();
                discoveryMap.put(resId, Long.valueOf(txMgr.getLastCompleteTxIdForResMgr(resId)));
            }
            dtxManager.discoverResMgrStatus(discoveryMap);
        }
        finally {
            readLock.unlock();
        }
    }
}
