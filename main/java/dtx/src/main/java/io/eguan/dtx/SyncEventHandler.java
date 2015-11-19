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

import static io.eguan.dtx.DtxResourceManagerState.LATE;
import static io.eguan.dtx.DtxResourceManagerState.SYNCHRONIZING;
import static io.eguan.dtx.DtxResourceManagerState.UNDETERMINED;
import io.eguan.dtx.events.DtxResourceManagerEvent;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

/**
 * Event handler for triggering synchronization actions.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class SyncEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncEventHandler.class);

    /**
     * Intercepts {@link DtxResourceManagerEvent}s for {@link DtxResourceManagerState#LATE} resource managers and
     * triggers synchronization.
     * 
     * @param event
     *            the posted {@link DtxResourceManagerEvent}
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void handleDtxResourceManagerEvent(@Nonnull final DtxResourceManagerEvent event) {
        final DtxResourceManagerState newState = event.getNewState();

        if (LATE != newState) {
            // not late, i.e. do not trigger synchronization
            return;
        }

        final UUID resId = event.getResourceManagerId();

        final DtxManager dtxManager = event.getSource();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Triggering synchronization; node=" + dtxManager.getNodeId() + ", resourceId=" + resId);
        }

        final TransactionManager txManager = dtxManager.getTxManager();

        if (txManager == null) {
            LOGGER.warn("Transaction manager is null, abandoning synchronization");
            return;
        }

        txManager.setResManagerSyncState(resId, SYNCHRONIZING);

        try {
            long lastLocalTxId = txManager.getLastCompleteTxIdForResMgr(resId);

            final Map<DtxNode, Long> updateMap = dtxManager.getClusterMapInfo(resId, Long.valueOf(lastLocalTxId));

            if (updateMap.isEmpty()) {
                return;
            }

            final long maxTxId = Collections.max(updateMap.values()).longValue();

            // TODO: choose update source more wisely
            final Iterator<DtxNode> nodeIter = updateMap.keySet().iterator();

            while (nodeIter.hasNext() && (lastLocalTxId < maxTxId)) {
                final DtxNode targetNode = nodeIter.next();
                final long targetNodeLastTxId = updateMap.get(targetNode).longValue();
                if (targetNodeLastTxId > lastLocalTxId) {
                    lastLocalTxId = dtxManager
                            .synchronizeWithNode(resId, targetNode, lastLocalTxId, targetNodeLastTxId);
                }
            }
        }
        finally {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Synchronization finished; node=" + dtxManager.getNodeId() + ", resourceId=" + resId);
            }
            txManager.setResManagerSyncState(resId, UNDETERMINED);
        }
    }

}
