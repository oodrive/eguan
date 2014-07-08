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

import static com.oodrive.nuage.dtx.DtxResourceManagerState.POST_SYNC_PROCESSING;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UNDETERMINED;
import static com.oodrive.nuage.dtx.DtxResourceManagerState.UP_TO_DATE;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.oodrive.nuage.dtx.events.DtxResourceManagerEvent;

/**
 * Class for processing post-sync jobs.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class PostSyncProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostSyncProcessor.class);

    private final Set<UUID> runningPostProcSet = new HashSet<UUID>();
    private final Set<UUID> resetAfterPostProcSet = new HashSet<UUID>();

    /**
     * Registers the reset of the target resource manager to an {@link DtxResourceManagerState#UNDETERMINED} state after
     * completing post-synchronization.
     * 
     * @param resUuid
     *            the target resource managers {@link UUID}
     */
    final void resetAfterPostProc(final UUID resUuid) {
        synchronized (runningPostProcSet) {
            if (!runningPostProcSet.contains(resUuid)) {
                return;
            }
            synchronized (resetAfterPostProcSet) {
                resetAfterPostProcSet.add(resUuid);
            }
        }
    }

    /**
     * Intercepts {@link DtxResourceManagerEvent}s for {@link DtxResourceManagerState#POST_SYNC_PROCESSING} resource
     * managers and executes all registered post-sync jobs for that resource manager.
     * 
     * @param event
     *            the posted {@link DtxResourceManagerEvent}
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void handleDtxResourceManagerEvent(@Nonnull final DtxResourceManagerEvent event) {
        final DtxResourceManagerState newState = event.getNewState();

        if (POST_SYNC_PROCESSING != newState) {
            // not post-processing, nothing to do here
            return;
        }

        final UUID resId = event.getResourceManagerId();

        synchronized (runningPostProcSet) {
            runningPostProcSet.add(resId);
        }

        final DtxManager dtxManager = event.getSource();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Post-synchronization processing starting; node=" + dtxManager.getNodeId() + ", resourceId="
                    + resId);
        }

        final TransactionManager txManager = dtxManager.getTxManager();
        if (txManager == null) {
            // fail graciously if no transaction manager is available
            LOGGER.warn("Transaction manager is null, aborting post-sync processing");
            return;
        }

        final DtxResourceManager targetResMgr = txManager.getRegisteredResourceManager(resId);
        if (targetResMgr == null) {
            LOGGER.warn("Target resource manager is null, aborting post-sync processing");
            return;
        }

        // attempt to execute the post-sync method
        boolean success = true;
        try {
            targetResMgr.processPostSync();
        }
        catch (final Throwable e) {
            success = false;
        }

        DtxResourceManagerState targetState = success ? UP_TO_DATE : UNDETERMINED;

        synchronized (runningPostProcSet) {
            synchronized (resetAfterPostProcSet) {
                if (resetAfterPostProcSet.contains(resId)) {
                    targetState = UNDETERMINED;
                    resetAfterPostProcSet.remove(resId);
                }
            }
            runningPostProcSet.remove(resId);
        }

        txManager.setResManagerSyncState(resId, targetState);
    }
}
