package io.eguan.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.dtx.DtxManager.ManagedDtxContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

/**
 * {@link Callable} implementing {@link HazelcastInstanceAware} designed to retrieve updated information on a specific
 * resource manager.
 * 
 * This is intended for discovery purposes whenever a {@link DtxResourceManager} is registered with one of the nodes.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class NodeDiscoveryHandler implements Callable<Map<UUID, Long>>, Serializable, HazelcastInstanceAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeDiscoveryHandler.class);

    private static final long serialVersionUID = 8340190292989909718L;

    private HazelcastInstance hazelcastInstance;

    private final Map<UUID, Long> discoveryMap;

    private final DtxNode targetNode;

    /**
     * Constructs an immutable instance.
     * 
     * @param discoveryMap
     *            the list of resource manager IDs mapped to their respective last completed transaction IDs
     * @param targetNode
     *            the node this handler is expected to run on
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    NodeDiscoveryHandler(final Map<UUID, Long> discoveryMap, final DtxNode targetNode) throws NullPointerException {
        this.discoveryMap = Objects.requireNonNull(discoveryMap);
        this.targetNode = targetNode;
    }

    @Override
    public final void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Setting Hazelcast instance; instance=" + hazelcastInstance);
        }
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public final Map<UUID, Long> call() throws Exception {

        final TransactionManager targetTxMgr = ((ManagedDtxContext) hazelcastInstance.getConfig().getManagedContext())
                .getTransactionManager();

        final DtxNode localNode = targetTxMgr.getLocalNode();

        // verify we're on the right node
        if (!localNode.equals(targetNode)) {
            throw new IllegalStateException("Running on the wrong node; target node=" + targetNode + ", found node="
                    + localNode);
        }

        final HashMap<UUID, Long> result = new HashMap<UUID, Long>();

        for (final UUID currResMgrId : discoveryMap.keySet()) {
            // checks if the resource manager is present
            if (targetTxMgr.getRegisteredResourceManager(currResMgrId) == null) {
                continue;
            }
            // updates the target's sync status
            targetTxMgr.evaluateResManagerSyncState(currResMgrId, discoveryMap.get(currResMgrId).longValue(), false);

            // gets the value to take home
            final long lastTxValue = targetTxMgr.getLastCompleteTxIdForResMgr(currResMgrId);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found last transaction value; node=" + localNode + ", resId=" + currResMgrId
                        + ", last txId=" + lastTxValue);
            }
            result.put(currResMgrId, Long.valueOf(lastTxValue));
        }
        return result;
    }

}
