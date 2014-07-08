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

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.oodrive.nuage.dtx.DtxManager.ManagedDtxContext;
import com.oodrive.nuage.dtx.proto.TxProtobufUtils;
import com.oodrive.nuage.proto.Common.Uuid;

/**
 * {@link Callable} implementing {@link HazelcastInstanceAware} designed to retrieve the ID of the node it runs on.
 * 
 * This is intended for discovery purposes whenever a node joins the Hazelcast cluster.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class NodeIdUpdateHandler implements Callable<Uuid>, Serializable, HazelcastInstanceAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeIdUpdateHandler.class);

    private static final long serialVersionUID = 8340190292989909718L;

    private HazelcastInstance hazelcastInstance;

    @Override
    public final void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Setting Hazelcast instance; instance=" + hazelcastInstance);
        }
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public final Uuid call() throws Exception {
        final UUID targetId = ((ManagedDtxContext) hazelcastInstance.getConfig().getManagedContext()).getNodeId();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting target node ID; nodeID=" + targetId);
        }
        return TxProtobufUtils.toUuid(((ManagedDtxContext) hazelcastInstance.getConfig().getManagedContext())
                .getNodeId());
    }

}
