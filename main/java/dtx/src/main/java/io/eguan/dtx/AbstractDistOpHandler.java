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

import io.eguan.dtx.DtxManager.ManagedDtxContext;
import io.eguan.dtx.proto.TxProtobufUtils;
import io.eguan.proto.dtx.DistTxWrapper.TxNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;

/**
 * Abstract superclass for {@link HazelcastInstanceAware} distributed tasks with participant recording.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
// TODO: factor out anything specific to distributed transaction execution and make NodeIdUpdateHandler extend this
abstract class AbstractDistOpHandler implements HazelcastInstanceAware, Serializable {

    private static final long serialVersionUID = 2251209817819094363L;

    private HazelcastInstance hazelcastInstance;

    /*
     * Note: Hazelcast uses Java serialization to transfer objects and protobuf objects implement Serializable using
     * their binary format. Ergo, we just use protobuf types here instead of serializing explicitly.
     */
    private final List<TxNode> participants = new ArrayList<TxNode>();

    /**
     * Internal constructor to be called by extending classes.
     * 
     * @param participants
     *            the {@link Set} of participating {@link DtxNode}s
     */
    public AbstractDistOpHandler(final Set<DtxNode> participants) {

        if (participants == null) {
            return;
        }

        for (final DtxNode currNode : participants) {
            this.participants.add(TxProtobufUtils.toTxNode(currNode));
        }
    }

    @Override
    public final void setHazelcastInstance(@Nonnull final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = Objects.requireNonNull(hazelcastInstance);
    }

    /**
     * Gets the local {@link TransactionManager}.
     * 
     * @return the transaction manager instance or <code>null</code> if it could not be retrieved
     */
    protected final TransactionManager getTransactionManager() {

        if (hazelcastInstance == null) {
            return null;
        }

        final ManagedContext managedContext = hazelcastInstance.getConfig().getManagedContext();
        if (!(managedContext instanceof ManagedDtxContext)) {
            return null;
        }

        return ((ManagedDtxContext) managedContext).getTransactionManager();
    }

    /**
     * Gets the local node ID.
     * 
     * @return the {@link UUID} of this node or <code>null</code> if it could not be retrieved
     */
    protected final UUID getNodeId() {
        if (hazelcastInstance == null) {
            return null;
        }

        final ManagedContext managedContext = hazelcastInstance.getConfig().getManagedContext();
        if (!(managedContext instanceof ManagedDtxContext)) {
            return null;
        }

        return ((ManagedDtxContext) managedContext).getNodeId();
    }

    /**
     * Gets the list of participants.
     * 
     * @return (possibly empty) {@link List}t of {@link TxNode}s
     */
    protected final List<TxNode> getParticipants() {
        return participants;
    }

    /**
     * Gets the local hazelcast instance.
     * 
     * @return a non-<code>null</code> {@link HazelcastInstance}
     */
    final HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

}
