package io.eguan.vvr.repository.core.api;

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

import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskApi;
import io.eguan.dtx.DtxTaskApiAbstract;
import io.eguan.dtx.DtxTaskStatus;
import io.eguan.dtx.config.DtxConfigurationContext;
import io.eguan.dtx.config.DtxTaskKeeperAbsoluteDurationConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperAbsoluteSizeConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperMaxDurationConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperMaxSizeConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperPurgeDelayConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperPurgePeriodConfigKey;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgServerRemoteStatus;
import io.eguan.net.MsgServerTimeoutException;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.vvr.configuration.keys.DeletedConfigKey;
import io.eguan.vvr.remote.VvrRemoteUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.eventbus.EventBus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

/**
 * Abstract superclass for all {@link VersionedVolumeRepository} implementations.
 * 
 * This class provides a {@link AbstractRepositoryImpl.Builder builder} taking a configuration parameter made available
 * via {@link #getConfiguration()}.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author ebredzinski
 * @author jmcaba
 * 
 */
public abstract class AbstractRepositoryImpl extends AbstractUniqueVvrObject implements VersionedVolumeRepository {

    /**
     * Implementation of the default Dtx task API. First 'naive' implementation: for now, all the tasks are considered
     * successful (which is not always true) and the tasks are executed by the current thread.
     * <p>
     * Used for unit tests in vvr.
     * 
     * 
     */
    private static final class DtxTaskApiImpl extends DtxTaskApiAbstract {

        private static MetaConfiguration defaultConfiguration;

        static {
            try {
                defaultConfiguration = MetaConfiguration.newConfiguration(new ByteArrayInputStream(new byte[0]),
                        DtxConfigurationContext.getInstance());
            }
            catch (ConfigValidationException | IOException e) {
                // Ignore
            }
        }

        private final VersionedVolumeRepository vvr;

        DtxTaskApiImpl(final VersionedVolumeRepository vvr) {

            super(new TaskKeeperParameters(DtxTaskKeeperAbsoluteDurationConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).longValue(), DtxTaskKeeperAbsoluteSizeConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).intValue(), DtxTaskKeeperMaxDurationConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).longValue(), DtxTaskKeeperMaxSizeConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).intValue(), DtxTaskKeeperPurgePeriodConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).longValue(), DtxTaskKeeperPurgeDelayConfigKey.getInstance()
                    .getTypedValue(defaultConfiguration).longValue()));
            this.vvr = vvr;
        }

        @Override
        public final UUID submit(final UUID resourceId, final byte[] payload) throws IllegalStateException {
            final RemoteOperation operation;
            try {
                operation = RemoteOperation.parseFrom(payload);
            }
            catch (final InvalidProtocolBufferException e) {
                throw new IllegalStateException(e);
            }
            vvr.handleMsg(operation);
            final UUID taskId = UUID.randomUUID();
            setTask(taskId, -1, resourceId, DtxTaskStatus.COMMITTED, null);
            return taskId;
        }

        @Override
        public final boolean cancel(final UUID taskId) throws IllegalStateException {
            // Should not get here
            throw new AssertionError();
        }

        @Override
        protected TaskLoader readTask(final UUID taskId) {
            return new TaskLoader(new DtxTaskAdm(taskId.toString(), "", "", null, DtxTaskStatus.COMMITTED), null);
        }
    }

    /** Create a {@link EventBus} for this {@link VersionedVolumeRepository}s */
    protected final EventBus eventBus;

    /** The owner of this repository. */
    private final UUID ownerId;
    /** The node on which this repository is running. */
    private final UUID nodeId;
    private final Uuid nodeIdTx;
    /** If not <code>null</code>, distributed task manager. */
    private final AtomicReference<DtxTaskApi> dtxTaskApiRef;
    /** If not <code>null</code>, destination of remote messages. */
    private final AtomicReference<MsgClientStartpoint> syncClientRef;
    /** Source of sent messages. The node UUID as a Uuid */
    private final Uuid msgSource;
    /** UUID of the VVR, proto VVR version */
    private final Uuid vvrUuid;

    /**
     * Reference to this repository's {@link MetaConfiguration}
     */
    @GuardedBy(value = "configuration")
    private volatile MetaConfiguration configuration;

    /**
     * Protected constructor to be called by subclasses.
     * 
     * @param builder
     *            a completely initialized {@link Builder}
     */
    protected AbstractRepositoryImpl(final Builder builder) {
        super(builder);

        if (builder.owner == null) {
            throw new NullPointerException("owner UUID must not be null");
        }
        if (builder.node == null) {
            throw new NullPointerException("node UUID must not be null");
        }
        this.ownerId = builder.owner;
        this.nodeId = builder.node;
        this.nodeIdTx = VvrRemoteUtils.newUuid(nodeId);
        this.syncClientRef = builder.syncClientRef;
        if (builder.dtxTaskApiRef == null) {
            // Create a default task manager
            this.dtxTaskApiRef = new AtomicReference<DtxTaskApi>(new DtxTaskApiImpl(this));
        }
        else {
            this.dtxTaskApiRef = builder.dtxTaskApiRef;
        }
        this.msgSource = VvrRemoteUtils.newUuid(nodeId);

        configuration = builder.configuration;

        if (configuration == null) {
            throw new IllegalStateException("Configuration is null");
        }

        final UUID uuid = getUuid();

        // Event bus
        eventBus = new EventBus("VVR-" + uuid);

        // Initialise
        vvrUuid = VvrRemoteUtils.newUuid(uuid);
    }

    @Override
    public final UUID getOwnerId() {
        return this.ownerId;
    }

    @Override
    public final UUID getNodeId() {
        return this.nodeId;
    }

    protected final MsgClientStartpoint getMsgClientStartpoint() {
        return syncClientRef.get();
    }

    protected final void enhanceMessage(final RemoteOperation.Builder opBuilder) {
        opBuilder.setVvr(vvrUuid);
        opBuilder.setSource(msgSource);
    }

    /**
     * @param opBuilder
     * @param type
     * @param opCode
     * @param async
     * @param peer
     * @return status of sync request or <code>null</code> for async messages or stand alone mode
     * @throws MsgServerTimeoutException
     * @throws InterruptedException
     * @throws ConnectException
     *             if peer is not <code>null</code>
     */
    protected Collection<MsgServerRemoteStatus> sendMessage(final RemoteOperation.Builder opBuilder, final Type type,
            final OpCode opCode, final boolean async, final UUID peer) throws MsgServerTimeoutException,
            InterruptedException, ConnectException {
        opBuilder.setVvr(vvrUuid);
        if (type == Type.VVR) {
            opBuilder.setUuid(vvrUuid);
        }
        return VvrRemoteUtils.sendMessage(opBuilder, syncClientRef.get(), msgSource, type, opCode, async, peer);
    }

    /**
     * Create the reply for a remote message.
     * 
     * @param opBuilder
     * @param type
     * @param opCode
     * @return the created message.
     */
    protected final MessageLite createMessageReply(final RemoteOperation.Builder opBuilder, final Type type,
            final OpCode opCode) {
        opBuilder.setVersion(ProtocolVersion.VERSION_1);
        opBuilder.setSource(msgSource);
        opBuilder.setType(type);
        opBuilder.setOp(opCode);
        return opBuilder.build();
    }

    /**
     * Submit a new distributed transaction.
     * 
     * @param opBuilder
     * @param type
     * @param opCode
     * @return the UUID of created task
     */
    protected UUID submitTransaction(final RemoteOperation.Builder opBuilder, final Type type, final OpCode opCode) {
        if (type == Type.VVR) {
            opBuilder.setUuid(vvrUuid);
            opBuilder.setVvr(vvrUuid);
        }
        else {
            opBuilder.setVvr(vvrUuid);
        }
        return VvrRemoteUtils.submitTransaction(opBuilder, dtxTaskApiRef.get(), getUuid(), nodeIdTx, type, opCode);
    }

    @Override
    protected final FutureVoid submitTransaction(final RemoteOperation.Builder opBuilder, final OpCode opCode) {

        return new FutureVoid(AbstractRepositoryImpl.this, submitTransaction(opBuilder, Type.VVR, opCode), getUuid());
    }

    protected final DtxTaskApi getDtxTaskApi() {
        return dtxTaskApiRef.get();
    }

    /**
     * Flag that the VVR must be deleted.
     */
    protected final void setDeleted() {
        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
        newKeyValueMap.put(DeletedConfigKey.getInstance(), Boolean.TRUE);
        updateConfiguration(newKeyValueMap);
    }

    @Override
    public final boolean isDeleted() {
        return DeletedConfigKey.getInstance().getTypedValue(configuration).booleanValue();
    }

    @Override
    public final void registerItemEvents(final Object subscriber) {
        eventBus.register(subscriber);
    }

    @Override
    public final void unregisterItemEvents(final Object subscriber) {
        eventBus.unregister(subscriber);
    }

    @Override
    public final MetaConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void updateConfiguration(final Map<AbstractConfigKey, Object> newKeyValueMap) {
        try {
            synchronized (configuration) {
                configuration = configuration.copyAndAlterConfiguration(newKeyValueMap);
            }
        }
        catch (final Exception e) {
            throw new IllegalArgumentException("Failed to update configuration", e);
        }
    }

    public static abstract class Builder extends AbstractUniqueVvrObject.Builder {
        /**
         * this repository's owner (mandatory).
         * <p>
         */
        private UUID owner;

        public final Builder ownerId(final UUID ownerId) {
            this.owner = ownerId;
            return this;
        }

        /**
         * Current node (mandatory).
         * <p>
         */
        private UUID node;

        public final Builder nodeId(final UUID nodeId) {
            this.node = nodeId;
            return this;
        }

        /**
         * Destination of remote messages (optional).
         * <p>
         */
        private AtomicReference<MsgClientStartpoint> syncClientRef = new AtomicReference<>();

        public final Builder syncClientRef(final AtomicReference<MsgClientStartpoint> syncClientRef) {
            this.syncClientRef = syncClientRef;
            return this;
        }

        /**
         * Destination of remote messages (optional).
         */
        private AtomicReference<DtxTaskApi> dtxTaskApiRef = null;

        public final Builder dtxTaskApiRef(final AtomicReference<DtxTaskApi> dtxTaskApiRef) {
            this.dtxTaskApiRef = dtxTaskApiRef;
            return this;
        }

        /**
         * The {@link MetaConfiguration} to build the {@link VersionedVolumeRepository} on.
         */
        private MetaConfiguration configuration;

        /**
         * Gets the current {@link MetaConfiguration} set by {@link #configuration(MetaConfiguration)}.
         * 
         * @return the immutable {@link MetaConfiguration} instance or <code>null</code> if none was set
         */
        protected MetaConfiguration getConfiguration() {
            return this.configuration;
        }

        public final Builder configuration(final MetaConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }
    }

}
