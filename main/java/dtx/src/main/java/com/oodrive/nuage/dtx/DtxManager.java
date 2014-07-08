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

import static com.oodrive.nuage.dtx.DtxNodeState.FAILED;
import static com.oodrive.nuage.dtx.DtxNodeState.INITIALIZED;
import static com.oodrive.nuage.dtx.DtxNodeState.NOT_INITIALIZED;
import static com.oodrive.nuage.dtx.DtxNodeState.STARTED;
import static com.oodrive.nuage.dtx.DtxUtils.dtxNodesToMembers;
import static com.oodrive.nuage.dtx.DtxUtils.dtxToMemberString;
import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.NORM_PRIORITY;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.Join;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.Slf4jFactory;
import com.hazelcast.util.ConcurrentHashSet;
import com.oodrive.nuage.dtx.events.DeadEventHandler;
import com.oodrive.nuage.dtx.events.DtxClusterEvent;
import com.oodrive.nuage.dtx.events.DtxClusterEvent.DtxClusterEventType;
import com.oodrive.nuage.dtx.events.DtxEvent;
import com.oodrive.nuage.dtx.events.DtxNodeEvent;
import com.oodrive.nuage.dtx.events.HazelcastToEvtBusLifecycleConverter;
import com.oodrive.nuage.dtx.proto.TxProtobufUtils;
import com.oodrive.nuage.proto.Common.Uuid;
import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxJournalEntry;

/**
 * The class encapsulating the DTX cluster's state and exporting its public API.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class DtxManager implements DtxTaskApi, DtxTaskInternal, DtxManagerMXBean {

    /**
     * The class used to describe the local cluster node.
     * 
     */
    public final class DtxLocalNode implements DtxLocalNodeMXBean {

        @Override
        public String getUuid() {
            return dtxConfig.getLocalPeer().getNodeId().toString();
        }

        @Override
        public String getIpAddress() {
            return dtxConfig.getLocalPeer().getAddress().getAddress().getHostAddress();
        }

        @Override
        public int getPort() {
            return dtxConfig.getLocalPeer().getAddress().getPort();
        }

        @Override
        public long getNextAtomicLong() {
            readLockIfOutsideTransaction();
            try {
                if (hazelcastInstance != null)
                    return hazelcastInstance.getAtomicNumber(TransactionInitiator.TX_ID_GENERATOR_NAME).get();
                else
                    return DtxConstants.DEFAULT_LAST_TX_VALUE;
            }
            finally {
                readUnlockIfOutsideTransaction();
            }
        }

        @Override
        public long getCurrentAtomicLong() {
            readLockIfOutsideTransaction();
            try {
                if (hazelcastInstance != null)
                    return hazelcastInstance.getAtomicNumber(TransactionInitiator.TX_CURRENT_ID).get();
                else
                    return DtxConstants.DEFAULT_LAST_TX_VALUE;
            }
            finally {
                readUnlockIfOutsideTransaction();
            }
        }

        @Override
        public DtxNodeState getStatus() {
            readLockIfOutsideTransaction();
            try {
                return status;
            }
            finally {
                readUnlockIfOutsideTransaction();
            }
        }

        @Override
        public DtxPeerAdm[] getPeers() {

            readLockIfOutsideTransaction();
            try {
                DtxPeerAdm[] result;
                if (registeredPeers == null) {
                    result = new DtxPeerAdm[0];
                }
                else {
                    final Set<DtxNode> peers = Collections.unmodifiableSet(registeredPeers);
                    if (peers.size() > 1) {
                        // remove local node
                        result = new DtxPeerAdm[peers.size() - 1];
                        int i = 0;
                        for (final DtxNode peer : peers) {
                            if (!peer.equals(dtxConfig.getLocalPeer())) {
                                result[i++] = new DtxPeerAdm(peer.getNodeId(), peer.getAddress(),
                                        onlinePeers.contains(peer));
                            }
                        }
                    }
                    else {
                        result = new DtxPeerAdm[0];
                    }
                }
                return result;
            }
            finally {
                readUnlockIfOutsideTransaction();
            }

        }
    }

    static {
        /*
         * explicitly set the logging type system property for hazelcast classes still getting their loggers from
         * com.hazelcast.logging.Logger.getLogger(String)
         */
        // TODO: remove as soon as all hazelcast classes use com.hazelcast.logging.LoggingService
        System.setProperty("hazelcast.logging.type", "slf4j");

        /*
         * explicitly set the logging class system property to create an explicit reference to the class, this time to
         * work around a bug in maven-shade-plugin (see pom.xml in vold) that will remove all non-references classes but
         * won't behave as documented when adding explicit inclusion filters!
         */
        // TODO: replace by an appropriate inclusion filter for maven-shade-plugin once it's fixed
        System.setProperty("hazelcast.logging.class", Slf4jFactory.class.getCanonicalName());
    }

    /**
     * A wrapping {@link ManagedContext} implementation to allow injection of custom objects into the context passed to
     * Callables that implement HazelcastInstanceAware.
     * 
     * 
     */
    final class ManagedDtxContext implements ManagedContext {

        private final ManagedContext wrappedContext;

        /**
         * Private constructor to create the instance on {@link Config} creation.
         * 
         * @param wrappedContext
         *            the existing {@link ManagedContext} contained in the {@link Config}
         */
        private ManagedDtxContext(final ManagedContext wrappedContext) {
            this.wrappedContext = wrappedContext;
        }

        @Override
        public final Object initialize(final Object obj) {
            if (wrappedContext == null) {
                return obj;
            }
            return wrappedContext.initialize(obj);
        }

        /**
         * Gets the {@link TransactionManager} associated with this {@link ManagedContext}.
         * 
         * @return a functional {@link TransactionManager} instance
         */
        final TransactionManager getTransactionManager() {
            return getTxManager();
        }

        /**
         * Gets the unique node ID provided with the initial {@link DtxManagerConfig}.
         * 
         * @return the non-<code>null</code> unique node ID
         */
        final UUID getNodeId() {
            readLockIfOutsideTransaction();
            try {
                return dtxConfig.getLocalPeer().getNodeId();
            }
            finally {
                readUnlockIfOutsideTransaction();
            }
        }
    }

    /**
     * Link between lifecycle events from Hazelcast and the internal EventBus.
     */
    private HazelcastToEvtBusLifecycleConverter hzEventConverter;

    /**
     * Implementation of the Dtx task API, except the submission and the cancellation of tasks.
     * 
     * 
     */
    private static final class DtxTaskApiImpl extends DtxTaskApiAbstract {
        private final DtxManager dtxManager;

        DtxTaskApiImpl(final DtxManager dtxManager, final TaskKeeperParameters parameters) {
            super(parameters);
            this.dtxManager = dtxManager;
        }

        @Override
        public final UUID submit(final UUID resourceId, final byte[] payload) throws IllegalStateException {
            // Should not get here
            throw new AssertionError();
        }

        @Override
        public final boolean cancel(final UUID taskId) throws IllegalStateException {
            // Should not get here
            throw new AssertionError();
        }

        @Override
        protected TaskLoader readTask(final UUID taskId) {
            return dtxManager.transactionMgr.readTask(taskId);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DtxManager.class);

    private static final int DISCOVER_TIMEOUT = 5;
    private static final int DISCOVER_RETRIES = 3;
    private static final int DISCOVER_NODE_ID_RETRY_DELAY_MS = 10;

    private static final int DEFAULT_NB_EVENT_THREADS = 3;
    private static final int EVENT_SHUTDOWN_TIMEOUT_S = 5;

    private static final int SYNCHRONIZE_TIMEOUT_S = 10;

    @GuardedBy("statusLock")
    private HazelcastInstance hazelcastInstance;

    @GuardedBy("statusLock")
    private TransactionManager transactionMgr;

    @GuardedBy("statusLock")
    private TransactionInitiator txInit;

    @GuardedBy("statusLock")
    private Set<DtxNode> registeredPeers;

    @GuardedBy("statusLock")
    private BlockingQueue<Request> requestQueue;

    @GuardedBy("statusLock")
    private DtxNodeState status = NOT_INITIALIZED;

    private final Set<DtxNode> onlinePeers = new ConcurrentHashSet<>();

    /**
     * Read/write lock for access to all fields related to the runtime status.
     */
    private final ReentrantReadWriteLock statusLock = new ReentrantReadWriteLock();

    /**
     * Configuration instance used to (repeatedly) {@link #init() initialize} this instance.
     */
    private final DtxManagerConfig dtxConfig;

    /**
     * Task management.
     */
    private final DtxTaskApi dtxTaskApi;
    private final DtxTaskInternal dtxTaskInternal;

    /**
     * Hazelcast {@link Config} instance (re-)created for each {@link #init() initialization}.
     */
    @GuardedBy("statusLock")
    private Config hazelCastConfig;

    /**
     * Local Hazelcast {@link Member}, set at each {@link #start()} and cleared at {@link #stop()}.
     */
    @GuardedBy("statusLock")
    private Member localMember;

    @GuardedBy("statusLock")
    private EventBus internalEventBus;

    @GuardedBy("statusLock")
    private EventBus externalEventBus;

    @GuardedBy("statusLock")
    private ExecutorService dtxExternalEventExecutor;

    @GuardedBy("statusLock")
    private PostSyncProcessor postSyncProcessor;

    private final ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock();
    @GuardedBy("mapLock")
    private final HashBasedTable<UUID, DtxNode, Long> clusterUpdateMap = HashBasedTable.create();

    /**
     * Constructs a {@link DtxManager} using a valid {@link DtxManagerConfig} instance.
     * 
     * Incomplete or invalid configurations will throw exceptions when calling {@link #init()} and/or {@link #start()}.
     * 
     * @param dtxConfig
     *            a complete {@link DtxManagerConfig} instance
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public DtxManager(@Nonnull final DtxManagerConfig dtxConfig) throws NullPointerException {
        super();
        this.dtxConfig = Objects.requireNonNull(dtxConfig, "Configuration must not be null");
        final DtxTaskApiImpl taskApiImpl = new DtxTaskApiImpl(this, dtxConfig.getParameters());

        this.dtxTaskInternal = taskApiImpl.getDtxTaskInternal();
        this.dtxTaskApi = taskApiImpl;
    }

    /**
     * Gets the current status of the DTX cluster node represented by this instance.
     * 
     * @return a valid {@link DtxNodeState}
     */
    public final DtxNodeState getStatus() {
        return status;
    }

    /**
     * Initializes the {@link DtxManager}.
     * 
     * This validates most parts of the provided configuration and transitions to a {@link DtxNodeState#INITIALIZED}
     * state. Calling in any state but {@link DtxNodeState#NOT_INITIALIZED} will have no effect.
     */
    public final void init() {
        statusLock.writeLock().lock();
        try {
            if (status != NOT_INITIALIZED) {
                return;
            }

            final File journalDir = dtxConfig.getJournalDirectory().toFile();
            if (!journalDir.exists() && !journalDir.mkdirs()) {
                throw new IllegalStateException("Could not create journal directory");
            }
            if (!journalDir.canWrite()) {
                throw new IllegalStateException("Journal directory is not writable");
            }

            // TODO: limit the capacity of the queue
            requestQueue = new LinkedBlockingQueue<Request>();

            registeredPeers = new ConcurrentHashSet<DtxNode>();
            // adds all configured peers to the list of registered ones
            registeredPeers.add(dtxConfig.getLocalPeer());
            registeredPeers.addAll(dtxConfig.getPeers());

            // Initializes the event bus
            dtxExternalEventExecutor = Executors.newFixedThreadPool(DEFAULT_NB_EVENT_THREADS, new ThreadFactory() {

                private int serial = 0;

                /**
                 * Give event threads a lower priority.
                 */
                private final int priority = NORM_PRIORITY + ((MIN_PRIORITY - NORM_PRIORITY) / 2);

                @Override
                public final Thread newThread(final Runnable r) {
                    final Thread result = new Thread(r, "DtxEventWorker_" + getNodeId() + "_" + (++serial));
                    result.setDaemon(true);
                    result.setPriority(priority);
                    return result;
                }
            });

            internalEventBus = new AsyncEventBus("dtxEventBus", dtxExternalEventExecutor);
            // add dead event handler
            internalEventBus.register(new DeadEventHandler(LOGGER));

            internalEventBus.register(new DiscoveryEventHandler());

            internalEventBus.register(new SyncEventHandler());

            this.postSyncProcessor = new PostSyncProcessor();
            internalEventBus.register(postSyncProcessor);

            externalEventBus = new EventBus("dtxEvents");

            // constructs a new transaction manager instance
            transactionMgr = new TransactionManager(this.dtxConfig, this);

            this.hazelCastConfig = new Config();

            // redirect Hazelcast logging to SLF4J/logback
            hazelCastConfig.setProperty("hazelcast.logging.type", "slf4j");

            // disable REST interface by default
            hazelCastConfig.setProperty("hazelcast.rest.enabled", "false");

            // avoid binding to any local address
            hazelCastConfig.setProperty("hazelcast.socket.bind.any", "false");

            this.hzEventConverter = new HazelcastToEvtBusLifecycleConverter(getNodeId(), internalEventBus,
                    externalEventBus);
            hazelCastConfig.addListenerConfig(new ListenerConfig(hzEventConverter));

            final ManagedContext mgdCtx = new ManagedDtxContext(hazelCastConfig.getManagedContext());
            this.hazelCastConfig.setManagedContext(mgdCtx);

            // Sets cluster name and access password
            final GroupConfig hzGroupConfig = hazelCastConfig.getGroupConfig();
            hzGroupConfig.setName(dtxConfig.getClusterName());
            hzGroupConfig.setPassword(dtxConfig.getClusterPassword());

            // Sets the instance name
            hazelCastConfig.setInstanceName(getNodeId().toString());

            // Sets the local peer
            final NetworkConfig hzNetworkConfig = hazelCastConfig.getNetworkConfig();
            hzNetworkConfig.getInterfaces().setEnabled(true);
            final InetSocketAddress localAddr = dtxConfig.getLocalPeer().getAddress();
            hzNetworkConfig.getInterfaces().addInterface(localAddr.getAddress().getHostAddress());
            hzNetworkConfig.setPort(localAddr.getPort());
            hzNetworkConfig.setPortAutoIncrement(false);

            final Join networkJoin = hzNetworkConfig.getJoin();

            // Sets the list of peers
            final TcpIpConfig tcpIpConfig = new TcpIpConfig();
            tcpIpConfig.setEnabled(true);
            tcpIpConfig.setMembers(dtxNodesToMembers(dtxConfig.getPeers()));
            networkJoin.setTcpIpConfig(tcpIpConfig);

            // Explicitly disable multicast
            final MulticastConfig multicastConfig = new MulticastConfig();
            multicastConfig.setEnabled(false);
            networkJoin.setMulticastConfig(multicastConfig);

            final DtxNodeState oldStatus = status;
            status = INITIALIZED;

            // posts the matching event
            postEvent(new DtxNodeEvent(this, oldStatus, status), true);

        }
        finally {
            statusLock.writeLock().unlock();
        }

    }

    /**
     * Shuts down the {@link DtxManager}.
     * 
     * After successful completion, this instance is set to the {@link DtxNodeState#NOT_INITIALIZED} state. This method
     * has no effect on instances already in this state.
     */
    public final void fini() {
        statusLock.writeLock().lock();
        try {
            if (status == NOT_INITIALIZED) {
                return;
            }

            if (status == STARTED) {
                stop();
            }

            registeredPeers = null;

            assert (requestQueue.isEmpty());
            requestQueue = null;

            transactionMgr = null;

            hazelCastConfig = null;

            final DtxNodeState oldStatus = status;
            status = NOT_INITIALIZED;

            hzEventConverter = null;

            // posts the matching event
            postEvent(new DtxNodeEvent(this, oldStatus, status), true);

            // shutdown the event bus
            dtxExternalEventExecutor.shutdown();
            try {
                if (!dtxExternalEventExecutor.awaitTermination(EVENT_SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS)) {
                    dtxExternalEventExecutor.shutdownNow();
                }
            }
            catch (final InterruptedException e) {
                dtxExternalEventExecutor.shutdownNow();
            }

            postSyncProcessor = null;

            internalEventBus = null;

            externalEventBus = null;
        }
        finally {
            status = NOT_INITIALIZED;
            statusLock.writeLock().unlock();
        }

    }

    /**
     * Starts the {@link DtxManager} instance to obtain an operational access to the DTX cluster.
     * 
     * @throws IllegalStateException
     *             if starting fails due to configuration or runtime errors
     */
    public final void start() throws IllegalStateException {
        statusLock.writeLock().lock();
        try {
            if (status != INITIALIZED) {
                return;
            }

            try {
                hazelcastInstance = Hazelcast.newHazelcastInstance(hazelCastConfig);
                refreshOnlinePeers();

                // registers the local member
                final Cluster hzCluster = hazelcastInstance.getCluster();
                this.localMember = hzCluster.getLocalMember();

                // adds a membership listener that dynamically adds/removes online members
                hzCluster.addMembershipListener(new MembershipListener() {

                    @Override
                    public final void memberRemoved(final MembershipEvent membershipEvent) {
                        statusLock.readLock().lock();
                        try {
                            removeOnlinePeer(membershipEvent.getMember());
                        }
                        finally {
                            statusLock.readLock().unlock();
                        }
                    }

                    @Override
                    public final void memberAdded(final MembershipEvent membershipEvent) {
                        statusLock.readLock().lock();
                        try {
                            addOnlinePeer(membershipEvent.getMember());
                        }
                        finally {
                            statusLock.readLock().unlock();
                        }
                    }
                });
            }
            catch (final HazelcastException he) {
                final DtxNodeState oldStatus = status;
                status = FAILED;
                postEvent(new DtxNodeEvent(this, oldStatus, status), true);
                throw new IllegalStateException(he);
            }

            txInit = new TransactionInitiator(getNodeId(), hazelcastInstance, this, requestQueue,
                    dtxConfig.getTransactionTimeout());
            txInit.start();

            transactionMgr.startUp(txInit);

            final DtxNodeState oldStatus = status;
            status = STARTED;

            // posts the matching event
            postEvent(new DtxNodeEvent(this, oldStatus, status), true);
        }
        finally {
            statusLock.writeLock().unlock();
        }
    }

    /**
     * Stops the {@link DtxManager}.
     * 
     * Successful completion from a {@link DtxNodeState#STARTED} state will revert the instance to the
     * {@link DtxNodeState#INITIALIZED} state. Calling this methods on instances that are not in a
     * {@link DtxNodeState#STARTED} state has no effect.
     * 
     * Note: Pending requests are cleared by this operation.
     * 
     * @throws IllegalThreadStateException
     *             if this is called from within a transaction
     */
    public final void stop() throws IllegalThreadStateException {
        statusLock.writeLock().lock();
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Stopping DTX manager; id=" + this.getNodeId());
            }
            if (status != STARTED) {
                LOGGER.debug("not stopping, state is " + status);
                return;
            }

            if (transactionMgr.holdsTransactionLock()) {
                throw new IllegalThreadStateException("Stop attempt from within a transaction");
            }

            try {

                // TODO: properly shut down the initiator before altering the Hazelcast instance
                txInit.stop();
                txInit = null;

                final LifecycleService hzLifeCycle = hazelcastInstance.getLifecycleService();
                hzLifeCycle.shutdown();

                if (hzLifeCycle.isRunning()) {
                    hzLifeCycle.kill();
                }
                assert !hzLifeCycle.isRunning();

                // clears hazelcast runtime state
                localMember = null;
                onlinePeers.clear();

                // remove the life cycle listener
                hzLifeCycle.removeLifecycleListener(hzEventConverter);

                final Set<HazelcastInstance> hzInstances = Hazelcast.getAllHazelcastInstances();
                hzInstances.remove(hazelcastInstance);
                assert !hzInstances.contains(hazelcastInstance);
                hazelcastInstance = null;

                transactionMgr.shutdown();

                // clear the request queue
                requestQueue.clear();

                // clear the update map
                clusterUpdateMap.clear();

                final DtxNodeState oldStatus = status;

                // posts the matching event
                postEvent(new DtxNodeEvent(this, oldStatus, INITIALIZED), true);
            }
            finally {
                status = INITIALIZED;
            }
        }
        finally {
            statusLock.writeLock().unlock();
        }

        // clears the cluster update map under lock
        mapLock.writeLock().lock();
        try {
            clusterUpdateMap.clear();
        }
        finally {
            mapLock.writeLock().unlock();
        }

    }

    /**
     * Registers a {@link DtxResourceManager}.
     * 
     * @param resourceManager
     *            the {@link DtxResourceManager} to include in distributed transactions
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been successfully {@link #init() initialized}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public final void registerResourceManager(@Nonnull final DtxResourceManager resourceManager)
            throws IllegalStateException, NullPointerException {

        readLockIfOutsideTransaction();
        try {
            if (this.transactionMgr == null) {
                throw new IllegalStateException("Not initialized");
            }

            this.transactionMgr.registerResourceManager(resourceManager, txInit);

        }
        finally {
            readUnlockIfOutsideTransaction();
        }

    }

    /**
     * Unregisters a {@link DtxResourceManager}.
     * 
     * Note: Unregistering does not prevent any currently running transaction phases involving the resource manager from
     * proceeding. The only guarantee is that after completion any newly started transaction phase targeting this
     * resource manager will fail.
     * 
     * @param resourceManagerId
     *            the {@link UUID} of the {@link DtxResourceManager} to exclude from distributed transactions
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been initialized properly
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public final void unregisterResourceManager(@Nonnull final UUID resourceManagerId) throws IllegalStateException,
            NullPointerException {

        Objects.requireNonNull(resourceManagerId);

        readLockIfOutsideTransaction();
        try {
            if (this.transactionMgr == null) {
                throw new IllegalStateException("Not initialized");
            }
            transactionMgr.unregisterResourceManager(resourceManagerId);
            removeClusterMapInfo(resourceManagerId);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Registers a peer with the cluster.
     * 
     * Note: Upon {@link #init()}, all configured peers are automatically registered.
     * 
     * @param peer
     *            the peer added to the cluster
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been successfully {@link #init() initialized}
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public final void registerPeer(final DtxNode peer) throws IllegalStateException, NullPointerException {
        statusLock.writeLock().lock();
        try {
            if (this.registeredPeers == null) {
                throw new IllegalStateException("Not initialized");
            }

            if (registeredPeers.contains(peer)) {
                return;
            }
            this.registeredPeers.add(Objects.requireNonNull(peer));

            if (STARTED.equals(status)) {
                final Cluster cluster = hazelcastInstance.getCluster();
                final Member newMember = peer.asHazelcastMember(cluster.getLocalMember());
                if (cluster.getMembers().contains(newMember)) {
                    addOnlinePeer(newMember);
                }
            }

            final TcpIpConfig hzTcpIpConfig = this.hazelCastConfig.getNetworkConfig().getJoin().getTcpIpConfig();

            hzTcpIpConfig.addMember(dtxToMemberString(peer));

            if (hazelcastInstance != null) {
                hazelcastInstance.getLifecycleService().restart();
            }
        }
        finally {
            statusLock.writeLock().unlock();
        }
    }

    /**
     * Gets the set of registered peers.
     * 
     * @return an unmodifiable {@link Set} of {@link DtxNode}s with the currently registered peers
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been successfully {@link #init() initialized}
     */
    public final Set<DtxNode> getRegisteredPeers() throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (this.registeredPeers == null) {
                throw new IllegalStateException("Not initialized");
            }
            return Collections.unmodifiableSet(this.registeredPeers);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Unregisters a peer.
     * 
     * @param peer
     *            a {@link DtxNode} describing the peer to remove
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been initialized properly
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public final void unregisterPeer(@Nonnull final DtxNode peer) throws IllegalStateException, NullPointerException {
        statusLock.writeLock().lock();
        try {
            if (this.registeredPeers == null) {
                throw new IllegalStateException("Not initialized");
            }

            Objects.requireNonNull(peer);

            final TcpIpConfig hzTcpIpConfig = this.hazelCastConfig.getNetworkConfig().getJoin().getTcpIpConfig();

            final List<String> newMembers = new ArrayList<String>(hzTcpIpConfig.getMembers());
            final String peerMember = dtxToMemberString(peer);
            newMembers.remove(peerMember);
            hzTcpIpConfig.setMembers(newMembers);

            // TODO: modify the hazelcast instance state to include the updated configuration (restart?)
            if (hazelcastInstance != null) {
                hazelcastInstance.getLifecycleService().restart();
            }
            this.registeredPeers.remove(peer);
        }
        finally {
            statusLock.writeLock().unlock();
        }
    }

    @Override
    public final UUID submit(final UUID resourceId, final byte[] payload) throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (STARTED != this.status) {
                throw new IllegalStateException("Not started");
            }

            final UUID taskId = UUID.randomUUID();

            final Semaphore submitSemaphore = this.txInit.getSubmitSemaphore();
            try {
                submitSemaphore.acquire();
            }
            catch (final InterruptedException e1) {
                throw new IllegalStateException("Interrupted on submit");
            }
            try {

                if (!isQuorumOnline()) {
                    throw new IllegalStateException("Quorum is not online, not accepting submissions");
                }

                try {
                    this.requestQueue.put(new Request(resourceId, taskId, payload));

                    // create a new task with pending status
                    final DtxResourceManager resource = transactionMgr.getRegisteredResourceManager(resourceId);
                    // check if dtx task info need to be created
                    if (resource != null) {
                        final DtxTaskInfo info = resource.createTaskInfo(payload);
                        setTask(taskId, DtxConstants.DEFAULT_LAST_TX_VALUE, resourceId, DtxTaskStatus.PENDING, info);
                    }
                    else {
                        setTask(taskId, DtxConstants.DEFAULT_LAST_TX_VALUE, resourceId, DtxTaskStatus.PENDING, null);
                    }
                }
                catch (final InterruptedException e) {
                    LOGGER.error("Request submission interrupted", e);
                    throw new IllegalStateException(e);
                }
            }
            finally {
                submitSemaphore.release();
            }

            return taskId;
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets the currently active {@link PostSyncProcessor}.
     * 
     * @return the {@link PostSyncProcessor} currently in charge of executing post-sync, <code>null</code> if not
     *         initialized
     */
    final PostSyncProcessor getPostSyncProcessor() {
        return postSyncProcessor;
    }

    @Override
    public final void startPurgeTaskKeeper() {
        dtxTaskInternal.startPurgeTaskKeeper();
    }

    @Override
    public final void stopPurgeTaskKeeper() {
        dtxTaskInternal.stopPurgeTaskKeeper();
    }

    @Override
    public final void setTask(final UUID taskId, final long txId, final UUID resourceId, final DtxTaskStatus status,
            final DtxTaskInfo info) {
        dtxTaskApi.setTask(taskId, txId, resourceId, status, info);
    }

    @Override
    public final void loadTask(final UUID taskId, final long txId, final UUID resourceId, final DtxTaskStatus status,
            final DtxTaskInfo info, final long timestamp) {
        dtxTaskInternal.loadTask(taskId, txId, resourceId, status, info, timestamp);
    }

    @Override
    public final void setTaskReadableId(final UUID taskId, final String name, final String description) {
        dtxTaskInternal.setTaskReadableId(taskId, name, description);
    }

    @Override
    public final void setTaskStatus(final UUID taskId, final DtxTaskStatus status) {
        dtxTaskInternal.setTaskStatus(taskId, status);
    }

    @Override
    public final void setTaskStatus(final long transactionId, final DtxTaskStatus status) {
        dtxTaskInternal.setTaskStatus(transactionId, status);
    }

    @Override
    public final void setTaskTransactionId(final UUID taskId, final long txId) {
        dtxTaskInternal.setTaskTransactionId(taskId, txId);
    }

    @Override
    public final void setDtxTaskInfo(final UUID taskId, final DtxTaskInfo taskInfo) {
        dtxTaskInternal.setDtxTaskInfo(taskId, taskInfo);
    }

    @Override
    public final boolean isDtxTaskInfoSet(final UUID taskId) {
        return dtxTaskInternal.isDtxTaskInfoSet(taskId);
    }

    @Override
    public DtxTaskInfo getDtxTaskInfo(final UUID taskId) {
        return dtxTaskApi.getDtxTaskInfo(taskId);
    }

    @Override
    public long getTaskTimestamp(final UUID taskId) {
        return dtxTaskInternal.getTaskTimestamp(taskId);
    }

    @Override
    public final DtxTaskAdm[] getTasks() {
        return dtxTaskApi.getTasks();
    }

    @Override
    public final DtxResourceManagerAdm[] getResourceManagers() {
        readLockIfOutsideTransaction();
        try {
            final Collection<DtxResourceManager> resMgrs = transactionMgr.getRegisteredResourceManagers();
            int i = 0;
            final DtxResourceManagerAdm[] result = new DtxResourceManagerAdm[resMgrs.size()];
            for (final DtxResourceManager resMgr : resMgrs) {
                final UUID resUuid = resMgr.getId();
                result[i++] = new DtxResourceManagerAdm(resUuid, getResourceManagerState(resUuid),
                        getLastCompleteTxIdForResMgr(resUuid), getJournalPathForResMgr(resUuid),
                        getJournalStatusForResMgr(resUuid));
            }
            return result;
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    @Override
    public final DtxRequestQueueAdm getRequestQueue() {
        return new DtxRequestQueueAdm(getNbOfPendingRequests(), getNextPendingRequest());
    }

    @Override
    public final DtxTaskAdm[] getResourceManagerTasks(final UUID resourceId) {
        return dtxTaskApi.getResourceManagerTasks(resourceId);
    }

    @Override
    public final DtxTaskAdm getTask(final UUID taskId) {
        return dtxTaskApi.getTask(taskId);
    }

    @Override
    public final boolean cancel(final UUID taskId) throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (STARTED != this.status) {
                throw new IllegalStateException("Not started");
            }
            Objects.requireNonNull(taskId);

            // checks if the request is still in the queue
            Request foundRequest = null;
            for (final Request currRequest : this.requestQueue) {
                if (currRequest.getTaskId().equals(taskId)) {
                    foundRequest = currRequest;
                    break;
                }
            }

            // if the request was found, try to remove it and return if successful
            // TODO: add checks and proper locking of the queue!
            if ((foundRequest != null) && (requestQueue.remove(foundRequest))) {
                return true;
            }

            // if not in the queue, request cancel with the initiator
            return this.txInit.requestCancel(taskId);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    @Override
    public DtxTaskAdm[] getResourceManagerTasks(final String resourceId) {
        return getResourceManagerTasks(UUID.fromString(resourceId));
    }

    @Override
    public final DtxTaskAdm getTask(final String taskId) {
        return getTask(UUID.fromString(taskId));
    }

    @Override
    public final boolean cancelTask(final String taskId) {
        return cancel(UUID.fromString(taskId));
    }

    @Override
    public void restart() {
        stop();
        start();
    }

    /**
     * Gets the ID of the last completed transaction for this instance.
     * 
     * @return a non-negative transaction ID or {@value TransactionManager#DEFAULT_LAST_TX_VALUE} if non was executed
     *         yet
     * @throws IllegalStateException
     *             if this instance is not {@link #startUp() started}
     */
    public final long getLastCompleteTxId() throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (transactionMgr == null) {
                throw new IllegalStateException("Not initialized");
            }
            return this.transactionMgr.getLastCompleteTxId();
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets the state of a registered {@link DtxResourceManager}.
     * 
     * @param resUuid
     *            the {@link UUID} of the requested {@link DtxResourceManager}
     * @return a
     * @throws IllegalStateException
     *             if this instance is not properly {@link #init() initialized}
     */
    public final DtxResourceManagerState getResourceManagerState(final UUID resUuid) throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (transactionMgr == null) {
                throw new IllegalStateException("Not initialized");
            }
            return this.transactionMgr.getResourceManagerState(resUuid);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Registers a listener for events triggered by state changes in the cluster membership.
     * 
     * @param listener
     *            the listener object taking a single ClusterNodeEvent as argument in its Subscribe method
     * @throws NullPointerException
     *             in case the argument is <code>null</code>
     * @throws IllegalStateException
     *             if this instance is not {@link #init() initialized}
     */
    public final void registerDtxEventListener(@Nonnull final Object listener) throws NullPointerException,
            IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (externalEventBus == null) {
                throw new IllegalStateException("Not initialized");
            }
            externalEventBus.register(listener);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Unregisters a listener for events triggered by state changes in the cluster membership.
     * 
     * @param listener
     *            the listener object taking a single ClusterNodeEvent as argument in its Subscribe method
     * @throws NullPointerException
     *             in case the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if the listener was not previously registered
     * @throws IllegalStateException
     *             if this instance is not {@link #init() initialized}
     */
    public final void unregisterDtxEventListener(@Nonnull final Object listener) throws NullPointerException,
            IllegalArgumentException, IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (externalEventBus == null) {
                throw new IllegalStateException("Not initialized");
            }
            externalEventBus.unregister(listener);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    @Override
    public final String toString() {
        return com.google.common.base.Objects.toStringHelper(this).add("nodeId", this.getNodeId())
                .add("status", status).toString();
    }

    /**
     * Gets the ID of the local cluster node.
     * 
     * @return the {@link UUID} of the local cluster node
     */
    final UUID getNodeId() {
        return dtxConfig.getLocalPeer().getNodeId();
    }

    /**
     * Gets the started property.
     * 
     * @return <code>true</code> if this instance is successfully {@link #start() started}, <code>false</code> otherwise
     */
    final boolean isStarted() {
        return STARTED == status;
    }

    /**
     * Gets the transaction manager of this instance.
     * 
     * @return the current {@link TransactionManager}, <code>null</code> if this instance was not initialized
     */
    final TransactionManager getTxManager() {
        statusLock.readLock().lock();
        try {
            return transactionMgr;
        }
        finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Gets a reference to the (shared) read lock linked to this instance's {@link #getStatus() status}.
     * 
     * @return the {@link ReadLock} preventing changes in status while held
     */
    final ReadLock getStatusReadLock() {
        return statusLock.readLock();
    }

    /**
     * Gets the number of request currently waiting to be executed.
     * 
     * @return the number of requests {@link #submit(UUID, byte[])}ed, but not yet started as transactions or 0 if
     *         either none is pending or this instance is not {@link #init() initialized}
     */
    final int getNbOfPendingRequests() {
        readLockIfOutsideTransaction();
        try {
            if (requestQueue == null) {
                return 0;
            }
            return requestQueue.size();
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets the next request currently waiting to be executed.
     * 
     * @return the next requests {@link #submit(UUID, byte[])}ed, but not yet started as transactions or null if either
     *         none is pending or this instance is not {@link #init() initialized}
     */
    final Request getNextPendingRequest() {
        readLockIfOutsideTransaction();
        try {
            if (requestQueue == null) {
                return null;
            }
            return requestQueue.peek();
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets the last complete transaction for a given {@link #registerResourceManager(DtxResourceManager) registered}
     * resource manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return a positive transaction ID or {@value #DEFAULT_LAST_TX_VALUE} if no transaction has been completed yet or
     *         the resource manager is not registered with this instance
     * @throws IllegalStateException
     *             if the resource manager cannot return a valid value
     */
    final long getLastCompleteTxIdForResMgr(final UUID resId) throws IllegalStateException {
        if (transactionMgr == null) {
            throw new IllegalStateException("Not initialized");
        }
        return this.transactionMgr.getLastCompleteTxIdForResMgr(resId);
    }

    /**
     * Gets the journal path for a given {@link #registerResourceManager(DtxResourceManager) registered} resource
     * manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return a String containing the journal path
     * 
     * @throws IllegalStateException
     *             if the resource manager cannot return a valid value
     */
    final String getJournalPathForResMgr(final UUID resId) throws IllegalStateException {
        if (transactionMgr == null) {
            throw new IllegalStateException("Not initialized");
        }
        return this.transactionMgr.getJournalPathForResMgr(resId);
    }

    /**
     * Gets the journal status for a given {@link #registerResourceManager(DtxResourceManager) registered} resource
     * manager.
     * 
     * @param resId
     *            the {@link UUID} of the {@link DtxResourceManager} to search for
     * @return true if the journal is started, else false.
     * 
     * @throws IllegalStateException
     *             if the resource manager cannot return a valid value
     */
    final boolean getJournalStatusForResMgr(final UUID resId) throws IllegalStateException {
        if (transactionMgr == null) {
            throw new IllegalStateException("Not initialized");
        }
        return this.transactionMgr.getJournalStatusForResMgr(resId);
    }

    /**
     * Internal getter for the node's {@link TransactionManager}s to retrieve a {@link DtxResourceManager} instance.
     * 
     * @param resMgrId
     *            the {@link UUID} of the requested {@link DtxResourceManager}
     * @return the registered instance or <code>null</code> if none was found
     * @throws IllegalStateException
     *             if the {@link DtxManager} has not been initialized properly
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    final DtxResourceManager getRegisteredResourceManager(@Nonnull final UUID resMgrId) {
        readLockIfOutsideTransaction();
        try {
            if (this.transactionMgr == null) {
                throw new IllegalStateException("Not initialized");
            }
            return this.transactionMgr.getRegisteredResourceManager(resMgrId);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets all peers that are currently {@link #registerPeer(DtxNode) registered} and considered to be online.
     * 
     * @return a possibly empty list of {@link DtxNode}s
     */
    final Set<DtxNode> getOnlinePeers() {
        return onlinePeers;
    }

    /**
     * Searches for a given task in the {@link TransactionManager}s journal archives.
     * 
     * @param taskId
     *            the {@link UUID} of the requested task
     * @return a valid {@link DtxTaskStatus}, {@link DtxTaskStatus#UNKNOWN} if it was not found
     */
    final DtxTaskStatus searchTaskStatus(final UUID taskId) {
        // TODO: add locking
        return this.transactionMgr.searchTaskStatus(taskId);
    }

    /**
     * Discovers a given resource managers synchronization status on the designated cluster nodes.
     * 
     * This needs external locking.
     * 
     * @param discoveryMap
     *            a map of resource manager ID mapped to last transaction IDs
     * @param nodes
     *            a set of target nodes, all online nodes will be solicited if left out
     */
    final void discoverResMgrStatus(final Map<UUID, Long> discoveryMap, final DtxNode... nodes) {

        if (discoveryMap.isEmpty()) {
            return;
        }

        final List<DtxNode> targetNodes = (nodes.length == 0 ? new ArrayList<DtxNode>(onlinePeers)
                : new ArrayList<DtxNode>(Arrays.asList(nodes)));

        // cuts the target node list to size
        targetNodes.retainAll(registeredPeers);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Discovering synchronization status; node=" + getNodeId() + ", resource managers="
                    + discoveryMap + ", target nodes=" + targetNodes);
        }

        final ConcurrentHashMap<DtxNode, Future<Map<UUID, Long>>> futureMap = new ConcurrentHashMap<>(
                targetNodes.size());
        for (final DtxNode targetNode : targetNodes) {

            final DistributedTask<Map<UUID, Long>> discoverTask = new DistributedTask<Map<UUID, Long>>(
                    new NodeDiscoveryHandler(discoveryMap, targetNode), targetNode.asHazelcastMember(localMember));

            // TODO: find a way to invoke this without having to cast the result
            @SuppressWarnings("unchecked")
            final Future<Map<UUID, Long>> discoFuture = (Future<Map<UUID, Long>>) hazelcastInstance
                    .getExecutorService().submit(discoverTask);
            futureMap.put(targetNode, discoFuture);
        }

        final ConcurrentHashMap<UUID, Long> resultMap = new ConcurrentHashMap<UUID, Long>(discoveryMap.size());
        int responseCount = 0;
        Long lastTxId;
        Long discLastTxId;
        long maxTxId = DtxConstants.DEFAULT_LAST_TX_VALUE;

        for (final DtxNode currNode : futureMap.keySet()) {
            Map<UUID, Long> discResultMap = null;
            for (int retryCount = 0; retryCount < DISCOVER_RETRIES && discResultMap == null; retryCount++) {
                try {
                    discResultMap = futureMap.get(currNode).get(DISCOVER_TIMEOUT, TimeUnit.SECONDS);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Discovered last transaction ID; local node=" + dtxConfig.getLocalPeer()
                                + ", target node=" + currNode + ", last tx IDs:" + discResultMap);
                    }
                    for (final UUID currResId : discResultMap.keySet()) {
                        lastTxId = resultMap.get(currResId);
                        discLastTxId = discResultMap.get(currResId);
                        if (lastTxId == null || (discLastTxId != null && lastTxId.compareTo(discLastTxId) < 0)) {
                            resultMap.put(currResId, discLastTxId);
                            maxTxId = Math.max(maxTxId, discLastTxId.longValue());
                        }
                        // updates cluster map
                        addClusterMapEntry(currResId, currNode, discLastTxId);
                    }
                    responseCount++;
                }
                catch (InterruptedException | TimeoutException e) {
                    if (retryCount >= DISCOVER_RETRIES) {
                        LOGGER.warn("Failed to update synchronization state, aborting discovery; local node="
                                + dtxConfig.getLocalPeer() + ", target node=" + currNode, e);
                        break;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                                "Failed to retrieve synchronization state, retrying; local node="
                                        + dtxConfig.getLocalPeer() + ", target node=" + currNode, e);
                    }
                    try {
                        Thread.sleep(DISCOVER_NODE_ID_RETRY_DELAY_MS);
                    }
                    catch (final InterruptedException e1) {
                        LOGGER.warn("Interrupted");
                    }
                }
                catch (final ExecutionException e) {
                    LOGGER.error("Error when updating synchronization state, aborting discovery; local node="
                            + dtxConfig.getLocalPeer() + ", target node=" + currNode, e);
                    // continue with other nodes if failure on one node
                    continue;
                }
            }
        }

        // merges last discovered transaction ID globally
        txInit.mergeLastTxCounters(maxTxId);

        final boolean quorumPresent = this.isQuorumOnline() && countsAsQuorum(responseCount);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Updating resource manager sync states; node=" + getNodeId() + ", node responses="
                    + responseCount + "/" + futureMap.size() + ", quorum present=" + quorumPresent
                    + ", tx ID update map=" + resultMap);
        }

        for (final UUID resUuid : resultMap.keySet()) {
            this.transactionMgr.evaluateResManagerSyncState(resUuid, resultMap.get(resUuid).longValue(), quorumPresent);
        }
    }

    /**
     * Synchronizes the given resource manager with the target node.
     * 
     * @param resId
     *            the affected resource manager's {@link UUID}
     * @param targetNode
     *            the target {@link DtxNode}
     * @param lastLocalTxId
     *            the ID of the last local transaction
     * @param targetTxId
     *            the target transaction ID to synchronize up to
     * @return the last effectively written transaction ID
     */
    final long synchronizeWithNode(final UUID resId, final DtxNode targetNode, final long lastLocalTxId,
            final long targetTxId) {
        readLockIfOutsideTransaction();
        try {
            if (!onlinePeers.contains(targetNode) || dtxConfig.getLocalPeer().equals(targetNode)) {
                // no use trying on local or offline target
                return transactionMgr.getLastCompleteTxIdForResMgr(resId);
            }

            final DistributedTask<Iterable<TxJournalEntry>> syncTask = new DistributedTask<Iterable<TxJournalEntry>>(
                    new SyncDataHandler(resId, lastLocalTxId, targetTxId));

            // TODO: find a way to invoke this without having to cast the result
            @SuppressWarnings("unchecked")
            final Future<Iterable<TxJournalEntry>> syncFuture = (Future<Iterable<TxJournalEntry>>) hazelcastInstance
                    .getExecutorService().submit(syncTask);

            final int expectedNbOfTx = Long.valueOf(targetTxId - lastLocalTxId).intValue();
            return transactionMgr.replayTransactions(resId, syncFuture.get(SYNCHRONIZE_TIMEOUT_S, TimeUnit.SECONDS),
                    expectedNbOfTx);

        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Exception while synchronizing", e);
            return transactionMgr.getLastCompleteTxIdForResMgr(resId);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets the online status regarding a strict quorum (more than half).
     * 
     * @return <code>true</code> if more than half the registered peers are online, <code>false</code> otherwise
     * @throws IllegalStateException
     *             if this instance is not {@link #init() initialized}
     */
    final boolean isQuorumOnline() throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (registeredPeers == null) {
                throw new IllegalStateException("Not initialized");
            }
            return this.onlinePeers.size() > (this.registeredPeers.size() / 2);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Computes the fact that a given number of responses is enough to form a quorum.
     * 
     * @param nbOfNodes
     *            the number of nodes represented
     * @return <code>true</code> if more than half the registered peers are represented by the node count,
     *         <code>false</code> otherwise
     * @throws IllegalStateException
     *             if this instance is not {@link #init() initialized}
     */
    final boolean countsAsQuorum(final int nbOfNodes) throws IllegalStateException {
        readLockIfOutsideTransaction();
        try {
            if (registeredPeers == null) {
                throw new IllegalStateException("Not initialized");
            }
            return nbOfNodes > (this.registeredPeers.size() / 2);
        }
        finally {
            readUnlockIfOutsideTransaction();
        }
    }

    /**
     * Gets a map of target nodes and last tx IDs for synchronization.
     * 
     * @param resId
     *            the target resource manager's ID
     * @param lowerLimit
     *            the lower limit above which to return last transaction IDs
     * @return a possibly empty {@link Map} with node IDs mapped to last complete transaction IDs
     */
    final Map<DtxNode, Long> getClusterMapInfo(final UUID resId, final Long lowerLimit) {
        try {
            mapLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted on waiting for lock", e);
        }
        try {
            final Map<DtxNode, Long> resUpdateMap = Maps.filterValues(clusterUpdateMap.row(resId),
                    Range.openClosed(lowerLimit, Long.valueOf(Long.MAX_VALUE)));
            // returns a defensive copy to avoid any concurrent modification exceptions
            return new HashMap<DtxNode, Long>(resUpdateMap);
        }
        finally {
            mapLock.readLock().unlock();
        }
    }

    /**
     * Posts a given {@link DtxEvent} to the internal and optionally the external {@link EventBus}.
     * 
     * Note: Calling this method needs external locking of the {@link #statusLock} to guarantee its success.
     * 
     * @param event
     *            the {@link DtxEvent} to post
     * @param includeExternal
     *            <code>true</code> if this
     * @throws NullPointerException
     *             if any argument is <code>null</code> or the {@link DtxManager} is shut down during execution
     */
    final void postEvent(final DtxEvent<?> event, final boolean includeExternal) {

        if (internalEventBus == null) {
            throw new IllegalStateException("EventBus is null");
        }
        this.internalEventBus.post(event);

        if (includeExternal) {
            this.externalEventBus.post(event);
        }
    }

    /**
     * Uses the internal {@link #clusterUpdateMap} filled by discovery operations to estimate if there is a quorum of
     * up-to-date nodes on the cluster.
     * 
     * @param resId
     *            the requested resource manager's {@link UUID}
     * @return <code>true</code> if a quorum of up-to-date nodes are found for this resource manager, <code>false</code>
     *         otherwise
     */
    final boolean isUpToDateOnClusterMapQuorum(final UUID resId) {
        try {
            mapLock.readLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted on waiting for lock", e);
        }
        try {
            final Map<DtxNode, Long> targetRow = clusterUpdateMap.row(resId);
            if (targetRow == null) {
                return false;
            }

            final Collection<Long> lastTxCounters = targetRow.values();
            final int upToDateCount = Collections.frequency(lastTxCounters, Collections.max(lastTxCounters));

            return countsAsQuorum(upToDateCount);
        }
        finally {
            mapLock.readLock().unlock();
        }
    }

    /**
     * Removes the information stored in the in-memory map of resource managers and their update status for a given
     * node.
     * 
     * @param node
     *            the {@link DtxNode} for which to remove the information
     */
    final void removeClusterMapInfo(final DtxNode node) {
        try {
            mapLock.writeLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted waiting for lock", e);
        }
        try {
            clusterUpdateMap.columnMap().remove(node);
        }
        finally {
            mapLock.writeLock().unlock();
        }
    }

    /**
     * Note: only call this while holding a write lock on {@link #statusLock}!
     */
    private final void refreshOnlinePeers() {
        if (hazelcastInstance == null) {
            throw new IllegalStateException("Hazelcast instance not started");
        }
        onlinePeers.clear();
        for (final Member onlineMember : hazelcastInstance.getCluster().getMembers()) {
            addOnlinePeer(onlineMember);
        }
    }

    /**
     * Adds a newly arrived peer to the list of online peers, if it corresponds to a previously registered peer.
     * 
     * @param newMember
     *            the new {@link Member} to inspect and possibly add
     */
    private final void addOnlinePeer(final Member newMember) {

        DtxNode newDtxNode = null;
        if (newMember.localMember()) {
            newDtxNode = new DtxNode(getNodeId(), newMember.getInetSocketAddress());
            assert newDtxNode.equals(dtxConfig.getLocalPeer());
        }
        else {
            for (int retryCount = 0; retryCount < DISCOVER_RETRIES && (newDtxNode == null); retryCount++) {
                // gets the new peer's node ID
                final DistributedTask<Uuid> idTask = new DistributedTask<Uuid>(new NodeIdUpdateHandler(), newMember);
                hazelcastInstance.getExecutorService().execute(idTask);

                try {
                    newDtxNode = new DtxNode(TxProtobufUtils.fromUuid(idTask.get(DISCOVER_TIMEOUT, TimeUnit.SECONDS)),
                            newMember.getInetSocketAddress());
                }
                catch (NullPointerException | InterruptedException | ExecutionException | TimeoutException e) {
                    if (retryCount >= DISCOVER_RETRIES) {
                        LOGGER.warn("Failed to determine new member's node ID, aborting discovery; local node="
                                + dtxConfig.getLocalPeer() + ", new member=" + newMember, e);
                        return;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Failed to retrieve node ID from member, retrying; local node="
                                + dtxConfig.getLocalPeer() + ", new member=" + newMember);
                    }
                    try {
                        Thread.sleep(DISCOVER_NODE_ID_RETRY_DELAY_MS);
                    }
                    catch (final InterruptedException e1) {
                        LOGGER.warn("Interrupted");
                    }
                }
            }
            if (newDtxNode == null) {
                return;
            }
        }
        if (onlinePeers.contains(newDtxNode)) {
            return;
        }
        if (registeredPeers.contains(newDtxNode)) {
            onlinePeers.add(newDtxNode);
            postEvent(new DtxClusterEvent(this, DtxClusterEventType.ADDED, newDtxNode, isQuorumOnline()), true);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Added online peer; local node=" + dtxConfig.getLocalPeer() + ", new node=" + newDtxNode
                        + ", online nodes=" + onlinePeers);
            }
            return;
        }
        LOGGER.warn("Unregistered peer detected, ignoring; this=" + this.getNodeId() + ", peer=" + newDtxNode);
    }

    private final void removeOnlinePeer(final Member remMember) {
        for (final Iterator<DtxNode> nodeIter = onlinePeers.iterator(); nodeIter.hasNext();) {
            final DtxNode currNode = nodeIter.next();
            if (currNode.getAddress().equals(remMember.getInetSocketAddress())) {
                nodeIter.remove();
                postEvent(new DtxClusterEvent(this, DtxClusterEventType.REMOVED, currNode, isQuorumOnline()), true);
                break;
            }
        }
    }

    /**
     * Acquires the read lock on {@link #statusLock} if the current thread is not executing a transaction.
     */
    private final void readLockIfOutsideTransaction() {
        if ((transactionMgr == null) || !transactionMgr.holdsTransactionLock()) {
            statusLock.readLock().lock();
            return;
        }
    }

    /**
     * Releases the read lock on {@link #statusLock} if the current thread is not executing a transaction.
     */
    private final void readUnlockIfOutsideTransaction() {
        if ((transactionMgr == null) || !transactionMgr.holdsTransactionLock()) {
            if (statusLock.getReadHoldCount() > 0) {
                statusLock.readLock().unlock();
            }
            return;
        }
    }

    private final void addClusterMapEntry(final UUID resId, final DtxNode node, final Long lastTxId) {
        if (lastTxId == null) {
            return;
        }
        try {
            mapLock.writeLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted waiting for lock", e);
        }
        try {
            clusterUpdateMap.put(resId, node, lastTxId);
        }
        finally {
            mapLock.writeLock().unlock();
        }
    }

    private final void removeClusterMapInfo(final UUID resId) {
        try {
            mapLock.writeLock().lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted waiting for lock", e);
        }
        try {
            clusterUpdateMap.rowMap().remove(resId);
        }
        finally {
            mapLock.writeLock().unlock();
        }
    }
}
