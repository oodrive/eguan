package io.eguan.vold.model;

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

import static io.eguan.dtx.DtxResourceManagerState.UNREGISTERED;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxResourceManagerState;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskApi;
import io.eguan.dtx.DtxTaskFutureVoid;
import io.eguan.dtx.DtxTaskInfo;
import io.eguan.dtx.events.DtxResourceManagerEvent;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.nbdsrv.NbdServer;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgServerHandler;
import io.eguan.nrs.NrsStorageConfigKey;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.proto.vvr.VvrRemote.Item;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.Files;
import io.eguan.vvr.configuration.keys.DescriptionConfigkey;
import io.eguan.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsOwnerUuidConfigKey;
import io.eguan.vvr.configuration.keys.NameConfigKey;
import io.eguan.vvr.configuration.keys.NodeConfigKey;
import io.eguan.vvr.persistence.repository.NrsRepository;
import io.eguan.vvr.remote.VvrDtxRmContext;
import io.eguan.vvr.remote.VvrRemoteUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;

import com.google.common.eventbus.Subscribe;
import com.google.protobuf.MessageLite;

/**
 * Manage {@link VvrOld}s. Create a VVR for the owner of the VOLD, exposes the {@link VvrOld}s by JMX.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * @author pwehrle
 * 
 */
public final class VvrManager implements VvrManagerMXBean, MsgServerHandler {
    private static final Logger LOGGER = Constants.LOGGER;

    /**
     * Control the purge of the {@link Vvr}s. Delete the IBS and the persistence of the {@link Vvr}s.
     * 
     */
    static final class VvrPurger extends ThreadPoolExecutor {

        /**
         * Purge a {@link Vvr}.
         * 
         */
        static class VvrPurge implements Runnable, Files.DeleteRecursiveProgress {
            /** {@link Vvr} to purge */
            private final Vvr vvr;
            private final AtomicBoolean stopRequested;

            VvrPurge(final Vvr vvr, final AtomicBoolean stopRequested) {
                super();
                this.vvr = vvr;
                this.stopRequested = stopRequested;
            }

            @Override
            public final void run() {
                // First pass: delete only contents
                purgeVvrResources(true);

                // Second pass: remove directories
                if (!stopRequested.get()) {
                    purgeVvrResources(false);
                }
                if (!stopRequested.get()) {
                    LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " purge completed");
                }
            }

            private final void purgeVvrResources(final boolean keepDirs) {
                // Clear Ibp
                {
                    final ArrayList<File> ibsIbp = vvr.getIbp();
                    for (final File file : ibsIbp) {
                        // Need to stop?
                        if (stopRequested.get()) {
                            LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " purge aborted");
                            return;
                        }
                        deleteVvrResource(file, keepDirs);
                    }
                }

                // Need to stop?
                if (stopRequested.get()) {
                    LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " purge aborted");
                    return;
                }

                // Clear IbpGen
                {
                    final File ibsGen = vvr.getIbpGen();
                    deleteVvrResource(ibsGen, keepDirs);

                    // Need to stop?
                    if (stopRequested.get()) {
                        LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " purge aborted");
                        return;
                    }
                }

                // Clear Storage
                final File nrs = vvr.getNrsStorage();
                deleteVvrResource(nrs, keepDirs);
            }

            /**
             * Delete the given directory.
             * 
             * @param dir
             *            directory to delete
             * @param keepDir
             *            when <code>true</code>, keep it to be able to reload the configuration in case of a partial
             *            deletion
             */
            private final void deleteVvrResource(final File dir, final boolean keepDir) {
                try {
                    Files.deleteRecursive(dir.toPath(), keepDir, this);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to delete " + dir, t);
                }
            }

            @Override
            public final FileVisitResult notify(final Path deleted) {
                // Abort delete if stop requested
                return stopRequested.get() ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
            }
        }

        /** Maximum number of threads to purge the {@link Vvr}s */
        private static final String THREAD_COUNT_PROPERTY = "io.eguan.vold.purgeThreadCount";
        private static final int THREAD_COUNT;
        static {
            final String threadCount = System.getProperty(THREAD_COUNT_PROPERTY, "1");
            THREAD_COUNT = Integer.valueOf(threadCount).intValue();
        }

        /** <code>true</code> when the operations must stop. */
        private final AtomicBoolean stopRequested = new AtomicBoolean(false);

        VvrPurger(final UUID owner) {
            super(0, THREAD_COUNT, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
                @Override
                public final Thread newThread(final Runnable r) {
                    return new Thread(r, "VVR purge " + owner);
                }
            });
        }

        @Override
        public final void shutdown() {
            stopRequested.set(true);
            super.shutdown();
        }

        /**
         * Request the purge of the given Vvr.
         * 
         * @param vvr
         *            {@link Vvr} to purge.
         */
        final void purgeVvr(final Vvr vvr) {
            final VvrPurge purge = new VvrPurge(vvr, stopRequested);
            execute(purge);
        }
    }

    /** MBean server */
    private final MBeanServer mbeanServer;
    /** Owner of the VVRs */
    private final UUID owner;
    /** Node where the VOLD is running */
    private final UUID node;
    /** Directory containing the persistence of the VVRs */
    private final File vvrDir;
    /** Template configuration to create new VVRs */
    private final MetaConfiguration vvrConfigurationTemplate;
    /** Associated iSCSI server */
    private final IscsiServer iscsiServer;
    /** Associated NBD server */
    private final NbdServer nbdServer;

    /** Protection for list of VVR and VvrManager Dtx state */
    private final ReadWriteLock vvrsLock = new ReentrantReadWriteLock();

    /**
     * VVRs managed by this manager. Synchronize on <code>vvrs</code> when the Vvr manager is registered on the MBean
     * server.
     */
    @GuardedBy(value = "vvrsLock")
    private final Map<UUID, Vvr> vvrs = new HashMap<>();

    @GuardedBy(value = "vvrsLock")
    private volatile DtxResourceManagerState vvrManagerState = UNREGISTERED;

    /** Send remote messages if not <code>null</code>. */
    private final AtomicReference<MsgClientStartpoint> syncClientRef = new AtomicReference<>();
    /** Dtx manager reference or <code>null</code>. */
    private final AtomicReference<DtxTaskApi> dtxTaskApiRef = new AtomicReference<>();
    /** Source of sent messages. The node UUID as a Uuid */
    private final Uuid msgSource;

    /** VVRs to purge. */
    private VvrPurger purger;

    /** Lock for JMX register/unregister */
    private final Lock jmxLock = new ReentrantLock();
    /** VVR manager JMX object name */
    @GuardedBy(value = "jmxLock")
    private ObjectName vvrManagerObjName;

    /** Timeout to wait for the end of the purger (in seconds) */
    private static final long PURGER_WAIT_END = 60L;

    public VvrManager(@Nonnull final MBeanServer mbeanServer, @Nonnull final UUID owner, @Nonnull final UUID node,
            final File vvrDir, final MetaConfiguration vvrConfigurationTemplate, final IscsiServer iscsiServer,
            final NbdServer nbdServer) {
        super();
        this.mbeanServer = Objects.requireNonNull(mbeanServer);
        this.owner = Objects.requireNonNull(owner);
        this.node = Objects.requireNonNull(node);
        this.vvrDir = vvrDir;
        this.vvrConfigurationTemplate = vvrConfigurationTemplate;
        this.iscsiServer = iscsiServer;
        this.nbdServer = nbdServer;
        this.msgSource = VvrRemoteUtils.newUuid(node);
    }

    /**
     * Initialize the manager.
     * 
     * @throws IOException
     */

    public final void init(final DtxManager dtxManager) throws IOException {
        // Check VVR directory
        if (!vvrDir.exists()) {
            if (!vvrDir.mkdirs()) {
                throw new IOException("Failed to create directory '" + vvrDir.getAbsolutePath() + "'");
            }
        }
        if (!vvrDir.isDirectory()) {
            throw new IllegalStateException("'" + vvrDir.getAbsolutePath() + "' is not a directory");
        }

        // Load VVR list to handle transactions
        loadVvrs();

        dtxRegisterVvrManager(dtxManager);
    }

    /**
     * Release the manager and all resources managed by this.
     */
    public final void fini() {

        // Initialize stop VVR purge
        if (purger != null) {
            try {
                purger.shutdown();
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while shuting down VVR purge", t);
            }
        }

        dtxUnregisterVvrManager();

        // Deregister vvr manager from JMX
        unregisterVvrManagerMXBean();

        // Release all the vvrs
        releaseVvrs();

        // Wait for the end of the purge of the VVRs
        if (purger != null) {
            try {
                purger.awaitTermination(PURGER_WAIT_END, TimeUnit.SECONDS);
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while stopping VVR purge", t);
            }
            purger = null;
        }
    }

    /**
     * Set client to send remote messages.
     * 
     * @param syncClient
     *            new client to set, may be null
     */
    public final void setSyncClient(final MsgClientStartpoint syncClient) {
        this.syncClientRef.set(syncClient);
    }

    private final void loadVvrs() throws IOException {
        vvrsLock.writeLock().lock();
        try {
            LOGGER.debug("resources init requested");
            // Initialize purge of VVRs
            assert purger == null;
            purger = new VvrPurger(owner);

            // Load existing VVRs
            final File[] vvrFiles = vvrDir.listFiles();
            for (final File vvrFile : vvrFiles) {

                final Vvr vvr = loadVvr(vvrFile);
                if (vvr == null) {
                    // Already logged
                    continue;
                }

                if (vvr.isDeleted()) {
                    purger.purgeVvr(vvr);
                    LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " to purge");
                    continue;
                }

                // Init VVR. Go on even if init fails to alert the administrator.
                try {
                    vvr.init(mbeanServer);
                }
                catch (final Throwable t) {
                    LOGGER.warn("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " initialization failed", t);
                }

                // Now, the VVR is managed even if it's not initialized
                vvrs.put(vvr.getUuidUuid(), vvr);
            }
        }
        finally {
            vvrsLock.writeLock().unlock();
        }
    }

    private final void releaseVvrs() {
        LOGGER.debug("resources release requested");

        // Release the VVRs - need to lock vvrs here
        vvrsLock.writeLock().lock();
        try {
            for (final Vvr vvr : vvrs.values()) {
                // Unregister VVR from DTX
                releaseVvr(vvr, true);
            }
            vvrs.clear();
        }
        finally {
            vvrsLock.writeLock().unlock();
        }

        // Reset reference
        this.dtxTaskApiRef.set(null);
        LOGGER.debug("VVR manager uninitialization done");
    }

    private final void registerVvrs() {
        // Register the VVRs
        vvrsLock.readLock().lock();
        try {
            for (final Vvr vvr : vvrs.values()) {
                // VVR from DTX
                vvr.dtxRegister((DtxManager) dtxTaskApiRef.get());
            }
        }
        finally {
            vvrsLock.readLock().unlock();
        }
    }

    private final void unregisterVvrs() {
        // Release the VVRs
        vvrsLock.readLock().lock();
        try {
            for (final Vvr vvr : vvrs.values()) {
                releaseVvr(vvr, false);
            }
        }
        finally {
            vvrsLock.readLock().unlock();
        }
    }

    /**
     * Handle state changes for the resource managers
     */
    @Subscribe
    public final void resourceManagerStateChanged(final DtxResourceManagerEvent event) {
        final DtxResourceManagerState newState = event.getNewState();
        final DtxResourceManagerState oldState = event.getPreviousState();

        vvrsLock.writeLock().lock();
        try {
            final UUID resId = event.getResourceManagerId();
            try {
                if (resId.equals(owner)) {
                    // VvrManager
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("VvrManager state transitioning; oldState=" + oldState + ", newState=" + newState
                                + ", lastState=" + vvrManagerState + ", ID=" + owner);
                    }
                    assert vvrManagerState == oldState;
                    vvrManagerState = newState;
                    switch (newState) {
                    case UP_TO_DATE:
                        LOGGER.debug("VVR manager up to date");
                        registerVvrs();
                        // Expose vvr manager into JMX
                        try {
                            registerVvrManagerMXbean(owner);
                        }
                        catch (final JMException e) {
                            LOGGER.error("Error while registering the VVR manager MXBean", e);
                        }
                        break;
                    case LATE:
                    case UNREGISTERED:
                    case SYNCHRONIZING:
                    case UNDETERMINED:
                        if (oldState == DtxResourceManagerState.UP_TO_DATE) {
                            LOGGER.debug("VVR manager late");
                            // Deregister vvr manager into JMX
                            unregisterVvrManagerMXBean();
                            unregisterVvrs();
                        }
                        break;
                    case POST_SYNC_PROCESSING:
                        break;

                    }
                }
                else {
                    // Get the requested VVR
                    final Vvr vvr = vvrs.get(resId);
                    if (vvr == null) {
                        LOGGER.warn("VVR not found: " + resId);
                        return;
                    }
                    switch (newState) {
                    case POST_SYNC_PROCESSING:
                        break;

                    case UP_TO_DATE:
                        LOGGER.debug("VVR " + vvr.getUuid() + " up to date");
                        startVvr(vvr);
                        try {
                            registerVvrMXBean(vvr);
                        }
                        catch (final JMException e) {
                            LOGGER.error("Error while registering the VVR  MXBean", e);
                        }
                        break;
                    case LATE:
                    case UNREGISTERED:
                    case SYNCHRONIZING:
                    case UNDETERMINED:
                        if (oldState == DtxResourceManagerState.UP_TO_DATE) {
                            LOGGER.debug("VVR " + vvr.getUuid() + " late");
                            // Unregister MXBean
                            unregisterVvrMXBean(vvr);
                            stopVvr(vvr);
                        }
                        break;
                    }
                }
            }
            catch (final Exception e) {
                LOGGER.error("Failed to handle state for a resource manager", e);
            }
        }
        finally {
            vvrsLock.writeLock().unlock();
        }
    }

    /**
     * Register a vvr manager in dtx manager
     * 
     * @param dtxManager
     */
    private final void dtxRegisterVvrManager(final DtxManager dtxManager) {
        this.dtxTaskApiRef.set(dtxManager);
        // Must get all the events on the VvrManager state
        dtxManager.registerDtxEventListener(this);
    }

    private final void dtxUnregisterVvrManager() {
        // Deregister DTX event listener first (state changes must no interfere with shutdown)
        final DtxManager dtxManager = (DtxManager) dtxTaskApiRef.get();
        try {
            dtxManager.unregisterDtxEventListener(this);
        }
        catch (final Exception e) {
            // Ignored
            LOGGER.warn("Error while unregistering DTX event listener", e);
        }

        // Deregister from DTX
        vvrsLock.writeLock().lock();
        try {
            dtxManager.unregisterResourceManager(owner);
            vvrManagerState = UNREGISTERED;
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering vvr manager from DTX", t);
        }
        finally {
            vvrsLock.writeLock().unlock();
        }

    }

    @Override
    public final String createVvr(final String name, final String description) throws IllegalStateException {
        // UUID of the VVR to create
        final UUID vvrUuid = UUID.randomUUID();
        final UUID taskId = submitCreateVvrTask(name, description, vvrUuid);

        // Wait for task end
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final DtxTaskFutureVoid future = new DtxTaskFutureVoid(taskId, dtxTaskApi);
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }

        final VvrManagerTaskInfo task = (VvrManagerTaskInfo) dtxTaskApi.getDtxTaskInfo(taskId);
        return task.getTargetId();
    }

    @Override
    public final void createVvr(final String name, final String description, final String uuid)
            throws IllegalStateException {
        // UUID of the VVR to create
        final UUID vvrUuid = UUID.fromString(Objects.requireNonNull(uuid, "uuid"));
        final UUID taskId = submitCreateVvrTask(name, description, vvrUuid);

        // Wait for task end
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final DtxTaskFutureVoid future = new DtxTaskFutureVoid(taskId, dtxTaskApi);
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String createVvrNoWait(final String name, final String description) {
        // UUID of the VVR to create
        final UUID vvrUuid = UUID.randomUUID();
        final UUID taskId = submitCreateVvrTask(name, description, vvrUuid);
        return taskId.toString();
    }

    @Override
    public final String createVvrNoWait(final String name, final String description, final String uuid) {
        // UUID of the VVR to create
        final UUID vvrUuid = UUID.fromString(Objects.requireNonNull(uuid, "uuid"));
        final UUID taskId = submitCreateVvrTask(name, description, vvrUuid);
        return taskId.toString();
    }

    /**
     * Submit a Dtx task for the creation of a new {@link Vvr}.
     * 
     * @param name
     * @param description
     * @param vvrUuid
     * @return the {@link UUID} of the submitted task.
     */
    private final UUID submitCreateVvrTask(final String name, final String description, final UUID vvrUuid) {
        // Build payload
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();
        opBuilder.setUuid(VvrRemoteUtils.newUuid(vvrUuid));

        // Get the UUID of the root snapshot
        final UUID rootUuid = UUID.randomUUID();
        opBuilder.setSnapshot(VvrRemoteUtils.newUuid(rootUuid));

        // Create item if needed
        if (name != null || description != null) {
            final VvrRemote.Item.Builder itemBuilder = VvrRemote.Item.newBuilder();
            if (name != null) {
                itemBuilder.setName(name);
            }
            if (description != null) {
                itemBuilder.setDescription(description);
            }
            opBuilder.setItem(itemBuilder.build());
        }

        // Submit transaction
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final UUID taskId = VvrRemoteUtils.submitTransaction(opBuilder, dtxTaskApi, owner, msgSource, Type.VVR,
                OpCode.CREATE);
        return taskId;
    }

    /**
     * Create a new {@link NrsRepository} on the local node.
     * 
     * @param vvrUuid
     * @param name
     *            name of the VVR, may be <code>null</code>
     * @param description
     *            description of the VVR, may be <code>null</code>
     * @param rootUuid
     *            {@link UUID} of the root snapshot
     * @return the newly created repository
     * @throws VvrManagementException
     */
    private final NrsRepository createVvr(final @Nonnull UUID vvrUuid, final String name, final String description,
            final @Nonnull UUID rootUuid) throws VvrManagementException {
        vvrsLock.writeLock().lock();
        try {
            // Check if the given UUID exists
            if (vvrs.containsKey(vvrUuid)) {
                throw new VvrManagementException("Failed to create VVR: id " + vvrUuid + " already exists");
            }

            // Create the configuration for the new VVR
            final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();

            final String vvrUuidStr = vvrUuid.toString();

            // - Set name
            if (name != null) {
                newKeyValueMap.put(NameConfigKey.getInstance(), name);
            }

            // - Set description
            if (description != null) {
                newKeyValueMap.put(DescriptionConfigkey.getInstance(), description);
            }

            // - Update owner of IBS, it's the vvr UUID
            newKeyValueMap.put(IbsOwnerUuidConfigKey.getInstance(), vvrUuid);

            // - Update node
            newKeyValueMap.put(NodeConfigKey.getInstance(), node);

            // - Update persistence path
            {
                final File nrsStorage = new File(vvrDir, vvrUuidStr);
                nrsStorage.mkdirs();
                newKeyValueMap.put(NrsStorageConfigKey.getInstance(), nrsStorage);

            }

            // - Update ibp paths
            {
                final IbsIbpPathConfigKey key = IbsIbpPathConfigKey.getInstance();
                final ArrayList<File> ibps = key.getTypedValue(vvrConfigurationTemplate);
                final ArrayList<File> ibpsVvr = new ArrayList<>(ibps.size());
                for (int i = 0; i < ibps.size(); i++) {
                    final File ibp = new File(ibps.get(i), vvrUuidStr);
                    ibpsVvr.add(ibp);
                    ibp.mkdirs();
                }
                newKeyValueMap.put(key, ibpsVvr);
            }

            // - Update ibpgen
            {
                final IbsIbpGenPathConfigKey key = IbsIbpGenPathConfigKey.getInstance();
                final File ibpgenDir = key.getTypedValue(vvrConfigurationTemplate);
                final File ibpgenDirVvr = new File(ibpgenDir, vvrUuidStr);
                ibpgenDirVvr.mkdirs();
                newKeyValueMap.put(key, ibpgenDirVvr);
            }

            // VVR creation: set configuration, uuid, name and description
            final MetaConfiguration vvrConfiguration = vvrConfigurationTemplate
                    .copyAndAlterConfiguration(newKeyValueMap);
            final NrsRepository.Builder builder = new NrsRepository.Builder();
            builder.configuration(vvrConfiguration).ownerId(owner).nodeId(node).syncClientRef(syncClientRef)
                    .dtxTaskApiRef(dtxTaskApiRef).uuid(vvrUuid).name(name).description(description);
            builder.rootUuid(rootUuid);
            final NrsRepository nrsRepository = builder.create();
            final Vvr vvr = new Vvr(owner, nrsRepository, dtxTaskApiRef, iscsiServer, nbdServer, node);
            LOGGER.info("VVR '" + name + "' created, uuid=" + vvrUuidStr);

            // Clean VVR resources on failure
            try {
                vvr.init(mbeanServer);
            }
            catch (RuntimeException | Error e) {
                vvr.delete();
                LOGGER.error("VVR '" + name + "', uuid=" + vvrUuidStr + " initialization failed (deleted)", e);
                throw e;
            }

            // Now, the VVR is managed
            vvrs.put(vvrUuid, vvr);

            if (vvrManagerState == DtxResourceManagerState.UP_TO_DATE) {
                // Register vvr in dtx manager (will do the start and expose mxbeans if necessary)
                vvr.dtxRegister((DtxManager) dtxTaskApiRef.get());
            }
            return nrsRepository;
        }
        catch (final ConfigValidationException e) {
            LOGGER.error("Invalid configuration");
            final List<ValidationError> errors = e.getValidationReport();
            for (final ValidationError error : errors) {
                LOGGER.error("\t" + ValidationError.getFormattedErrorReport(error));
            }
            throw new VvrManagementException("Failed to create VVR " + name, e);
        }
        catch (final Exception e) {
            LOGGER.error("Failed to create VVR " + name, e);
            throw new VvrManagementException("Failed to create VVR " + name, e);
        }
        finally {
            vvrsLock.writeLock().unlock();
        }
    }

    /**
     * Delete a {@link Vvr}.
     * 
     * @see {@link VvrManager#delete(String)}.
     * 
     * @param uuid
     */
    private final void delete(final UUID uuid) {
        vvrsLock.writeLock().lock();
        try {
            // Get the requested VVR
            final Vvr vvr = vvrs.get(uuid);
            if (vvr == null) {
                throw new IllegalArgumentException("VVR not found: " + uuid);
            }

            // Flag as deleted (may fail if the VVR is started)
            vvr.delete();

            // Release VVR
            try {
                releaseVvr(vvr, true);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while releasing " + vvr, t);
            }

            // Purge requested
            vvrs.remove(uuid);
            if (purger != null) {
                purger.purgeVvr(vvr);
            }
            LOGGER.info("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " to purge");
        }
        finally {
            vvrsLock.writeLock().unlock();
        }
    }

    /**
     * Starts or stops the given VVR. If the source node is set, start/stop on ther source only.
     * 
     * @param uuid
     *            VVR
     * @param node
     *            UUID of the node or <code>null</code> for all nodes
     * @param start
     *            <code>true</code> to start, <code>false</code> to stop
     */
    private final void startStop(final UUID uuid, final Uuid node, final boolean start) {
        vvrsLock.readLock().lock();
        try {
            final Vvr vvr = vvrs.get(uuid);
            if (vvr == null) {
                throw new IllegalArgumentException("VVR not found: " + uuid);
            }
            if (node == null || VvrRemoteUtils.equalsUuid(node, msgSource)) {
                if (start) {
                    try {
                        vvr.doStart();
                    }
                    catch (final JMException e) {
                        throw new IllegalStateException("Failed to start " + uuid, e);
                    }
                }
                else {
                    vvr.doStop();
                }
            }
        }
        finally {
            vvrsLock.readLock().unlock();
        }
    }

    @Override
    public final void delete(final String uuid) throws IllegalStateException {
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final UUID taskId = createDeleteTask(uuid);

        // Wait for task end
        final DtxTaskFutureVoid future = new DtxTaskFutureVoid(taskId, dtxTaskApi);
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String deleteNoWait(final String uuid) throws IllegalArgumentException {
        final UUID taskId = createDeleteTask(uuid);
        return taskId.toString();
    }

    /**
     * Launch a task to delete the given VVR.
     * 
     * @param uuid
     *            uuid of the VVR to delete
     * @return uuid of the task.
     */
    private final UUID createDeleteTask(final String uuid) {
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();

        // UUID of the VVR to delete
        opBuilder.setUuid(VvrRemoteUtils.newUuid(UUID.fromString(Objects.requireNonNull(uuid, "uuid"))));

        // Submit transaction
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final UUID taskId = VvrRemoteUtils.submitTransaction(opBuilder, dtxTaskApi, owner, msgSource, Type.VVR,
                OpCode.DELETE);
        return taskId;
    }

    @Override
    public final String getOwnerUuid() {
        return owner.toString();
    }

    @Override
    public final VvrManagerTask getVvrManagerTask(final String taskId) {
        final UUID taskUuid = UUID.fromString(Objects.requireNonNull(taskId, "taskId"));
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();

        final VvrManagerTaskInfo taskInfo = (VvrManagerTaskInfo) dtxTaskApi.getDtxTaskInfo(taskUuid);
        final DtxTaskAdm taskAdm = dtxTaskApi.getTask(taskUuid);
        // check the resource ID
        if (getOwnerUuid().equals(taskAdm.getResourceId()))
            return new VvrManagerTask(taskAdm.getTaskId(), taskAdm.getStatus(), taskInfo);
        else
            return null;
    }

    @Override
    public final VvrManagerTask[] getVvrManagerTasks() {
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();

        final DtxTaskAdm[] tasksAdm = dtxTaskApi.getResourceManagerTasks(owner);
        final VvrManagerTask[] tasks = new VvrManagerTask[tasksAdm.length];

        for (int i = 0; i < tasksAdm.length; i++) {
            final VvrManagerTaskInfo taskInfo = (VvrManagerTaskInfo) dtxTaskApi.getDtxTaskInfo(UUID
                    .fromString(tasksAdm[i].getTaskId()));
            tasks[i] = new VvrManagerTask(tasksAdm[i].getTaskId(), tasksAdm[i].getStatus(), taskInfo);
        }
        return tasks;
    }

    @Override
    public final Map<String, String> getVvrConfiguration() {
        final Map<String, String> result = new HashMap<>();
        if (vvrConfigurationTemplate != null) {
            final Properties properties = vvrConfigurationTemplate.getCompleteConfigurationAsProperties();
            for (final String key : properties.stringPropertyNames()) {
                result.put(key, properties.getProperty(key));
            }
        }
        return result;
    }

    /**
     * Load a persisted VVR (NrsRepository implementation) from a directory.
     * 
     * @param vvrDir
     *            the path where the VVR is stored
     * @return the loaded Vvr or <code>null</code>
     */
    private final Vvr loadVvr(final File vvrDir) {

        final String vvrPath = vvrDir.getAbsolutePath();
        final UUID vvrUuid;
        try {
            vvrUuid = UUID.fromString(vvrDir.getName());
        }
        catch (final Throwable t) {
            LOGGER.warn("Unexpected directory: '" + vvrPath + "' (ignored)", t);
            return null;
        }

        final NrsRepository repository;
        try {
            final NrsRepository.Builder builder = new NrsRepository.Builder();
            builder.repositoryPath(vvrDir).ownerId(owner).nodeId(node).syncClientRef(syncClientRef)
                    .dtxTaskApiRef(dtxTaskApiRef).uuid(vvrUuid);
            repository = builder.load();
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to load VVR in '" + vvrPath + "' (ignored)", t);
            return null;
        }

        // // Check vvr owner
        // MetaConfiguration configuration = repository.getConfiguration();
        // UUID ownerConfig = OwnerConfigKey.getInstance().getTypedValue(configuration);
        // if(!ownerConfig.equals(owner)){
        // LOGGER.warn("Wrong owner: '" + vvrPath + "', owner expected=" + owner + ", actual=" + ownerConfig +
        // " (ignored)");
        // return null;
        // }

        LOGGER.info("VVR loaded (path='" + vvrPath + "', name='" + repository.getName() + "')");
        return new Vvr(owner, repository, dtxTaskApiRef, iscsiServer, nbdServer, node);
    }

    /**
     * Release resources allocated by the {@link Vvr}.
     * 
     * @param vvr
     * @param full
     *            <code>true</code> when the vvr must be completely released
     */
    private final void releaseVvr(final Vvr vvr, final boolean full) {
        // Unregister MXBean
        try {
            unregisterVvrMXBean(vvr);
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering " + vvr, t);
        }

        // Stop the VVR (if started)
        stopVvr(vvr);

        // Unregister VVR from DTX
        try {
            vvr.dtxUnregister((DtxManager) this.dtxTaskApiRef.get());
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering " + vvr, t);
        }

        // Uninitialize the VVR (if initialized)
        if (full) {
            if (vvr.isInitialized()) {
                try {
                    vvr.fini();
                }
                catch (final Throwable t) {
                    // Ignored
                    LOGGER.warn("Error while uninitializing " + vvr, t);
                }
            }
        }
    }

    /**
     * Start the VVR provided according to its started flag afterward the function
     */
    private final void startVvr(final Vvr vvr) {
        // Start VVR if requested
        final boolean start = vvr.wasStarted();
        if (start) {
            try {
                vvr.doStart(false);
            }
            catch (final Throwable t) {
                LOGGER.error("VVR '" + vvr.getName() + "', uuid=" + vvr.getUuid() + " failed to start");
            }
        }
    }

    /**
     * Stop the VVR provided according to its started flag afterward the function
     */
    private final void stopVvr(final Vvr vvr) {
        // Stop the VVR (if started)
        if (vvr.isStarted()) {
            try {
                vvr.doStop(false);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while stopping " + vvr, t);
            }
        }
    }

    /**
     * Register a VVR manager in the MBean server.
     * 
     * @param vvr
     *            VVR to register
     * @throws JMException
     */
    private final void registerVvrManagerMXbean(final UUID uid) throws JMException {
        jmxLock.lock();
        try {
            vvrManagerObjName = VvrObjectNameFactory.newVvrManagerObjectName(uid);
            mbeanServer.registerMBean(this, vvrManagerObjName);
        }
        finally {
            jmxLock.unlock();
        }
    }

    /**
     * Unregister a VVR in the MBean server.
     * 
     * @param vvr
     *            VVR to unregister
     */
    private final void unregisterVvrManagerMXBean() {
        jmxLock.lock();
        try {
            if (vvrManagerObjName != null) {
                mbeanServer.unregisterMBean(vvrManagerObjName);
                vvrManagerObjName = null;
            }
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering the VVR manager MXBean", t);
        }
        finally {
            jmxLock.unlock();
        }
    }

    /**
     * Register a VVR in the MBean server.
     * 
     * @param vvr
     *            VVR to register
     * @throws JMException
     */
    private final void registerVvrMXBean(final Vvr vvr) throws JMException {
        final ObjectName objectName = VvrObjectNameFactory.newVvrObjectName(owner, vvr.getUuidUuid());
        mbeanServer.registerMBean(vvr, objectName);
        LOGGER.info("VVR " + vvr.getUuid() + " registered");
    }

    /**
     * Unregister a VVR in the MBean server.
     * 
     * @param vvr
     *            VVR to unregister
     */
    private final void unregisterVvrMXBean(final Vvr vvr) {
        try {
            final ObjectName objectName = VvrObjectNameFactory.newVvrObjectName(owner, vvr.getUuidUuid());
            mbeanServer.unregisterMBean(objectName);
            LOGGER.info("VVR " + vvr.getUuid() + " unregistered");
        }
        catch (final InstanceNotFoundException e) {
            // Ignored
            LOGGER.debug(vvr + " not found");
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering " + vvr, t);
        }
    }

    // / Remote message handling ///

    @Override
    public final MessageLite handleMessage(final MessageLite message) {
        final RemoteOperation op = (RemoteOperation) message;
        final Type type = op.getType();
        final OpCode opCode = op.getOp();

        if (type == Type.VVR
                && (opCode == OpCode.CREATE || opCode == OpCode.DELETE || opCode == OpCode.START || opCode == OpCode.STOP)) {
            handleMessageVvr(op);
            return null;
        }
        else {
            return handleMessageItem(op);
        }
    }

    private final void handleMessageVvr(final RemoteOperation op) {
        final OpCode opCode = op.getOp();
        final UUID vvrUuid = getMessageOperationObjectUuid(op);

        switch (opCode) {
        case CREATE: {
            // Create VVR with a given UUID, name and description
            final String name;
            final String description;
            if (op.hasItem()) {
                final Item item = op.getItem();
                name = item.hasName() ? item.getName() : null;
                description = item.hasDescription() ? item.getDescription() : null;
            }
            else {
                name = null;
                description = null;
            }

            // The UUID of the root snapshot must be set
            assert op.hasSnapshot();
            final UUID rootUuid = VvrRemoteUtils.fromUuid(op.getSnapshot());
            try {
                createVvr(vvrUuid, name, description, rootUuid);
            }
            catch (final VvrManagementException e) {
                // TODO now vold is not more up-to-date
                throw new IllegalStateException("Failed to create VVR, uuid=" + vvrUuid + ", name=" + name
                        + ", description=" + description);
            }
        }
            break;
        case DELETE: {
            delete(vvrUuid);
        }
            break;
        case START: {
            final Uuid node = op.getSource();
            startStop(vvrUuid, node, true);
        }
            break;
        case STOP: {
            final Uuid node = op.getSource();
            startStop(vvrUuid, node, false);
        }
            break;
        default:
            LOGGER.warn("Unexpected message on VVR, op=" + op);
        }
    }

    /**
     * Handle a message on a VVR item.
     * 
     * @param op
     */
    private final MessageLite handleMessageItem(final RemoteOperation op) {
        // VVR id should be set
        if (!op.hasVvr()) {
            throw new IllegalArgumentException("VVR not set");
        }
        final UUID vvrId = VvrRemoteUtils.fromUuid(op.getVvr());

        final Vvr vvr;
        vvrsLock.readLock().lock();
        try {
            vvr = vvrs.get(vvrId);
        }
        finally {
            vvrsLock.readLock().unlock();
        }
        if (vvr == null) {
            LOGGER.warn("VVR " + vvrId + " not found, op=" + op.getOp() + ", type=" + op.getType());
            return null;
        }
        return vvr.handleMsg(op);
    }

    private final UUID getMessageOperationObjectUuid(final RemoteOperation op) {
        if (!op.hasUuid()) {
            throw new IllegalArgumentException("Uuid not set");
        }
        final Uuid uuid = op.getUuid();
        return VvrRemoteUtils.fromUuid(uuid);
    }

    /* VVR manager DTX resource management */

    public final Boolean prepare(final VvrDtxRmContext context) throws XAException {
        final VvrDtxRmContext vvrDtxRmContext = context;
        // TODO handle errors
        try {
            handleMessage(vvrDtxRmContext.getOperation());
            return Boolean.TRUE;
        }
        catch (final IllegalStateException e) {
            // Most of the time, a pre-condition error
            LOGGER.error("Exception on prepare", e);
            final XAException xaException = new XAException(XAException.XA_RBROLLBACK);
            xaException.initCause(e);
            throw xaException;
        }
    }

    public final void commit(final VvrDtxRmContext vvrDtxRmContext) throws XAException {
        // TODO real commit
    }

    public final void rollback(final VvrDtxRmContext vvrDtxRmContext) throws XAException {
        // TODO real rollback
    }

    public final void processPostSync() {
        // Nothing to do
    }

    /**
     * Constructs information for vvr manager task.
     * 
     * @param operation
     *            The complete operation used to construct the task info
     */
    public final DtxTaskInfo createTaskInfo(final RemoteOperation operation) {
        VvrManagerTaskOperation op;
        VvrManagerTargetType targetType;

        final String source = VvrRemoteUtils.fromUuid(operation.getSource()).toString();
        final String targetId = VvrRemoteUtils.fromUuid(operation.getUuid()).toString();

        switch (operation.getType()) {
        case VVR:
            targetType = VvrManagerTargetType.VVR;
            break;
        default:
            throw new AssertionError("type=" + operation.getType());
        }

        switch (operation.getOp()) {
        case CREATE:
            op = VvrManagerTaskOperation.CREATE;
            break;
        case DELETE:
            op = VvrManagerTaskOperation.DELETE;
            break;
        case START:
            op = VvrManagerTaskOperation.START;
            break;
        case STOP:
            op = VvrManagerTaskOperation.STOP;
            break;
        default:
            throw new AssertionError("type=" + operation.getOp());
        }
        return new VvrManagerTaskInfo(source, op, targetType, targetId);
    }
}
