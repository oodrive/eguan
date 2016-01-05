package io.eguan.vold.model;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskApi;
import io.eguan.dtx.DtxTaskFutureVoid;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.nbdsrv.NbdServer;
import io.eguan.nrs.NrsStorageConfigKey;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.Type;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpPathConfigKey;
import io.eguan.vvr.configuration.keys.StartedConfigKey;
import io.eguan.vvr.persistence.repository.VvrTaskInfo;
import io.eguan.vvr.remote.VvrRemoteUtils;
import io.eguan.vvr.repository.core.api.Device;
import io.eguan.vvr.repository.core.api.FutureVoid;
import io.eguan.vvr.repository.core.api.Snapshot;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository;
import io.eguan.vvr.repository.core.api.VvrItem;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository.ItemChangedEvent;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository.ItemCreatedEvent;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository.ItemDeletedEvent;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.management.AttributeChangeNotification;
import javax.management.JMException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.slf4j.Logger;

import com.google.common.eventbus.Subscribe;
import com.google.protobuf.MessageLite;

/**
 * The class {@link #Vvr()} encapsulates a {@link VersionedVolumeRepository} and is exported as a {@link MXBean}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 * @author jmcaba
 * 
 */
final class Vvr implements VvrMXBean, NotificationEmitter {
    private static final Logger LOGGER = Constants.LOGGER;

    /** Owner of the VVR */
    private final UUID owner;
    /** VVR instance */
    private final VersionedVolumeRepository vvrInstance;
    /** Transaction manager */
    private final AtomicReference<DtxTaskApi> dtxTaskApiRef;
    /** Server to register new items */
    private volatile MBeanServer server;
    /** Associated iSCSI server */
    private final IscsiServer iscsiServer;
    /** Associated NBD server */
    private final NbdServer nbdServer;
    /** Current node */
    private final UUID node;
    /** Device MXBeans */
    private final Map<UUID, WeakReference<VvrDevice>> devicesMXBeans = new ConcurrentHashMap<>();
    /** JMX notifications support */
    private final NotificationBroadcasterSupport notificationEmitter = new NotificationBroadcasterSupport();
    /** JMX notification sequence number */
    private final AtomicInteger notificationSequenceNumber = new AtomicInteger(1);

    Vvr(final UUID owner, final VersionedVolumeRepository vvrInstance,
            @Nonnull final AtomicReference<DtxTaskApi> dtxTaskApiRef, @Nonnull final IscsiServer iscsiServer,
            @Nonnull final NbdServer nbdServer, @Nonnull final UUID node) {
        super();
        this.owner = owner;
        this.vvrInstance = vvrInstance;
        this.dtxTaskApiRef = Objects.requireNonNull(dtxTaskApiRef);
        this.iscsiServer = Objects.requireNonNull(iscsiServer);
        this.nbdServer = Objects.requireNonNull(nbdServer);
        this.node = Objects.requireNonNull(node);
    }

    final void init(final MBeanServer server) {
        this.server = server;
        vvrInstance.init();
        vvrInstance.registerItemEvents(this);
    }

    final void fini() {
        try {
            vvrInstance.unregisterItemEvents(this);
        }
        catch (final Throwable t) {
            LOGGER.warn("Error while unregistering from " + toString() + " event bus", t);
        }
        vvrInstance.fini();
        this.server = null;
    }

    final void dtxRegister(@Nonnull final DtxManager dtxManager) {
        dtxManager.registerResourceManager(vvrInstance);
    }

    final void dtxUnregister(@Nonnull final DtxManager dtxManager) {
        dtxManager.unregisterResourceManager(vvrInstance.getId());
    }

    /**
     * Register the snapshots and the devices to the given {@link MBeanServer}.
     * 
     * @param server
     * @throws JMException
     */
    private final void registerElements() throws JMException {
        final UUID vvrUuid = getUuidUuid();

        // Register snapshots
        {
            final Set<UUID> snapshots = vvrInstance.getSnapshots();
            for (final UUID uuid : snapshots) {
                final Snapshot snapshot = vvrInstance.getSnapshot(uuid);
                final VvrSnapshot mbean = new VvrSnapshot(snapshot);
                final ObjectName objectName = VvrObjectNameFactory.newSnapshotObjectName(owner, vvrUuid, uuid);
                server.registerMBean(mbean, objectName);
            }
        }

        // Register devices
        {
            final Set<UUID> devices = vvrInstance.getDevices();
            for (final UUID uuid : devices) {
                final Device device = vvrInstance.getDevice(uuid);
                final VvrDevice mbean = VvrDevice.loadVvrDevice(device, iscsiServer, nbdServer, node);
                devicesMXBeans.put(uuid, new WeakReference<>(mbean));
                final ObjectName objectName = VvrObjectNameFactory.newDeviceObjectName(owner, vvrUuid, uuid);
                server.registerMBean(mbean, objectName);
            }
        }
    }

    private final void unregisterElements() throws JMException {
        final UUID vvrUuid = getUuidUuid();

        // Unregister snapshots
        {
            final Set<UUID> snapshots = vvrInstance.getSnapshots();
            for (final UUID uuid : snapshots) {
                try {
                    final ObjectName objectName = VvrObjectNameFactory.newSnapshotObjectName(owner, vvrUuid, uuid);
                    server.unregisterMBean(objectName);
                }
                catch (final Throwable t) {
                    LOGGER.warn(toString() + ": error while unregistering snapshot " + uuid, t);
                }
            }
        }

        // Unregister devices
        {
            final Set<UUID> devices = vvrInstance.getDevices();
            for (final UUID uuid : devices) {
                try {
                    final ObjectName objectName = VvrObjectNameFactory.newDeviceObjectName(owner, vvrUuid, uuid);
                    server.unregisterMBean(objectName);

                    // Must deactivate the device (keep state if activated)
                    final WeakReference<VvrDevice> deviceRef = devicesMXBeans.remove(uuid);
                    final VvrDevice device = deviceRef.get();
                    if (device != null) {
                        device.doDeactivate();
                    }
                }
                catch (final Throwable t) {
                    LOGGER.warn(toString() + ": error while unregistering device " + uuid, t);
                }
            }
        }
    }

    final boolean wasStarted() {
        return StartedConfigKey.getInstance().getTypedValue(vvrInstance.getConfiguration()).booleanValue();
    }

    final ArrayList<File> getIbp() {
        final MetaConfiguration configuration = vvrInstance.getConfiguration();
        return IbsIbpPathConfigKey.getInstance().getTypedValue(configuration);
    }

    final File getIbpGen() {
        final MetaConfiguration configuration = vvrInstance.getConfiguration();
        return IbsIbpGenPathConfigKey.getInstance().getTypedValue(configuration);
    }

    final File getNrsStorage() {
        final MetaConfiguration configuration = vvrInstance.getConfiguration();
        return NrsStorageConfigKey.getInstance().getTypedValue(configuration);
    }

    /*
     * Should be getUuidString(), but, in this case, the JMX attribute would be 'uuidString'
     * 
     * @see io.eguan.vold.model.VvrMXBean#getUuid()
     */
    @Override
    public final String getUuid() {
        return getUuidUuid().toString();
    }

    final UUID getUuidUuid() {
        return vvrInstance.getUuid();
    }

    @Override
    public final String getOwnerUuid() {
        return vvrInstance.getOwnerId().toString();
    }

    @Override
    public final String getName() {
        return vvrInstance.getName();
    }

    @Override
    public final void setName(final String name) {
        final FutureVoid futureTask = vvrInstance.setName(name);
        if (futureTask == null) {
            return;
        }
        try {
            futureTask.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // propagate failure
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final String getDescription() {
        return vvrInstance.getDescription();
    }

    @Override
    public final void setDescription(final String description) {
        final FutureVoid futureTask = vvrInstance.setDescription(description);
        if (futureTask == null) {
            return;
        }
        try {
            futureTask.get();
        }
        catch (InterruptedException | ExecutionException e) {
            // propagate failure
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final boolean isInitialized() {
        return vvrInstance.isInitialized();
    }

    @Override
    public final boolean isStarted() {
        return vvrInstance.isStarted();
    }

    @Override
    public final void start() {
        startStopVvr(true);
    }

    @Override
    public final String startNoWait() {
        return startStopVvrNoWait(true).toString();
    }

    final void doStart() throws JMException {
        doStart(true);
    }

    final void doStart(final boolean saveState) throws JMException {
        vvrInstance.start(saveState);

        // Expose the children of the VVR
        try {
            registerElements();
        }
        catch (RuntimeException | JMException e) {
            LOGGER.error("Failed to register the children of " + toString(), e);

            // Start failed: unregister and stop VVR
            doStop(false);

            throw e;
        }
    }

    @Override
    public final void stop() {
        startStopVvr(false);
    }

    @Override
    public final String stopNoWait() {
        return startStopVvrNoWait(false).toString();
    }

    final void doStop() {
        // Can not stop VVR if a device is activated
        for (final WeakReference<VvrDevice> deviceRef : devicesMXBeans.values()) {
            final VvrDevice device = deviceRef.get();
            if (device != null && device.isActiveLocally()) {
                throw new IllegalStateException("Device " + device.getUuid() + " is activated");
            }
        }

        doStop(true);
    }

    final void doStop(final boolean saveState) {

        // Unregister the children of the VVR
        try {
            unregisterElements();
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering " + toString() + " elements", t);
        }

        vvrInstance.stop(saveState);
    }

    private final void startStopVvr(final boolean start) {
        final UUID taskId = startStopVvrNoWait(start);

        // Wait for task end
        final DtxTaskFutureVoid future = new DtxTaskFutureVoid(taskId, dtxTaskApiRef.get());
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    private final UUID startStopVvrNoWait(final boolean start) {
        // Build payload
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();

        // UUID of the VVR to start/stop
        final UUID vvrUuid = vvrInstance.getId();
        opBuilder.setUuid(VvrRemoteUtils.newUuid(vvrUuid));

        // Submit transaction
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();
        final UUID taskId = VvrRemoteUtils.submitTransaction(opBuilder, dtxTaskApi, owner,
                VvrRemoteUtils.newUuid(node), Type.VVR, start ? OpCode.START : OpCode.STOP);
        return taskId;
    }

    @Override
    public final VvrTask getVvrTask(final String taskId) {
        final UUID taskUuid = UUID.fromString(taskId);
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();

        final VvrTaskInfo taskInfo = (VvrTaskInfo) dtxTaskApi.getDtxTaskInfo(taskUuid);
        final DtxTaskAdm taskAdm = dtxTaskApi.getTask(taskUuid);

        // check the resource ID
        if (getUuid().equals(taskAdm.getResourceId()))
            return new VvrTask(taskAdm.getTaskId(), taskAdm.getStatus(), taskInfo);
        else
            return null;
    }

    @Override
    public final VvrTask[] getVvrTasks() {
        final DtxTaskApi dtxTaskApi = dtxTaskApiRef.get();

        final DtxTaskAdm[] tasksAdm = dtxTaskApi.getResourceManagerTasks(getUuidUuid());
        final VvrTask[] tasks = new VvrTask[tasksAdm.length];

        for (int i = 0; i < tasksAdm.length; i++) {
            final VvrTaskInfo taskInfo = (VvrTaskInfo) dtxTaskApi.getDtxTaskInfo(UUID.fromString(tasksAdm[i]
                    .getTaskId()));
            tasks[i] = new VvrTask(tasksAdm[i].getTaskId(), tasksAdm[i].getStatus(), taskInfo);
        }
        return tasks;
    }

    /**
     * Persist the state <code>deleted</code>.
     */
    final void delete() {
        this.vvrInstance.delete();
    }

    final boolean isDeleted() {
        return vvrInstance.isDeleted();
    }

    @Subscribe
    public final void recordItemCreation(final ItemCreatedEvent e) {
        assert vvrInstance == e.getRepository();

        // server should not be null, be may be null if so event have been added before unregistering
        if (server == null) {
            return;
        }

        // Ignore event if the VVR is not started (probably a remote event)
        if (!vvrInstance.isStarted()) {
            return;
        }

        final VvrItem item = e.getItem();
        final UUID uuid = item.getUuid();

        try {
            if (item instanceof Snapshot) {
                final Snapshot snapshot = (Snapshot) item;
                final VvrSnapshot mbean = new VvrSnapshot(snapshot);
                final ObjectName objectName = VvrObjectNameFactory.newSnapshotObjectName(owner, getUuidUuid(), uuid);
                server.registerMBean(mbean, objectName);
                LOGGER.info(toString() + ": new snapshot " + uuid);
            }
            else if (item instanceof Device) {
                final Device device = (Device) item;
                final VvrDevice mbean = VvrDevice.loadVvrDevice(device, iscsiServer, nbdServer, node);
                devicesMXBeans.put(uuid, new WeakReference<>(mbean));
                final ObjectName objectName = VvrObjectNameFactory.newDeviceObjectName(owner, getUuidUuid(), uuid);
                server.registerMBean(mbean, objectName);
                LOGGER.info(toString() + ": new device " + uuid);
            }
            else {
                LOGGER.error("Unexpected item " + item.getClass().getCanonicalName());
            }
        }
        catch (final Throwable t) {
            LOGGER.warn(toString() + ": failed to register item " + uuid);
        }
    }

    @Subscribe
    public final void recordItemDeletion(final ItemDeletedEvent e) {
        assert vvrInstance == e.getRepository();

        // server should not be null, be may be null if so event have been added before unregistering
        if (server == null) {
            return;
        }

        // Ignore event if the VVR is not started (probably a remote event)
        if (!vvrInstance.isStarted()) {
            return;
        }

        final UUID uuid = e.getItemUuid();
        final Class<? extends VvrItem> clazz = e.getClazz();
        try {
            if (Snapshot.class.isAssignableFrom(clazz)) {
                final ObjectName objectName = VvrObjectNameFactory.newSnapshotObjectName(owner, getUuidUuid(), uuid);
                server.unregisterMBean(objectName);
                LOGGER.info(toString() + ": snapshot " + uuid + " deleted");
            }
            else if (Device.class.isAssignableFrom(clazz)) {
                final ObjectName objectName = VvrObjectNameFactory.newDeviceObjectName(owner, getUuidUuid(), uuid);
                server.unregisterMBean(objectName);
                devicesMXBeans.remove(uuid);
                LOGGER.info(toString() + ": device " + uuid + " deleted");
            }
            else {
                LOGGER.error("Unexpected item " + clazz.getCanonicalName());
            }
        }
        catch (final Throwable t) {
            LOGGER.warn(toString() + ": failed to register item " + uuid);
        }
    }

    @Subscribe
    public final void recordItemChange(final ItemChangedEvent e) {
        assert vvrInstance == e.getRepository();

        // server should not be null, be may be null if so event have been added before unregistering
        if (server == null) {
            return;
        }

        final UUID itemUuid = e.getItemUuid();
        final ObjectName objectName;
        final String oldValue = e.getOldValue();
        final String newValue = e.getNewValue();

        // Find item type
        if (itemUuid.equals(getUuidUuid())) {
            objectName = VvrObjectNameFactory.newVvrObjectName(owner, getUuidUuid());
        }
        else {
            // Ignore event if the VVR is not started (probably a remote event)
            if (!vvrInstance.isStarted()) {
                return;
            }
            if (devicesMXBeans.containsKey(itemUuid)) {
                objectName = VvrObjectNameFactory.newDeviceObjectName(owner, getUuidUuid(), itemUuid);
            }
            else {
                objectName = VvrObjectNameFactory.newSnapshotObjectName(owner, getUuidUuid(), itemUuid);
            }
        }

        // Notify
        final Notification n;
        switch (e.getType()) {
        case NAME:
            n = new AttributeChangeNotification(objectName, notificationSequenceNumber.getAndIncrement(),
                    System.currentTimeMillis(), "Name changed", "Name", "string", oldValue, newValue);
            break;
        case DESCRIPTION:
            n = new AttributeChangeNotification(objectName, notificationSequenceNumber.getAndIncrement(),
                    System.currentTimeMillis(), "Description changed", "Description", "string", oldValue, newValue);
            break;
        default:
            n = null;
            break;
        }
        if (n != null) {
            notificationEmitter.sendNotification(n);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#addNotificationListener(javax.management.NotificationListener,
     * javax.management.NotificationFilter, java.lang.Object)
     */
    @Override
    public final void addNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws IllegalArgumentException {
        notificationEmitter.addNotificationListener(listener, filter, handback);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationEmitter#removeNotificationListener(javax.management.NotificationListener,
     * javax.management.NotificationFilter, java.lang.Object)
     */
    @Override
    public final void removeNotificationListener(final NotificationListener listener, final NotificationFilter filter,
            final Object handback) throws ListenerNotFoundException {
        notificationEmitter.removeNotificationListener(listener, filter, handback);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#removeNotificationListener(javax.management.NotificationListener)
     */
    @Override
    public final void removeNotificationListener(final NotificationListener listener) throws ListenerNotFoundException {
        notificationEmitter.removeNotificationListener(listener);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.management.NotificationBroadcaster#getNotificationInfo()
     */
    @Override
    public final MBeanNotificationInfo[] getNotificationInfo() {
        final String[] types = new String[] { AttributeChangeNotification.ATTRIBUTE_CHANGE };
        final String name = AttributeChangeNotification.class.getName();
        final String description = "An attribute has changed";
        final MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);
        return new MBeanNotificationInfo[] { info };
    }

    final MessageLite handleMsg(final RemoteOperation op) {
        return vvrInstance.handleMsg(op);
    }

    @Override
    public final String toString() {
        return "VVR[" + getName() + ", " + getUuid() + "]";
    }

}
