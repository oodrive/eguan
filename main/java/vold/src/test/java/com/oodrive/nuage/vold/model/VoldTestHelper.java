package com.oodrive.nuage.vold.model;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.dtx.DtxTaskApiAbstract.TaskKeeperParameters;
import com.oodrive.nuage.dtx.DtxTaskStatus;
import com.oodrive.nuage.dtx.config.DtxConfigurationContext;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperAbsoluteDurationConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperAbsoluteSizeConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperMaxDurationConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperMaxSizeConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperPurgeDelayConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperPurgePeriodConfigKey;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.iscsisrv.IscsiServerConfigurationContext;
import com.oodrive.nuage.iscsisrv.IscsiServerInetAddressConfigKey;
import com.oodrive.nuage.iscsisrv.IscsiServerPortConfigKey;
import com.oodrive.nuage.nbdsrv.NbdServerConfigurationContext;
import com.oodrive.nuage.nbdsrv.NbdServerInetAddressConfigKey;
import com.oodrive.nuage.nbdsrv.NbdServerPortConfigKey;
import com.oodrive.nuage.nrs.NrsConfigurationContext;
import com.oodrive.nuage.nrs.NrsStorageConfigKey;
import com.oodrive.nuage.vold.EnableIscsiConfigKey;
import com.oodrive.nuage.vold.EnableNbdConfigKey;
import com.oodrive.nuage.vold.NodeConfigKey;
import com.oodrive.nuage.vold.OwnerConfigKey;
import com.oodrive.nuage.vold.PeersConfigKey;
import com.oodrive.nuage.vold.ServerEndpointInetAddressConfigKey;
import com.oodrive.nuage.vold.ServerEndpointPortConfigKey;
import com.oodrive.nuage.vold.Vold;
import com.oodrive.nuage.vold.VoldConfigurationContext;
import com.oodrive.nuage.vvr.configuration.CommonConfigurationContext;
import com.oodrive.nuage.vvr.configuration.IbsConfigurationContext;
import com.oodrive.nuage.vvr.configuration.keys.HashAlgorithmConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.IbsCompressionConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.IbsIbpPathConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.IbsLogLevelConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.IbsOwnerUuidConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.StartedConfigKey;

/**
 * Create, delete a VOLD directory and manage a Vold instance.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * @author jmcaba
 * @author pwehrle
 */
public class VoldTestHelper implements NotificationListener {
    private static final Logger LOGGER = LoggerFactory.getLogger("vold.test");

    private final static String TEMP_FILE_PREFIX = "vold";
    private final static String VOLD_DIR = "vold";
    private final static String IBS_IBP_DIR = "ibp";
    private final static String IBS_IBPGEN_DIR = "ibpGen";
    final static String VOLD_CONFIG_FILE = "vold.cfg";
    final static String VVR_TEMPLATE = "vvr.cfg";
    final static String VVR_PERSISTENCE_DIR = "vvrs";

    private final CompressionType compression;
    private final HashAlgorithm hash;
    private final Boolean vvrStarted;
    private File baseFile;
    private File voldFile;
    private File ibpFile;
    private File ibpGenFile;
    private File vvrsFile;

    protected final String VOLD_OWNER_UUID_TEST_STR;
    protected final UUID VOLD_OWNER_UUID_TEST;

    /** Number of MX Bean after Vold init */
    public static int MXBEANS_NUMBER_INIT = 3;

    private static final long WAIT_MXBEAN_TIMEOUT = 10 * 1000L; // 20 s

    /** Lock for latch map put/get */
    private final Lock latchLock = new ReentrantLock();
    /** Hashmap for object name */
    @GuardedBy(value = "latchLock")
    private final Map<ObjectName, CountDownLatch> objectNameLatchs = new HashMap<ObjectName, CountDownLatch>();

    // For now, must run VOLD in a background thread
    private Vold vold;

    public static enum CompressionType {
        no, front, back
    }

    public VoldTestHelper() {
        this(Boolean.TRUE);
    }

    public VoldTestHelper(final Boolean vvrStarted) {
        this(CompressionType.no, HashAlgorithm.MD5, vvrStarted);
    }

    public VoldTestHelper(final CompressionType compression, final HashAlgorithm hash, final Boolean vvrStarted) {
        this(UUID.randomUUID(), compression, hash, vvrStarted);
    }

    public VoldTestHelper(final UUID voldOwner, final CompressionType compression, final HashAlgorithm hash,
            final Boolean vvrStarted) {
        super();
        this.compression = compression;
        this.hash = hash;
        this.vvrStarted = vvrStarted;
        VOLD_OWNER_UUID_TEST = voldOwner;
        VOLD_OWNER_UUID_TEST_STR = VOLD_OWNER_UUID_TEST.toString();
    }

    public final String createTemporary() throws IOException {
        createTempFolders();
        // Default vold file
        writeVoldFile(null, null, null, null, null, null, null, null, null);
        writeVvrTemplateFile();
        return baseFile.toString();
    }

    public final String createTemporary(final TaskKeeperParameters parameters) throws IOException {
        createTempFolders();
        // Default vold file
        writeVoldFile(null, null, null, null, null, null, null, null, parameters);
        writeVvrTemplateFile();
        return baseFile.toString();
    }

    public final String createTemporary(final String nodeUuid, final String serverAddress, final Integer port,
            final String remotePeers, final String iscsiServerAddress, final Integer iscsiServerPort,
            final String nbdServerAddress, final Integer nbdServerPort) throws IOException {

        // Change MxBeans number
        if (remotePeers != null) {
            MXBEANS_NUMBER_INIT = 5;
        }
        createTempFolders();
        writeVoldFile(nodeUuid, serverAddress, port, remotePeers, iscsiServerAddress, iscsiServerPort,
                nbdServerAddress, nbdServerPort, null);
        writeVvrTemplateFile();
        return baseFile.toString();
    }

    public final String createTemporary(final String nodeUuid, final String serverAddress, final Integer port,
            final String remotePeers, final String iscsiServerAddress, final Integer iscsiServerPort,
            final String nbdServerAddress, final Integer nbdServerPort, final TaskKeeperParameters parameters)
            throws IOException {

        // Change MxBeans number
        if (remotePeers != null) {
            MXBEANS_NUMBER_INIT = 5;
        }
        createTempFolders();
        writeVoldFile(nodeUuid, serverAddress, port, remotePeers, iscsiServerAddress, iscsiServerPort, null, null,
                parameters);
        writeVvrTemplateFile();
        return baseFile.toString();
    }

    public final void createTempFolders() throws IOException {
        baseFile = Files.createTempDirectory(TEMP_FILE_PREFIX).toFile();
        voldFile = new File(baseFile, VOLD_DIR);
        ibpFile = new File(baseFile, IBS_IBP_DIR);
        ibpGenFile = new File(baseFile, IBS_IBPGEN_DIR);
        vvrsFile = new File(voldFile, VVR_PERSISTENCE_DIR);
        Assert.assertTrue(voldFile.mkdirs());
        Assert.assertTrue(ibpFile.mkdirs());
        Assert.assertTrue(ibpGenFile.mkdirs());
        Assert.assertTrue(vvrsFile.mkdirs());
    }

    /**
     * Write <code>vold.cfg</code>. The iSCSI server is started by default.
     * 
     * @throws IOException
     */
    private final void writeVoldFile(final String nodeUuid, final String serverAddress, final Integer port,
            final String remotePeers, final String iscsiServerAddress, final Integer iscsiServerPort,
            final String nbdServerAddress, final Integer nbdServerPort, final TaskKeeperParameters parameters)
            throws IOException {
        final Properties properties = new Properties();
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();
        final DtxConfigurationContext dtxContext = DtxConfigurationContext.getInstance();

        final IscsiServerConfigurationContext iscsiContext = IscsiServerConfigurationContext.getInstance();

        final NbdServerConfigurationContext nbdContext = NbdServerConfigurationContext.getInstance();

        properties.setProperty(voldContext.getPropertyKey(OwnerConfigKey.getInstance()), VOLD_OWNER_UUID_TEST_STR);
        properties.setProperty(voldContext.getPropertyKey(NodeConfigKey.getInstance()), UUID.randomUUID().toString());
        properties.setProperty(voldContext.getPropertyKey(EnableIscsiConfigKey.getInstance()), "yes");
        properties.setProperty(voldContext.getPropertyKey(EnableNbdConfigKey.getInstance()), "yes");

        if (nodeUuid != null) {
            properties.setProperty(voldContext.getPropertyKey(NodeConfigKey.getInstance()), nodeUuid);
        }

        if (serverAddress != null) {
            properties.setProperty(voldContext.getPropertyKey(ServerEndpointInetAddressConfigKey.getInstance()),
                    serverAddress);
        }

        if (port != null) {
            properties.setProperty(voldContext.getPropertyKey(ServerEndpointPortConfigKey.getInstance()),
                    port.toString());
        }

        if (remotePeers != null) {
            properties.setProperty(voldContext.getPropertyKey(PeersConfigKey.getInstance()), remotePeers);
        }

        if (iscsiServerPort != null) {
            properties.setProperty(iscsiContext.getPropertyKey(IscsiServerPortConfigKey.getInstance()),
                    iscsiServerPort.toString());
        }

        if (iscsiServerAddress != null) {
            properties.setProperty(iscsiContext.getPropertyKey(IscsiServerInetAddressConfigKey.getInstance()),
                    iscsiServerAddress);
        }

        if (nbdServerPort != null) {
            properties.setProperty(nbdContext.getPropertyKey(NbdServerPortConfigKey.getInstance()),
                    nbdServerPort.toString());
        }

        if (nbdServerAddress != null) {
            properties.setProperty(nbdContext.getPropertyKey(NbdServerInetAddressConfigKey.getInstance()),
                    nbdServerAddress);
        }

        if (parameters != null) {
            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperAbsoluteDurationConfigKey.getInstance()),
                    String.valueOf(parameters.getAbsoluteDuration()));
            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperMaxDurationConfigKey.getInstance()),
                    String.valueOf(parameters.getMaxDuration()));
            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperAbsoluteSizeConfigKey.getInstance()),
                    String.valueOf(parameters.getAbsoluteSize()));
            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperMaxSizeConfigKey.getInstance()),
                    String.valueOf(parameters.getMaxSize()));

            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperPurgePeriodConfigKey.getInstance()),
                    String.valueOf(parameters.getPeriod()));
            properties.setProperty(dtxContext.getPropertyKey(DtxTaskKeeperPurgeDelayConfigKey.getInstance()),
                    String.valueOf(parameters.getDelay()));
        }

        try (FileOutputStream fos = new FileOutputStream(new File(voldFile, VOLD_CONFIG_FILE))) {
            properties.store(fos, "VVR template");
        }
    }

    private final void writeVvrTemplateFile() throws IOException {
        final Properties properties = new Properties();
        final IbsConfigurationContext ibsContext = IbsConfigurationContext.getInstance();
        final CommonConfigurationContext configurationContext = CommonConfigurationContext.getInstance();
        final NrsConfigurationContext nrsConfigurationContext = NrsConfigurationContext.getInstance();

        properties.setProperty(nrsConfigurationContext.getPropertyKey(NrsStorageConfigKey.getInstance()),
                this.voldFile.getAbsolutePath());
        properties.setProperty(ibsContext.getPropertyKey(IbsIbpPathConfigKey.getInstance()),
                this.ibpFile.getAbsolutePath());
        properties.setProperty(ibsContext.getPropertyKey(IbsIbpGenPathConfigKey.getInstance()),
                this.ibpGenFile.getAbsolutePath());
        properties.setProperty(ibsContext.getPropertyKey(IbsCompressionConfigKey.getInstance()), compression.name());
        properties.setProperty(configurationContext.getPropertyKey(HashAlgorithmConfigKey.getInstance()), hash.name());
        properties.setProperty(ibsContext.getPropertyKey(IbsOwnerUuidConfigKey.getInstance()), UUID.randomUUID()
                .toString());
        properties.setProperty(configurationContext
                .getPropertyKey(com.oodrive.nuage.vvr.configuration.keys.NodeConfigKey.getInstance()), UUID
                .randomUUID().toString());

        properties.put(configurationContext.getPropertyKey(StartedConfigKey.getInstance()), vvrStarted.toString());

        properties.setProperty(ibsContext.getPropertyKey(IbsLogLevelConfigKey.getInstance()), "off");

        try (FileOutputStream fos = new FileOutputStream(new File(voldFile, VVR_TEMPLATE))) {
            properties.store(fos, "VVR template");
        }
    }

    public final File getIbpFile() {
        return ibpFile;
    }

    public final String getIbpPath() {
        return ibpFile.getAbsolutePath();
    }

    public final File getIbpGenFile() {
        return ibpGenFile;
    }

    public final String getIbpGenPath() {
        return ibpGenFile.getAbsolutePath();
    }

    public final File getVoldFile() {
        return voldFile;
    }

    public final File getVoldCfgFile() {
        return new File(voldFile, VOLD_CONFIG_FILE);
    }

    public final String getVoldPath() {
        return voldFile.getAbsolutePath();
    }

    public final File getVvrsFile() {
        return vvrsFile;
    }

    public final String getVvrsPath() {
        return vvrsFile.getAbsolutePath();
    }

    public final boolean isVvrStarted() {
        return vvrStarted.booleanValue();
    }

    public final void destroy() {
        try {
            com.oodrive.nuage.utils.Files.deleteRecursive(baseFile.toPath());
        }
        catch (final IOException e) {
            LOGGER.warn("Failed to delete baseFile='" + baseFile.getAbsolutePath() + "'", e);
        }
    }

    /**
     * @brief Mimic init method from Vold but allowing change of MBeanServer
     * @param server
     * @param voldObjName
     * @throws IOException
     * @throws JMException
     */
    private final void initVoldForTest(final MBeanServer server, final ObjectName voldObjName) throws IOException,
            JMException {
        // Open a .lock file and try to take a lock
        callVoldMethod(getVoldMethod("openAndLockVoldChannel"));

        // Load vold configuration
        callVoldMethod(getVoldMethod("setLoadMetaConfiguration"));

        // Initialize JMX
        callVoldMethod(getVoldMethod("setMBeanServer", MBeanServer.class), server);

        // Export Vold MXBean
        callVoldMethod(getVoldMethod("setVoldObjName", ObjectName.class), voldObjName);

        LOGGER.debug("VOLD server initialized FOR Tests");
    }

    /**
     * Return a vold Method to call with callVoldMethod
     * 
     * @param methodName
     * @param parameterTypes
     * @return
     * @see callVoldMethod
     */
    private final Method getVoldMethod(final String methodName, final Class<?>... parameterTypes) {
        try {
            final Method method = vold.getClass().getDeclaredMethod(methodName, parameterTypes);
            return method;
        }
        catch (final Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Call a vold Method whatever it's privacy is.
     * 
     * @param method
     * @param args
     * @return
     */
    private final Object callVoldMethod(final Method method, final Object... args) {
        try {
            method.setAccessible(true);
            return method.invoke(vold, args);
        }
        catch (final Exception e) {
            throw new AssertionError(e);
        }
    }

    public final void initVold(final MBeanServer server, final ObjectName voldObjName) throws JMException, IOException,
            InterruptedException {
        vold = new Vold(voldFile);

        initVoldForTest(server, voldObjName);
    }

    public final void finiVold() throws IOException {
        if (vold != null) {
            vold.fini();
            vold = null;
        }
    }

    public final void start() throws JMException, IOException, InterruptedException {
        assert vold == null;
        vold = new Vold(voldFile);
        vold.init(false);
        addMXBeanRegistrationListener();
        vold.start();
    }

    public final void stop() throws IOException {

        try {
            removeMXBeanRegistrationListener();
        }
        catch (InstanceNotFoundException | ListenerNotFoundException e) {
            LOGGER.warn("Failed to remove MBean Listener", e);
        }
        if (vold != null) {
            vold.stop();
            vold.fini();
            vold = null;
        }
    }

    public final ObjectName newVoldObjectName() {
        return VvrObjectNameFactory.newVoldObjectName(VOLD_OWNER_UUID_TEST);
    }

    public final ObjectName newVvrManagerObjectName() {
        return VvrObjectNameFactory.newVvrManagerObjectName(VOLD_OWNER_UUID_TEST);
    }

    public final ObjectName newDtxManagerObjectName() {
        return VvrObjectNameFactory.newDtxManagerObjectName(VOLD_OWNER_UUID_TEST);
    }

    public final ObjectName newDtxLocalNodeObjectName() {
        return VvrObjectNameFactory.newDtxLocalNodeObjectName(VOLD_OWNER_UUID_TEST);
    }

    public final ObjectName newVvrObjectName(final UUID vvrUuid) {
        return VvrObjectNameFactory.newVvrObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);
    }

    public final ObjectName newSnapshotObjectName(final UUID vvrUuid, final UUID snapshotUuid) {
        return VvrObjectNameFactory.newSnapshotObjectName(VOLD_OWNER_UUID_TEST, vvrUuid, snapshotUuid);
    }

    public final ObjectName newDeviceObjectName(final UUID vvrUuid, final UUID deviceUuid) {
        return VvrObjectNameFactory.newDeviceObjectName(VOLD_OWNER_UUID_TEST, vvrUuid, deviceUuid);
    }

    /**
     * Wait the registration of a vvr.
     * 
     * @param server
     * @param vvrUuid
     * 
     * @return the MXbean for this vvr
     */
    public VvrMXBean waitVvrMXBeanRegistration(final MBeanServer server, final UUID vvrUuid) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;

        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);

        if (isDummyMBeanServer) {
            return (VvrMXBean) dummyMBeanServer.waitMXBean(vvrObjectName);
        }
        else {
            // may have to wait for the MXBean to be registered
            try {
                Assert.assertTrue(waitMXBeanRegistration(vvrObjectName));
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
            return JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false);
        }
    }

    /**
     * Returns when the vvr task is done or throws an exception on failure.
     * 
     * @param taskId
     * @return the user data of the task
     */
    public final String waitTaskEnd(final UUID vvrUuid, final String taskId, final MBeanServer server) {

        final VvrMXBean vvr = waitVvrMXBeanRegistration(server, vvrUuid);

        while (true) {
            final VvrTask task = vvr.getVvrTask(taskId);
            if (task == null) {
                throw new AssertionFailedError("taskId=" + taskId + " not found");
            }

            Assert.assertEquals(taskId, task.getTaskId());

            final DtxTaskStatus status = task.getStatus();
            if (status == DtxTaskStatus.COMMITTED) {
                return task.getInfo().getTargetId();
            }
            else if (status == DtxTaskStatus.ROLLED_BACK || status == DtxTaskStatus.UNKNOWN) {
                throw new IllegalStateException("Task status=" + status);
            }

            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
        }
    }

    /**
     * Returns when the vvr manager task is done or throws an exception on failure.
     * 
     * @param taskId
     * @return the target id of the task
     */
    public final String waitVvrManagerTaskEnd(final String taskId, final MBeanServer server) {

        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;

        final VvrManagerMXBean vvrManager;
        final ObjectName vvrManagerObjectName = newVvrManagerObjectName();
        if (isDummyMBeanServer) {
            vvrManager = (VvrManagerMXBean) dummyMBeanServer.waitMXBean(vvrManagerObjectName);
        }
        else {
            // may have to wait for the MXBean to be registered
            try {
                Assert.assertTrue(waitMXBeanRegistration(vvrManagerObjectName));
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }

            vvrManager = JMX.newMXBeanProxy(server, vvrManagerObjectName, VvrManagerMXBean.class, false);
        }

        while (true) {
            final VvrManagerTask task = vvrManager.getVvrManagerTask(taskId);
            if (task == null) {
                throw new AssertionFailedError("taskId=" + taskId + " not found");
            }

            Assert.assertEquals(taskId, task.getTaskId());

            final DtxTaskStatus status = task.getStatus();
            if (status == DtxTaskStatus.COMMITTED) {
                return task.getInfo().getTargetId();
            }
            else if (status == DtxTaskStatus.ROLLED_BACK || status == DtxTaskStatus.UNKNOWN) {
                throw new IllegalStateException("Task status=" + status);
            }

            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
        }
    }

    /**
     * Create a VVR. Based on the local MBeanServer.
     * 
     * @param name
     * @param description
     * @return proxy on the new VVR
     * @throws VvrManagementException
     */
    public final VvrMXBean createVvr(final String name, final String description) throws VvrManagementException {
        return createVvr(ManagementFactory.getPlatformMBeanServer(), name, description);
    }

    public final VvrMXBean createVvr(final MBeanServer server, final String name, final String description)
            throws VvrManagementException {

        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        final ObjectName vvrManagerObjectName = newVvrManagerObjectName();
        final VvrManagerMXBean vvrManager;
        final UUID vvrUuid;

        if (isDummyMBeanServer) {
            vvrManager = (VvrManagerMXBean) dummyMBeanServer.getMXBean(vvrManagerObjectName);
        }
        else {
            vvrManager = JMX.newMXBeanProxy(server, vvrManagerObjectName, VvrManagerMXBean.class, false);
        }

        vvrUuid = UUID.fromString(vvrManager.createVvr(name, description));
        return waitVvrMXBeanRegistration(server, vvrUuid);
    }

    /**
     * Deactivate all the devices of a VVR
     * 
     * @param server
     * @param vvrUuid
     * @param snapshotUuid
     */
    public void deactivateDevice(final MBeanServer server, final UUID vvrUuid, final String snapshotUuid) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        final SnapshotMXBean snapshot;
        final ObjectName snapshotObjectName = VvrObjectNameFactory.newSnapshotObjectName(VOLD_OWNER_UUID_TEST, vvrUuid,
                UUID.fromString(snapshotUuid));

        if (isDummyMBeanServer) {
            snapshot = (SnapshotMXBean) dummyMBeanServer.getMXBean(snapshotObjectName);
        }
        else {
            snapshot = JMX.newMXBeanProxy(server, snapshotObjectName, SnapshotMXBean.class, false);
        }
        Assert.assertNotNull(snapshot);
        final String[] devices = snapshot.getChildrenDevices();
        if (devices.length != 0) {
            for (final String deviceUuid : devices) {
                final DeviceMXBean device;
                final ObjectName deviceObjectName = VvrObjectNameFactory.newDeviceObjectName(VOLD_OWNER_UUID_TEST,
                        vvrUuid, UUID.fromString(deviceUuid));

                if (isDummyMBeanServer) {
                    device = (DeviceMXBean) dummyMBeanServer.getMXBean(deviceObjectName);
                }
                else {
                    device = JMX.newMXBeanProxy(server, deviceObjectName, DeviceMXBean.class, false);
                }
                if (device.isActive()) {
                    waitTaskEnd(vvrUuid, device.deActivate(), server);
                }
            }
        }

        final String[] snapshots = snapshot.getChildrenSnapshots();
        if (snapshots.length != 0) {
            for (final String uuid : snapshots) {
                deactivateDevice(server, vvrUuid, uuid);
            }
        }
    }

    /**
     * Start a vvr.
     * 
     * @param server
     * @param vvrUuid
     */
    public final void startVvr(final MBeanServer server, final UUID vvrUuid) {
        final VvrMXBean vvr = lookupVvr(server, vvrUuid);
        if (!vvr.isStarted()) {
            vvr.start();
        }
        Assert.assertTrue(vvr.isStarted());
    }

    /**
     * Stop a vvr.
     * 
     * @param server
     * @param vvrUuid
     */
    public final void stopVvr(final MBeanServer server, final UUID vvrUuid) {
        final VvrMXBean vvr = lookupVvr(server, vvrUuid);
        if (vvr.isStarted()) {
            vvr.stop();
        }
        Assert.assertFalse(vvr.isStarted());
    }

    /**
     * Delete a VVR. Based on the local MBeanServer.
     * 
     */
    public final void deleteVvr(final MBeanServer server, final UUID vvrUuid) throws VvrManagementException {

        final VvrMXBean vvr = lookupVvr(server, vvrUuid);
        if (vvr.isStarted()) {
            final String rootSnapshotUuid = VvrManagerTestUtils.getSnapshotRoot(server, this,
                    UUID.fromString(vvr.getUuid())).getUuid();
            deactivateDevice(server, vvrUuid, rootSnapshotUuid);
            vvr.stop();
        }

        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        final ObjectName vvrManagerObjectName = newVvrManagerObjectName();
        final VvrManagerMXBean vvrManager;
        if (isDummyMBeanServer) {
            vvrManager = (VvrManagerMXBean) dummyMBeanServer.getMXBean(vvrManagerObjectName);
        }
        else {
            vvrManager = JMX.newMXBeanProxy(server, vvrManagerObjectName, VvrManagerMXBean.class, false);
        }

        vvrManager.delete(vvrUuid.toString());

        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);
        if (isDummyMBeanServer) {
            Assert.assertTrue(dummyMBeanServer.waitMXBeanUnregistered(vvrObjectName));
        }
        else {
            // may have to wait for the MXBean to be registered
            try {
                Assert.assertTrue(waitMXBeanUnRegistration(vvrObjectName));
            }
            catch (final InterruptedException e) {
                throw new AssertionError("Interrupted", e);
            }
        }
    }

    public final VvrMXBean lookupVvr(final MBeanServer server, final UUID vvrUuid) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);

        if (isDummyMBeanServer) {
            return (VvrMXBean) dummyMBeanServer.getMXBean(vvrObjectName);
        }
        else {
            return JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false);
        }
    }

    public final VvrMXBean getVvr(final UUID vvrUuid) {
        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);
        return getVvr(ManagementFactory.getPlatformMBeanServer(), vvrObjectName);
    }

    private final VvrMXBean getVvr(final MBeanServer server, final ObjectName vvrObjectName) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        if (isDummyMBeanServer) {
            return (VvrMXBean) dummyMBeanServer.getMXBean(vvrObjectName);
        }
        else {
            return JMX.newMXBeanProxy(server, vvrObjectName, VvrMXBean.class, false);
        }
    }

    /**
     * Gets the device list of a VVR.
     * 
     * @param vvrUuid
     * @return the device list
     */
    public final Set<DeviceMXBean> getDevices(final UUID vvrUuid) {
        return getDevices(ManagementFactory.getPlatformMBeanServer(), vvrUuid);
    }

    public final Set<DeviceMXBean> getDevices(final MBeanServer server, final UUID vvrUuid) {
        final ObjectName query = VvrObjectNameFactory.newDeviceQueryListObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);
        final Set<ObjectName> deviceNames;

        if (server instanceof DummyMBeanServer) {
            deviceNames = ((DummyMBeanServer) server).getByType(vvrUuid.toString(), Constants.DEVICE_TYPE);
        }
        else {
            deviceNames = server.queryNames(query, null);
        }

        final Set<DeviceMXBean> result = new HashSet<>(deviceNames.size());
        for (final ObjectName deviceName : deviceNames) {
            result.add(getDevice(server, deviceName));
        }
        return result;
    }

    // Task UUID
    public final DeviceMXBean getDevice(final UUID vvrUuid, final String deviceTaskUuid) {
        return getDevice(ManagementFactory.getPlatformMBeanServer(), vvrUuid, deviceTaskUuid);
    }

    // Task UUID
    public final DeviceMXBean getDevice(final MBeanServer server, final UUID vvrUuid, final String deviceTaskUuid) {
        final String deviceUuid = waitTaskEnd(vvrUuid, deviceTaskUuid.toString(), server);
        return getDevice(server, vvrUuid, UUID.fromString(deviceUuid));
    }

    // Device UUID
    public final DeviceMXBean getDevice(final UUID vvrUuid, final UUID deviceUuid) {
        return getDevice(ManagementFactory.getPlatformMBeanServer(), vvrUuid, deviceUuid);
    }

    // Device UUID
    public final DeviceMXBean getDevice(final MBeanServer server, final UUID vvrUuid, final UUID deviceUuid) {
        final ObjectName deviceObjectName = VvrObjectNameFactory.newDeviceObjectName(VOLD_OWNER_UUID_TEST, vvrUuid,
                deviceUuid);
        return getDevice(server, deviceObjectName);
    }

    public final ObjectName getDeviceObjectName(final UUID vvrUuid, final UUID deviceUuid) {
        return VvrObjectNameFactory.newDeviceObjectName(VOLD_OWNER_UUID_TEST, vvrUuid, deviceUuid);
    }

    private final DeviceMXBean getDevice(final MBeanServer server, final ObjectName deviceObjectName) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        if (isDummyMBeanServer) {
            return (DeviceMXBean) dummyMBeanServer.getMXBean(deviceObjectName);
        }
        else {
            return JMX.newMXBeanProxy(server, deviceObjectName, DeviceMXBean.class, false);
        }
    }

    /**
     * Gets the snapshot list of a VVR.
     * 
     * @param vvrUuid
     * @return the snapshot list
     */
    public final Set<SnapshotMXBean> getSnapshots(final UUID vvrUuid) {
        return getSnapshots(ManagementFactory.getPlatformMBeanServer(), vvrUuid);
    }

    public final Set<SnapshotMXBean> getSnapshots(final MBeanServer server, final UUID vvrUuid) {
        final ObjectName query = VvrObjectNameFactory.newSnapshotQueryListObjectName(VOLD_OWNER_UUID_TEST, vvrUuid);
        final Set<ObjectName> snapshotNames;

        if (server instanceof DummyMBeanServer) {
            snapshotNames = ((DummyMBeanServer) server).getByType(vvrUuid.toString(), Constants.SNAPSHOT_TYPE);
        }
        else {
            snapshotNames = server.queryNames(query, null);
        }

        final Set<SnapshotMXBean> result = new HashSet<>(snapshotNames.size());
        for (final ObjectName snapshotName : snapshotNames) {
            result.add(getSnapshot(server, snapshotName));
        }
        return result;
    }

    // Task UUID
    public final SnapshotMXBean getSnapshot(final UUID vvrUuid, final String snapshotTaskUuid) {
        return getSnapshot(ManagementFactory.getPlatformMBeanServer(), vvrUuid, snapshotTaskUuid);
    }

    // Task UUID
    public final SnapshotMXBean getSnapshot(final MBeanServer server, final UUID vvrUuid, final String snapshotTaskUuid) {
        final String snapshotUuid = waitTaskEnd(vvrUuid, snapshotTaskUuid.toString(), server);
        return getSnapshot(server, vvrUuid, UUID.fromString(snapshotUuid));
    }

    // Snapshot UUID
    public final SnapshotMXBean getSnapshot(final UUID vvrUuid, final UUID snapshotUuid) {
        return getSnapshot(ManagementFactory.getPlatformMBeanServer(), vvrUuid, snapshotUuid);
    }

    // Snapshot UUID
    public final SnapshotMXBean getSnapshot(final MBeanServer server, final UUID vvrUuid, final UUID snapshotUuid) {
        final ObjectName snapshotObjectName = VvrObjectNameFactory.newSnapshotObjectName(VOLD_OWNER_UUID_TEST, vvrUuid,
                snapshotUuid);
        return getSnapshot(server, snapshotObjectName);
    }

    private final SnapshotMXBean getSnapshot(final MBeanServer server, final ObjectName snapshotObjectName) {
        final boolean isDummyMBeanServer = server instanceof DummyMBeanServer;
        final DummyMBeanServer dummyMBeanServer = isDummyMBeanServer ? (DummyMBeanServer) server : null;
        if (isDummyMBeanServer) {
            return (SnapshotMXBean) dummyMBeanServer.getMXBean(snapshotObjectName);
        }
        else {
            return JMX.newMXBeanProxy(server, snapshotObjectName, SnapshotMXBean.class, false);
        }
    }

    /**
     * Wait the registration of an MX Bean. A listener on the MX Bean must have been set before
     * (addMXBeanRegistrationListener).
     * 
     * @param ObjectName
     * @throws InterruptedException
     */
    public final boolean waitMXBeanRegistration(final ObjectName objectName) throws InterruptedException {

        CountDownLatch latch;
        latchLock.lock();
        try {
            latch = objectNameLatchs.get(objectName);
            if (latch == null) {
                latch = new CountDownLatch(1);
                objectNameLatchs.put(objectName, latch);
            }
        }
        finally {
            latchLock.unlock();
        }
        return latch.await(10, TimeUnit.SECONDS);
    }

    /**
     * Wait the unregistration of an MX Bean. A listener on the MX Bean must have been set before
     * (addMXBeanRegistrationListener).
     * 
     * @param ObjectName
     * @throws InterruptedException
     */
    public final boolean waitMXBeanUnRegistration(final ObjectName objectName) throws InterruptedException {
        CountDownLatch latch;
        final long end = System.currentTimeMillis() + WAIT_MXBEAN_TIMEOUT;
        do {
            latchLock.lock();
            try {
                latch = objectNameLatchs.get(objectName);
            }
            finally {
                latchLock.unlock();
            }
            try {
                Thread.sleep(WAIT_MXBEAN_TIMEOUT / 15);
            }
            catch (final InterruptedException e) {
                // Ignored
            }
        } while (latch != null && end > System.currentTimeMillis());
        return (latch == null);
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
        final MBeanServerNotification mbs = (MBeanServerNotification) notification;
        if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {
            CountDownLatch latch;
            latchLock.lock();
            try {
                latch = objectNameLatchs.get(mbs.getMBeanName());
                if (latch == null) {
                    latch = new CountDownLatch(1);
                    objectNameLatchs.put(mbs.getMBeanName(), latch);
                }
            }
            finally {
                latchLock.unlock();
            }
            latch.countDown();

        }
        else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) {
            CountDownLatch latch;
            latchLock.lock();
            try {
                latch = objectNameLatchs.get(mbs.getMBeanName());
                if (latch != null) {
                    objectNameLatchs.remove(mbs.getMBeanName());
                }
            }
            finally {
                latchLock.unlock();
            }
        }
    }

    /**
     * Add a listener on all the MX Bean registrations
     * 
     */
    private void addMXBeanRegistrationListener() throws InstanceNotFoundException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
        filter.enableAllObjectNames();
        server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, filter, null);
    }

    /**
     * Remove a listener on all the MX Bean registrations
     * 
     */
    private void removeMXBeanRegistrationListener() throws InstanceNotFoundException, ListenerNotFoundException {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
        filter.enableAllObjectNames();
        server.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
        latchLock.lock();
        try {
            objectNameLatchs.clear();
        }
        finally {
            latchLock.unlock();
        }
    }
}
