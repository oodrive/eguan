package io.eguan.vold;

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

import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.AbstractConfigurationContext;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.DtxTaskInfo;
import io.eguan.dtx.config.DtxConfigurationContext;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiServerConfigurationContext;
import io.eguan.iscsisrv.IscsiServerInetAddressConfigKey;
import io.eguan.iscsisrv.IscsiServerPortConfigKey;
import io.eguan.nbdsrv.NbdServer;
import io.eguan.nbdsrv.NbdServerConfigurationContext;
import io.eguan.nbdsrv.NbdServerInetAddressConfigKey;
import io.eguan.nbdsrv.NbdServerPortConfigKey;
import io.eguan.nbdsrv.NbdServerTrimConfigKey;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgNode;
import io.eguan.nrs.NrsConfigurationContext;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.LogUtils;
import io.eguan.utils.mapper.FileMapperConfigurationContext;
import io.eguan.vold.model.Constants;
import io.eguan.vold.model.VvrManager;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vvr.configuration.CommonConfigurationContext;
import io.eguan.vvr.configuration.IbsConfigurationContext;
import io.eguan.vvr.configuration.PersistenceConfigurationContext;
import io.eguan.vvr.remote.VvrRemoteUtils;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.management.AttributeChangeNotification;
import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;

/**
 * Entry point to launch the volume management daemon (VOLD).
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * @author ebredzinski
 * @author pwehrle
 */
public final class Vold implements VoldMXBean {
    private static final Logger LOGGER = Constants.LOGGER;

    /**
     * VOLD shutdown hook.
     * 
     * 
     */
    class ShutdownHook implements Runnable {

        @Override
        public final void run() {

            LOGGER.info("VOLD shutdown requested");
            try {
                Vold.this.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD stop", t);
            }
            try {
                Vold.this.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD shutdown", t);
            }
            LOGGER.info("VOLD shutdown completed");
        }
    }

    /** Invalid usage */
    private static final int EXIT_END = 0;
    /** Invalid usage */
    private static final int EXIT_USAGE = 1;
    /** Invalid VOLD config */
    private static final int EXIT_CONFIG_VOLD = 2;
    /** Invalid VVR config */
    private static final int EXIT_CONFIG_VVR = 3;
    /** Init failed */
    private static final int EXIT_INIT_FAILED = 4;
    /** Start failed */
    private static final int EXIT_START_FAILED = 5;

    /** Name of the VOLD configuration file */
    private static final String VOLD_CONFIG = "vold.cfg";
    /** Name of the VOLD previous configuration file */
    private static final String VOLD_CONFIG_PREV = "vold.cfg.bak";

    /** Name of the VVR template configuration file */
    private static final String VVR_TEMPLATE_CONFIG = "vvr.cfg";
    /** Directory of the VVR persistence */
    private static final String VVR_PERSISTENCE_DIR = "vvrs";

    /** Address set in stand-alone mode */
    private static final String STANDALONE_HOST_ADDR = "localhost";

    /** Directory containing the VOLD configuration file */
    private final File voldDir;

    /** Keep the list of peers. */
    private final VoldPeers voldPeers;

    /** Channel to lock the VOLD directory */
    private FileChannel voldDirChannel;

    /** Vold JMX object name */
    private ObjectName voldObjName;

    /** Vold status started */
    @GuardedBy(value = "startedLock")
    private boolean started;
    private final Lock startedLock = new ReentrantLock();

    /** File lock on the VOLD directory */
    private FileLock fileLock;

    /** Name of the file lock for the VOLD directory */
    private static final String VOLD_LOCK_NAME = ".lock";

    /** Vold configuration */
    @GuardedBy(value = "metaConfigurationLock")
    private MetaConfiguration metaConfiguration;
    private final Lock metaConfigurationLock = new ReentrantLock();

    /** JMX server */
    private MBeanServer mbeanServer;

    /** DTX manager */
    private DtxManager dtxManager;
    /** DTX manager JMX object name */
    private ObjectName dtxManagerObjName;
    /** DTX Local Node JMX object name */
    private ObjectName dtxLocalNodeObjName;

    /** VVR manager */
    private VvrManager vvrManager;

    /** Dtx resource management */
    private VoldDtxResourceManager voldDtxResourceManager;

    /** Remote mode server. Handle remote messages */
    private VoldSyncServer syncServer;
    /** Remote mode client. Send remote messages */
    private MsgClientStartpoint syncClient;

    /** Remote mode server JMX object name */
    private ObjectName syncClientObjName;
    /** Remote mode client JMX object name */
    private ObjectName syncServerObjName;

    /** iSCSI server */
    private IscsiServer iscsiServer;
    /** iSCSI server JMX object name */
    private ObjectName iscsiServerObjName;
    /** iSCSI notification listener to persist the configuration */
    private final NotificationListener iscsiServerNotificationListener = new NotificationListener() {
        @Override
        public final void handleNotification(final Notification notification, final Object handback) {
            final AttributeChangeNotification attributeChangeNotification = (AttributeChangeNotification) notification;
            final Object newValue = attributeChangeNotification.getNewValue();
            // Int, InetAddress or Boolean
            if (newValue instanceof Integer) {
                // Port changed
                LOGGER.info("iSCSI new port=" + newValue);

                // Update port in configuration (under configuration lock)
                try {
                    final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                    newKeyValueMap.put(IscsiServerPortConfigKey.getInstance(), newValue);
                    updateConfiguration(newKeyValueMap);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to save new iSCSI port", t);
                }
            }
            else if (newValue instanceof InetAddress) {
                // Address changed
                LOGGER.info("iSCSI new addr=" + newValue);

                // Update address in configuration
                try {
                    final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                    newKeyValueMap.put(IscsiServerInetAddressConfigKey.getInstance(), newValue);
                    updateConfiguration(newKeyValueMap);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to save new iSCSI address", t);
                }
            }
            else if (newValue instanceof Boolean) {
                // Started state changed
                LOGGER.info("iSCSI new started state=" + newValue);

                // Update started state in configuration
                try {
                    final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                    newKeyValueMap.put(EnableIscsiConfigKey.getInstance(), newValue);
                    updateConfiguration(newKeyValueMap);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to save new iSCSI started state", t);
                }
            }
            else {
                LOGGER.debug("Unexpected iSCSI server value type: " + newValue.getClass() + ", newValue=" + newValue);
            }
        }
    };

    /** NBD server */
    private NbdServer nbdServer;
    /** NBD server JMX object name */
    private ObjectName nbdServerObjName;
    /** NBD notification listener to persist the configuration */
    private final NotificationListener nbdServerNotificationListener = new NotificationListener() {
        @Override
        public final void handleNotification(final Notification notification, final Object handback) {
            final AttributeChangeNotification attributeChangeNotification = (AttributeChangeNotification) notification;
            final Object newValue = attributeChangeNotification.getNewValue();
            // Int, InetAddress or Boolean
            if (newValue instanceof Integer) {
                // Port changed
                LOGGER.info("NBD new port=" + newValue);

                // Update port in configuration (under configuration lock)
                try {
                    final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                    newKeyValueMap.put(NbdServerPortConfigKey.getInstance(), newValue);
                    updateConfiguration(newKeyValueMap);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to save new NBD port", t);
                }
            }
            else if (newValue instanceof InetAddress) {
                // Address changed
                LOGGER.info("NBD new addr=" + newValue);

                // Update address in configuration
                try {
                    final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                    newKeyValueMap.put(NbdServerInetAddressConfigKey.getInstance(), newValue);
                    updateConfiguration(newKeyValueMap);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Failed to save new NBD address", t);
                }
            }
            else if (newValue instanceof Boolean) {

                if (attributeChangeNotification.getAttributeName().equals("Started")) {
                    // Started state changed
                    LOGGER.info("NBD new started state=" + newValue);

                    // Update started state in configuration
                    try {
                        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                        newKeyValueMap.put(EnableNbdConfigKey.getInstance(), newValue);
                        updateConfiguration(newKeyValueMap);
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to save new NBD started state", t);
                    }
                }
                else if (attributeChangeNotification.getAttributeName().equals("Trim")) {
                    // Trim state changed
                    LOGGER.info("NBD new trim state=" + newValue);

                    // Update trim state in configuration
                    try {
                        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
                        newKeyValueMap.put(NbdServerTrimConfigKey.getInstance(), newValue);
                        updateConfiguration(newKeyValueMap);
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to save new NBD Trim state", t);
                    }
                }
                else {
                    LOGGER.debug("Unexpected NBD server value type: " + newValue.getClass() + ", newValue=" + newValue);
                }
            }
            else {
                LOGGER.debug("Unexpected NBD server value type: " + newValue.getClass() + ", newValue=" + newValue);
            }
        }
    };

    public Vold(final File voldDir) {
        super();
        this.voldDir = voldDir;
        this.voldPeers = new VoldPeers(this);
    }

    /**
     * Lock a Vold file to prevent from multiple starts.
     * <p>
     * Note: called from unit tests (reflection invocation).
     */
    private final void openAndLockVoldChannel() throws IOException {
        voldDirChannel = FileChannel.open(new File(voldDir, VOLD_LOCK_NAME).toPath(), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        try {
            assert voldDirChannel.isOpen();
            // if a new vold in the same JVM try to lock, it will throw OverlappingFileLockException
            this.fileLock = voldDirChannel.tryLock();
            // if a new vold not in the same JVM try to lock, fileLock will be returned null
            assert this.fileLock != null;
        }
        catch (final Exception e) {
            // close channel
            if (voldDirChannel.isOpen()) {
                voldDirChannel.close();
            }
            throw e;
        }
    }

    /**
     * Initialize the daemon.
     * 
     * @throws JMException
     * @throws IOException
     */
    public final void init(final boolean enableShutdownHook) throws IOException, JMException {

        // Open a .lock file and try to take a lock
        openAndLockVoldChannel();

        // Init shutdown hook
        if (enableShutdownHook) {
            final Thread hook = new Thread(new ShutdownHook(), "VOLD shutdown hook");
            Runtime.getRuntime().addShutdownHook(hook);
        }

        // Load vold configuration
        setLoadMetaConfiguration();

        // Initialize JMX
        initJmx();

        // Export Vold MXBean
        final UUID nodeUuid = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
        setVoldObjName(VvrObjectNameFactory.newVoldObjectName(nodeUuid));

        // Server ready
        LOGGER.info("VOLD server initialized");
    }

    /**
     * Shuts down the daemon.
     * 
     * @throws IOException
     */
    public final void fini() throws IOException {

        // Unregister Vold MXBean
        try {
            if (voldObjName != null) {
                mbeanServer.unregisterMBean(voldObjName);
                voldObjName = null;
            }
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while unregistering the DTX manager MXBean", t);
        }

        // Release JMX resources
        finiJmx();

        try {
            // releases file lock
            if (fileLock.isValid()) {
                fileLock.release();
            }
        }
        catch (final IOException e) {
            LOGGER.warn("Error releasing lock on vold directory" + e);
        }
        finally {
            if (voldDirChannel.isOpen()) {
                voldDirChannel.close();
            }
        }
    }

    /**
     * Sets the vold object name.
     * <p>
     * Note: called from unit tests (reflection invocation).
     */
    private final void setVoldObjName(final ObjectName voldObjName) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException {
        this.voldObjName = voldObjName;
        mbeanServer.registerMBean(this, voldObjName);
    }

    /**
     * Sets the current meta configuration.
     * <p>
     * Note: called from unit tests (reflection invocation).
     */
    private final void setLoadMetaConfiguration() {
        metaConfiguration = loadConfiguration(voldDir, VOLD_CONFIG, EXIT_CONFIG_VOLD,
                VoldConfigurationContext.getInstance(), IscsiServerConfigurationContext.getInstance(),
                NbdServerConfigurationContext.getInstance(), DtxConfigurationContext.getInstance());
    }

    /**
     * Start the daemon.
     * 
     * @throws JMException
     * @throws IOException
     */
    @Override
    public final void start() throws IOException, JMException {

        startedLock.lock();
        try {
            if (!started) {
                // Re-load vold configuration
                setLoadMetaConfiguration();

                // Initialize the iSCSI server
                initIscsiServer();

                // Initialize the NBD server
                initNbdServer();

                // Initialize the VVR manager
                initVvrManager();

                started = true;

            }
            else {
                throw new IllegalStateException("Vold already started");
            }
        }
        finally {
            startedLock.unlock();
        }
        // Server ready
        LOGGER.info("VOLD server started");
    }

    /**
     * Release resources before daemon end.
     * 
     * @throws IOException
     */
    @Override
    public final void stop() throws IOException {

        startedLock.lock();
        try {
            // Stop the VVR manager
            finiVvrManager();

            // Stop the NBD server
            finiNbdServer();

            // Stop the iSCSI server
            finiIscsiServer();

            started = false;
        }
        finally {
            startedLock.unlock();
        }
    }

    @Override
    public final String getPath() {
        return voldDir.getAbsolutePath();
    }

    @Override
    public final Map<String, String> getVoldConfiguration() {
        final Map<String, String> result = new HashMap<>();
        if (metaConfiguration != null) {
            final Properties properties = metaConfiguration.getCompleteConfigurationAsProperties();
            for (final String key : properties.stringPropertyNames()) {
                result.put(key, properties.getProperty(key));
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.vold.VoldMXBean#addPeer(java.lang.String, java.lang.String, int)
     */
    @Override
    public final void addPeer(final String uuid, final String address, final int port) throws JMException {
        voldPeers.addPeer(uuid, address, port, dtxManager);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.vold.VoldMXBean#addPeerNoWait(java.lang.String, java.lang.String, int)
     */
    @Override
    public final String addPeerNoWait(final String uuid, final String address, final int port) throws JMException {
        return voldPeers.addPeerNoWait(uuid, address, port, dtxManager);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.vold.VoldMXBean#removePeer(java.lang.String)
     */
    @Override
    public final void removePeer(final String uuid) throws JMException {
        voldPeers.removePeer(uuid, dtxManager);
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.eguan.vold.VoldMXBean#removePeerNoWait(java.lang.String)
     */
    @Override
    public final String removePeerNoWait(final String uuid) throws JMException {
        return voldPeers.removePeerNoWait(uuid, dtxManager);
    }

    /**
     * Register a peer node in NET and DTX.
     * 
     * @param uuid
     * @param address
     */
    final void registerPeer(final UUID uuid, final InetSocketAddress address) {
        if (syncClient == null) {
            // Recreate remote based on new peers list to add first node
            finiRemote();
            initRemote();
            vvrManager.setSyncClient(syncClient);
        }
        else {
            // register NET new node
            final MsgClientStartpoint syncClientTmp = syncClient;
            if (syncClientTmp == null) {
                // need to
                throw new IllegalStateException("syncClient must not be null!");
            }
            syncClientTmp.addPeer(new MsgNode(uuid, address));
        }

        // register DTX new node
        final DtxManager dtxManagerTmp = dtxManager;
        if (dtxManagerTmp == null) {
            throw new IllegalStateException("dtxManager must not be null!");
        }
        dtxManagerTmp.registerPeer(new DtxNode(uuid, address));
    }

    /**
     * Unregister a peer node from NET and DTX.
     * 
     * @param uuid
     * @param address
     */
    final void unregisterPeer(final UUID uuid, final InetSocketAddress address) {
        // unregister NET and DTX new node
        if (syncClient != null) {
            final MsgClientStartpoint syncClientTmp = syncClient;
            if (syncClientTmp == null) {
                throw new IllegalStateException("syncClient must not be null!");
            }
            syncClientTmp.removePeer(new MsgNode(uuid, address));
        }

        if (dtxManager != null) {
            final DtxManager dtxManagerTmp = dtxManager;
            if (dtxManagerTmp == null) {
                throw new IllegalStateException("dtxManager must not be null!");
            }
            dtxManagerTmp.unregisterPeer(new DtxNode(uuid, address));
        }
    }

    /**
     * Load a configuration file.
     * 
     * @return the loaded configuration
     */
    private static final MetaConfiguration loadConfiguration(final File voldDir, final String configFile,
            final int exitCode, final AbstractConfigurationContext... contexts) {
        final File config = new File(voldDir, configFile);
        if (!config.isFile()) {
            LOGGER.error("vold configuration file not found (" + config.getAbsolutePath() + ")");
            System.exit(exitCode);
        }
        try {
            return MetaConfiguration.newConfiguration(config, contexts);
        }
        catch (final ConfigValidationException e) {
            LOGGER.error("Invalid configuration file (" + config.getAbsolutePath() + "):");
            final List<ValidationError> errors = e.getValidationReport();
            for (final ValidationError error : errors) {
                LOGGER.error("\t" + ValidationError.getFormattedErrorReport(error));
            }
        }
        catch (NullPointerException | IllegalArgumentException e) {
            LOGGER.error("Invalid configuration file (" + config.getAbsolutePath() + ")", e);
        }
        catch (final IOException e) {
            LOGGER.error("Failed to read configuration file (" + config.getAbsolutePath() + ")", e);
        }
        System.exit(exitCode);
        // Make compiler happy: will never get here
        return null;
    }

    /**
     * Write the current configuration on disk. Keep the previous configuration: restore it if the write of the
     * configuration fails.
     * 
     * @throws IOException
     *             if the configuration write failed
     * @throws ConfigValidationException
     *             Should not occur
     */
    final void updateConfiguration(final Map<AbstractConfigKey, Object> newKeyValueMap) throws IOException,
            ConfigValidationException {

        metaConfigurationLock.lock();
        try {
            metaConfiguration = metaConfiguration.copyAndAlterConfiguration(newKeyValueMap);
            final File config = new File(voldDir, VOLD_CONFIG);
            final File prevConfig = new File(voldDir, VOLD_CONFIG_PREV);
            metaConfiguration.storeConfiguration(config, prevConfig, true);
        }
        finally {
            metaConfigurationLock.unlock();
        }
    }

    /**
     * Return peers list.
     * 
     * @return Peers list (not null, may be empty)
     */
    final List<VoldLocation> getPeersList() {
        metaConfigurationLock.lock();
        try {
            final ArrayList<VoldLocation> peers = PeersConfigKey.getInstance().getTypedValue(metaConfiguration);
            if (peers == null) {
                // does not return null
                return Collections.emptyList();
            }
            return peers;
        }
        finally {
            metaConfigurationLock.unlock();
        }
    }

    /**
     * Return local node.
     * 
     * @return local vold location.
     */
    final VoldLocation getVoldLocation() {
        metaConfigurationLock.lock();
        try {
            final UUID node = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
            final InetAddress ip = ServerEndpointInetAddressConfigKey.getInstance().getTypedValue(metaConfiguration);
            final int port = ServerEndpointPortConfigKey.getInstance().getTypedValue(metaConfiguration).intValue();
            return new VoldLocation(node, new InetSocketAddress(ip, port));
        }
        finally {
            metaConfigurationLock.unlock();
        }
    }

    /**
     * Return owner UUID
     * 
     * @return owner UUID
     */
    final UUID getOwnerUuid() {
        metaConfigurationLock.lock();
        try {
            return OwnerConfigKey.getInstance().getTypedValue(metaConfiguration);
        }
        finally {
            metaConfigurationLock.unlock();
        }
    }

    /**
     * Return node UUID
     * 
     * @return node UUID
     */
    final UUID getNodeUuid() {
        metaConfigurationLock.lock();
        try {
            return NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
        }
        finally {
            metaConfigurationLock.unlock();
        }
    }

    /**
     * Sets the current JMX server.
     * <p>
     * Note: called from unit tests (reflection invocation).
     */
    private final void setMBeanServer(final MBeanServer server) {
        mbeanServer = server;
    }

    /**
     * Initialize JMX.
     */
    private final void initJmx() {
        // Start the local JMX server
        setMBeanServer(ManagementFactory.getPlatformMBeanServer());
    }

    /**
     * Release JMX resources.
     */
    private final void finiJmx() {
        mbeanServer = null;
    }

    /**
     * Initialize DTX.
     */
    private final void initDtx() {
        // Create the config of dtx manager
        final UUID nodeUuid = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
        final List<DtxNode> dtxNodes;
        final DtxNode localDtxNode;

        // Stand alone or distributed?
        final ArrayList<VoldLocation> peers = PeersConfigKey.getInstance().getTypedValue(metaConfiguration);
        if (peers == null) {
            // Empty list of peers
            dtxNodes = Collections.emptyList();

            // Local node: stand alone and dynamic port (no incoming connection)
            final InetSocketAddress localPeer = new InetSocketAddress(STANDALONE_HOST_ADDR, 0);
            localDtxNode = new DtxNode(nodeUuid, localPeer);
        }
        else {

            // List of peers
            dtxNodes = new ArrayList<>(peers.size());
            for (final VoldLocation voldLocation : peers) {
                // TODO clean the port number creation, temporary : same port + 1
                final InetSocketAddress peerSockAddr = voldLocation.getSockAddr();
                final InetSocketAddress sockAddr = new InetSocketAddress(peerSockAddr.getAddress(),
                        peerSockAddr.getPort() + 1);
                final DtxNode dtxNode = new DtxNode(voldLocation.getNode(), sockAddr);
                dtxNodes.add(dtxNode);
            }

            // Local dtx node
            final InetAddress localAddress = ServerEndpointInetAddressConfigKey.getInstance().getTypedValue(
                    metaConfiguration);
            // TODO clean the port number creation, temporary : same port + 1
            final int localPort = ServerEndpointPortConfigKey.getInstance().getTypedValue(metaConfiguration).intValue() + 1;
            final InetSocketAddress localPeer = new InetSocketAddress(localAddress, localPort);
            localDtxNode = new DtxNode(nodeUuid, localPeer);
        }

        // cluster name and password
        final String clusterName = OwnerConfigKey.getInstance().getTypedValue(metaConfiguration).toString();
        final String clusterPassword = clusterName; // TODO temporary password

        final DtxManagerConfig dtxManagerConfig = new DtxManagerConfig(metaConfiguration, voldDir.toPath(),
                clusterName, clusterPassword, localDtxNode, dtxNodes.toArray(new DtxNode[dtxNodes.size()]));

        dtxManager = new DtxManager(dtxManagerConfig);
        dtxManager.init();
        dtxManager.start();
    }

    /**
     * Release DTX resources.
     */
    private final void finiDtx() {
        if (dtxManager != null) {

            try {
                dtxManager.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while stopping the dtx manager", t);
            }

            try {
                dtxManager.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while releasing the dtx manager", t);
            }

            dtxManager = null;
        }
    }

    /**
     * Initialize remote mode (client and server). The VVR manager must be set.
     * 
     * @throws MalformedObjectNameException
     * @throws NotCompliantMBeanException
     * @throws MBeanRegistrationException
     * @throws InstanceAlreadyExistsException
     */
    private final void initRemote() {
        final ArrayList<VoldLocation> peers = PeersConfigKey.getInstance().getTypedValue(metaConfiguration);
        if (peers != null) {
            boolean done = false;
            final UUID nodeUuid = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
            final InetAddress serverEndpoint = ServerEndpointInetAddressConfigKey.getInstance().getTypedValue(
                    metaConfiguration);
            final int serverPort = ServerEndpointPortConfigKey.getInstance().getTypedValue(metaConfiguration)
                    .intValue();
            syncServer = new VoldSyncServer(nodeUuid, vvrManager, serverEndpoint, serverPort);

            try {
                final List<MsgNode> peerNodes = new ArrayList<>(peers.size());
                for (final VoldLocation voldLocation : peers) {
                    peerNodes.add(new MsgNode(voldLocation.getNode(), voldLocation.getSockAddr()));
                }
                syncClient = new MsgClientStartpoint(nodeUuid, peerNodes);

                try {
                    syncServer.start();
                    syncClient.start();

                    // Register MXBeans
                    try {
                        syncServerObjName = syncServer.registerMXBean(mbeanServer);
                    }
                    catch (final Exception e) {
                        syncServerObjName = null;
                        LOGGER.warn("Error while registering the server MXBean", e);
                    }
                    try {
                        syncClientObjName = syncClient.registerMXBean(mbeanServer);
                    }
                    catch (final Exception e) {
                        syncClientObjName = null;
                        LOGGER.warn("Error while registering the client MXBean", e);
                    }
                    done = true;
                }
                finally {
                    if (!done) {
                        // Can stop even if the client is not started
                        syncClient.stop();
                        syncClient = null;
                        syncClientObjName = null;
                    }
                }
            }
            finally {
                if (!done) {
                    // Can stop even if the server is not started
                    syncServer.stop();
                    syncServer = null;
                    syncServerObjName = null;
                }
            }
        }
        else {
            syncServer = null;
            syncClient = null;
            syncServerObjName = null;
            syncClientObjName = null;
            LOGGER.info("VOLD remote mode disabled");
        }
    }

    /**
     * Release remote resources (client and server).
     */
    private final void finiRemote() {
        // Stop server (if any)
        try {
            try {
                if (syncServerObjName != null) {
                    mbeanServer.unregisterMBean(syncServerObjName);
                    syncServerObjName = null;
                }
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the server MXBean", t);
            }
            if (syncServer != null) {
                syncServer.stop();
                syncServer = null;
            }
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while stopping the remote server", t);
        }
        // Stop client (if any)
        try {
            try {
                if (syncClientObjName != null) {
                    mbeanServer.unregisterMBean(syncClientObjName);
                    syncClientObjName = null;
                }
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the client MXBean", t);
            }
            if (syncClient != null) {
                syncClient.stop();
                syncClient = null;
            }
        }
        catch (final Throwable t) {
            // Ignored
            LOGGER.warn("Error while stopping the remote client", t);
        }
    }

    /**
     * Initialize the configuration of the iSCSI server.
     * 
     * @throws JMException
     *             if JMX initialization failed
     */
    private final void initIscsiServer() throws JMException {
        assert iscsiServer == null;

        // Create server
        iscsiServer = new IscsiServer(metaConfiguration);
        final boolean enableIscsiServer = EnableIscsiConfigKey.getInstance().getTypedValue(metaConfiguration)
                .booleanValue();
        if (enableIscsiServer) {
            iscsiServer.start();
        }

        // Get JMX notifications to persist configuration changes
        iscsiServer.addNotificationListener(iscsiServerNotificationListener, null, null);

        // Export MXBean
        final String iscsiServerObjNameStr = iscsiServer.getClass().getPackage().getName() + ":type=Server";
        iscsiServerObjName = new ObjectName(iscsiServerObjNameStr);
        mbeanServer.registerMBean(iscsiServer, iscsiServerObjName);
    }

    /**
     * Release iSCSI resources.
     */
    private final void finiIscsiServer() {
        if (iscsiServer != null) {
            // Unexport MXBean
            try {
                mbeanServer.unregisterMBean(iscsiServerObjName);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the iSCSI server MXBean", t);
            }

            // Unset JMX notifications listener
            try {
                iscsiServer.removeNotificationListener(iscsiServerNotificationListener);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while removing the listener of the iSCSI server MXBean", t);
            }

            // Stop the server
            // Does nothing if the server is not started
            // Done after the removal of the JMX notification (must no be saved)
            try {
                iscsiServer.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while stopping the iSCSI server", t);
            }
            iscsiServer = null;
            iscsiServerObjName = null;
        }
    }

    /**
     * Initialize the configuration of the NBD server.
     * 
     * @throws JMException
     *             if JMX initialization failed
     */
    private final void initNbdServer() throws JMException {
        assert nbdServer == null;

        // Create server
        nbdServer = new NbdServer(metaConfiguration);
        final boolean enableNbdServer = EnableNbdConfigKey.getInstance().getTypedValue(metaConfiguration)
                .booleanValue();
        if (enableNbdServer) {
            nbdServer.start();
        }

        // Get JMX notifications to persist configuration changes
        nbdServer.addNotificationListener(nbdServerNotificationListener, null, null);

        // Export MXBean
        final String nbdServerObjNameStr = nbdServer.getClass().getPackage().getName() + ":type=Server";
        nbdServerObjName = new ObjectName(nbdServerObjNameStr);
        mbeanServer.registerMBean(nbdServer, nbdServerObjName);
    }

    /**
     * Release NBD resources.
     */
    private final void finiNbdServer() {
        if (nbdServer != null) {
            // Unexport MXBean
            try {
                mbeanServer.unregisterMBean(nbdServerObjName);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the NBD server MXBean", t);
            }

            // Unset JMX notifications listener
            try {
                nbdServer.removeNotificationListener(nbdServerNotificationListener);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while removing the listener of the NBD server MXBean", t);
            }

            // Stop the server
            // Does nothing if the server is not started
            // Done after the removal of the JMX notification (must no be saved)
            try {
                nbdServer.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error while stopping the NBD server", t);
            }
            nbdServer = null;
            nbdServerObjName = null;
        }
    }

    /**
     * Initialize VVR manager.
     * 
     * @throws JMException
     * @throws IOException
     */
    private final void initVvrManager() throws JMException, IOException {
        // Load Vvr template
        final UUID ownerUuid = OwnerConfigKey.getInstance().getTypedValue(metaConfiguration);
        final UUID nodeUuid = NodeConfigKey.getInstance().getTypedValue(metaConfiguration);
        final MetaConfiguration vvrTemplate = loadConfiguration(voldDir, VVR_TEMPLATE_CONFIG, EXIT_CONFIG_VVR,
                CommonConfigurationContext.getInstance(), FileMapperConfigurationContext.getInstance(),
                IbsConfigurationContext.getInstance(), NrsConfigurationContext.getInstance(),
                PersistenceConfigurationContext.getInstance());
        vvrManager = new VvrManager(mbeanServer, ownerUuid, nodeUuid, new File(voldDir, VVR_PERSISTENCE_DIR),
                vvrTemplate, iscsiServer, nbdServer);

        // Initialize dtx and remote management before activating VVR manager
        initDtx();

        // Export DtxManager MXBean
        dtxManagerObjName = VvrObjectNameFactory.newDtxManagerObjectName(ownerUuid);
        mbeanServer.registerMBean(dtxManager, dtxManagerObjName);

        // Export DtxLocalNode MXBean
        dtxLocalNodeObjName = VvrObjectNameFactory.newDtxLocalNodeObjectName(ownerUuid);
        mbeanServer.registerMBean(dtxManager.new DtxLocalNode(), dtxLocalNodeObjName);

        initRemote();
        vvrManager.setSyncClient(syncClient);

        // Register resource manager in dtx
        vvrManager.init(dtxManager);
        voldDtxResourceManager = new VoldDtxResourceManager(ownerUuid, this, vvrManager);
        dtxManager.registerResourceManager(voldDtxResourceManager);
    }

    /**
     * Release VVR manager resources.
     */
    private final void finiVvrManager() {
        if (vvrManager != null) {
            try {
                if (dtxManagerObjName != null) {
                    mbeanServer.unregisterMBean(dtxManagerObjName);
                    dtxManagerObjName = null;
                }
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the DTX manager MXBean", t);
            }
            try {
                if (dtxLocalNodeObjName != null) {
                    mbeanServer.unregisterMBean(dtxLocalNodeObjName);
                    dtxLocalNodeObjName = null;
                }
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while unregistering the DTX Local node MXBean", t);
            }

            // Release resources
            try {
                vvrManager.fini();
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while uninitializing the VVR manager", t);
            }

            // End of dispatch/sending of remote messages
            try {
                finiRemote();
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while stopping remote management", t);
            }
            try {
                vvrManager.setSyncClient(null);
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while stopping remote management", t);
            }
            try {
                finiDtx();
            }
            catch (final Throwable t) {
                // Ignored
                LOGGER.warn("Error while releasing dtx manager", t);
            }

            vvrManager = null;
        }
    }

    final Boolean prepare(final VoldDtxRmContext dtxContext) throws XAException {
        return voldPeers.prepare(dtxContext);
    }

    final void commit(final VoldDtxRmContext dtxContext) throws XAException {
        voldPeers.commit(dtxContext);
    }

    final void rollback(final VoldDtxRmContext dtxContext) throws XAException {
        voldPeers.rollback(dtxContext);
    }

    final void processPostSync() {
        // Nothing to do
    }

    /**
     * Constructs information for Vold task.
     * 
     * @param resourceId
     *            The globally unique ID of the resourceId
     * @param operation
     *            The complete operation used to construct the task info
     */
    final DtxTaskInfo createTaskInfo(final RemoteOperation operation) {
        VoldTaskOperation op;
        VoldTargetType targetType;

        final String source = VvrRemoteUtils.fromUuid(operation.getSource()).toString();
        final String targetId = VvrRemoteUtils.fromUuid(operation.getUuid()).toString();

        switch (operation.getType()) {
        case VOLD:
            targetType = VoldTargetType.VOLD;
            break;
        default:
            throw new AssertionError("type=" + operation.getType());
        }

        switch (operation.getOp()) {
        case SET:
            switch (operation.getPeer().getAction()) {
            case ADD:
                op = VoldTaskOperation.ADD_PEER;
                break;
            case REM:
                op = VoldTaskOperation.REMOVE_PEER;
                break;
            default:
                throw new AssertionError("action=" + operation.getPeer().getAction());
            }
            break;
        default:
            throw new AssertionError("type=" + operation.getOp());
        }
        return new VoldTaskInfo(source, op, targetType, targetId);
    }

    /**
     * Simply print usage on standard output.
     */
    private static void displayUsage() {
        System.out.println("vold usage:");
        System.out.println("     java -jar vold.jar <vold directory>");
        System.exit(EXIT_USAGE);
    }

    /**
     * Launch the VOLD.
     * 
     * @param args
     *            only one argument: the directory containing the configuration and the persistence of the VOLD.
     */
    public static final void main(final String[] args) {
        // Get uncaught exceptions
        Thread.setDefaultUncaughtExceptionHandler(new VoldUncaughtExceptionHandler());

        // Configure SysLog appender for logback
        LogUtils.initSysLog();

        if (args.length != 1) {
            displayUsage();
        }
        final File voldDir = new File(args[0]);
        if (!voldDir.isDirectory()) {
            displayUsage();
        }
        final Vold vold = new Vold(voldDir);
        try {
            vold.init(true);

            try {
                vold.start();
            }
            catch (final Throwable t) {
                LOGGER.error("Failed to start vold", t);
                System.exit(EXIT_START_FAILED);
            }
        }
        catch (final Throwable t) {
            LOGGER.error("Failed to initialize vold", t);
            System.exit(EXIT_INIT_FAILED);
        }

        // Keep the server running
        try {
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (final InterruptedException e) {
            LOGGER.error("Interrupted", e);
            System.exit(EXIT_END);
        }
    }

}
