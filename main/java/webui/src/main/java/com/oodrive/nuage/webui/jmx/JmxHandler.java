package com.oodrive.nuage.webui.jmx;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import javax.management.AttributeChangeNotification;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.vold.model.VvrObjectNameFactory;
import com.oodrive.nuage.webui.JmxServerUrlConfigKey;
import com.oodrive.nuage.webui.VvrManagerUi;
import com.oodrive.nuage.webui.WebUiConfigurationContext;
import com.oodrive.nuage.webui.model.DeviceModel;
import com.oodrive.nuage.webui.model.ModelCreator;
import com.oodrive.nuage.webui.model.SnapshotModel;
import com.oodrive.nuage.webui.model.VvrManagerModel;
import com.oodrive.nuage.webui.model.VvrModel;

/**
 * The class handles the JMX connection and create object model via JMX.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class JmxHandler implements NotificationListener, ModelCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxHandler.class);
    private JmxClient jmxClient;
    private final VvrManagerUi vvrUi;
    private UUID ownerUuid;
    private final InputStream configInput;

    /**
     * Context to load as default from internal resources.
     */
    private static final String DEFAULT_CONFIG_RESOURCE = "/webuiConfig.cfg";

    public JmxHandler(final VvrManagerUi vvrUi) {
        final Class<? extends JmxHandler> clazz = getClass();
        this.vvrUi = vvrUi;
        this.configInput = clazz.getResourceAsStream(DEFAULT_CONFIG_RESOURCE);
    }

    @Override
    public final DeviceModel createDeviceModel(final VvrModel vvr, final UUID deviceUuid) {
        return new JmxDeviceModel(jmxClient.getConnection(), ownerUuid, vvr, deviceUuid);
    }

    @Override
    public final SnapshotModel createSnapshotModel(final VvrModel vvr, final UUID snapshotUuid) {
        return new JmxSnapshotModel(jmxClient.getConnection(), ownerUuid, vvr, snapshotUuid);
    }

    @Override
    public final VvrModel createVvrModel(final UUID vvrUuid) {
        final ObjectName vvrObjectName = VvrObjectNameFactory.newVvrObjectName(ownerUuid, vvrUuid);
        try {
            jmxClient.getConnection().addNotificationListener(vvrObjectName, this, null, null);
        }
        catch (InstanceNotFoundException | IOException e) {
            LOGGER.error("Can not add a listener", e);
        }
        return new JmxVvrModel(jmxClient.getConnection(), ownerUuid, vvrUuid);
    }

    @Override
    public final VvrManagerModel createVvrManagerModel() {
        return new JmxVvrManagerModel(jmxClient.getConnection(), ownerUuid);
    }

    @Override
    public final void handleNotification(final Notification notification, final Object handback) {

        if (notification instanceof MBeanServerNotification) {
            final MBeanServerNotification mbs = (MBeanServerNotification) notification;
            final ObjectName objectname = mbs.getMBeanName();

            if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(mbs.getType())) {
                LOGGER.debug("MBean registered=" + objectname);

                vvrUi.access(new Runnable() {
                    @Override
                    public void run() {
                        if (isVvr(objectname)) {
                            vvrUi.addVvr(getVvrUuid(objectname));
                        }
                        else if (isSnapshot(objectname)) {
                            vvrUi.addSnapshot(getVvrUuid(objectname), getSnapshotUuid(objectname));
                        }
                        else if (isDevice(objectname)) {
                            vvrUi.addDevice(getVvrUuid(objectname), getDeviceUuid(objectname));
                        }

                    }
                });
            }
            else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(mbs.getType())) {
                LOGGER.debug("MBean unregistered=" + objectname);

                vvrUi.access(new Runnable() {
                    @Override
                    public void run() {

                        if (isVvr(objectname)) {
                            vvrUi.removeVvr(getVvrUuid(objectname));
                        }
                        else if (isSnapshot(objectname)) {
                            vvrUi.removeSnapshot(getVvrUuid(objectname), getSnapshotUuid(objectname));
                        }
                        else if (isDevice(objectname)) {
                            vvrUi.removeDevice(getVvrUuid(objectname), getDeviceUuid(objectname));
                        }
                    }
                });
            }
        }
        else if (notification instanceof AttributeChangeNotification) {
            LOGGER.debug("MBean changed=" + notification.getSource());

            final AttributeChangeNotification attrNotif = (AttributeChangeNotification) notification;
            final ObjectName objectname = (ObjectName) notification.getSource();
            vvrUi.access(new Runnable() {
                @Override
                public void run() {

                    if (isVvr(objectname)) {
                        if (isName(attrNotif.getAttributeName())) {
                            vvrUi.modifyVvrName(getVvrUuid(objectname), (String) attrNotif.getNewValue());
                        }
                    }
                    else if (isSnapshot(objectname)) {
                        if (isName(attrNotif.getAttributeName())) {
                            vvrUi.modifySnapshotName(getVvrUuid(objectname), getSnapshotUuid(objectname),
                                    (String) attrNotif.getNewValue());
                        }
                        else if (isDescription(attrNotif.getAttributeName())) {
                            vvrUi.modifySnapshotDescription(getVvrUuid(objectname), getSnapshotUuid(objectname),
                                    (String) attrNotif.getNewValue());
                        }
                    }
                    else if (isDevice(objectname)) {
                        if (isName(attrNotif.getAttributeName())) {
                            vvrUi.modifyDeviceName(getVvrUuid(objectname), getDeviceUuid(objectname),
                                    (String) attrNotif.getNewValue());
                        }
                        else if (isDescription(attrNotif.getAttributeName())) {
                            vvrUi.modifyDeviceDescription(getVvrUuid(objectname), getDeviceUuid(objectname),
                                    (String) attrNotif.getNewValue());
                        }
                    }
                }
            });

        }
    }

    /**
     * Disconnect to the MBean server.
     */
    public final void disconnect() {
        try {
            jmxClient.getConnection().removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
        }
        catch (final Exception e) {
            LOGGER.error("Can not disconnect", e);
        }
    }

    /**
     * Connect on the MBean server
     * 
     * @throws Exception
     */
    public final void connect() throws Exception {
        jmxClient = new JmxClient();
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(configInput,
                WebUiConfigurationContext.getInstance());
        final String serverUrl = JmxServerUrlConfigKey.getInstance().getTypedValue(configuration);

        ownerUuid = jmxClient.connect(serverUrl);

        LOGGER.debug("Connect on=" + ownerUuid);

        final MBeanServerNotificationFilter filter = new MBeanServerNotificationFilter();
        filter.enableAllObjectNames();
        jmxClient.getConnection().addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, filter, null);
    }

    /**
     * Gets UUID for vvr ObjectName.
     * 
     * @param vvrObjectName
     * 
     * @return the UUID
     */
    static final UUID getVvrUuid(final ObjectName vvrObjectName) {
        return UUID.fromString(vvrObjectName.getKeyProperty("vvr"));
    }

    /**
     * Gets UUID from snapshot ObjectName
     * 
     * @param snapshotObjectName
     * @return
     */
    static final UUID getSnapshotUuid(final ObjectName snapshotObjectName) {
        return UUID.fromString(snapshotObjectName.getKeyProperty("snapshot"));
    }

    /**
     * Gets UUID from device object name
     * 
     * @param deviceObjectName
     * @return
     */
    static final UUID getDeviceUuid(final ObjectName deviceObjectName) {
        return UUID.fromString(deviceObjectName.getKeyProperty("device"));
    }

    private final boolean isVvr(final ObjectName vvrObjectName) {
        return vvrObjectName.getKeyProperty("type").equals("Vvr");
    }

    private final boolean isSnapshot(final ObjectName snapshotObjectName) {
        return snapshotObjectName.getKeyProperty("type").equals("Snapshot");
    }

    private final boolean isDevice(final ObjectName deviceObjectName) {
        return deviceObjectName.getKeyProperty("type").equals("Device");
    }

    private final boolean isName(final String attributes) {
        return attributes.equals("Name");
    }

    private final boolean isDescription(final String attributes) {
        return attributes.equals("Description");
    }
}
