package io.eguan.webui.jmx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.vold.model.VvrManagerMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class manages the connection and the disconnection of JMX.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class JmxClient {

    private JMXConnector jmxConnector;

    private MBeanServerConnection connection;

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxClient.class);

    public final MBeanServerConnection getConnection() {
        return connection;
    }

    /**
     * Retrieves the owner {@link java.util.UUID} from the {@link VvrManagerMXBean} and connect the JMX client.
     * 
     * @return a non-empty {@link String} of the owner UUID
     * @throws IllegalStateException
     *             if querying the {@link VvrManagerMXBean} fails
     */
    public final UUID connect(final String serverUrl) throws IllegalStateException {
        final Set<ObjectName> foundManagers;
        try {
            if (serverUrl.isEmpty()) {
                LOGGER.debug("Local JMX Connection");
                connection = newLocalConnection();
            }
            else {
                LOGGER.debug("Remote JMX Connection serverUrl=" + serverUrl);
                connection = newRemoteConnection(serverUrl);
            }
            foundManagers = connection.queryNames(null,
                    Query.isInstanceOf(Query.value(VvrManagerMXBean.class.getCanonicalName())));
        }
        catch (SecurityException | IOException e) {
            throw new IllegalStateException(e);
        }

        if (foundManagers.isEmpty()) {
            throw new IllegalStateException("No VvrManager found; serverUrl=" + serverUrl);
        }
        final VvrManagerMXBean vvrMgrProxy = JMX.newMXBeanProxy(connection, foundManagers.iterator().next(),
                VvrManagerMXBean.class);
        return UUID.fromString(vvrMgrProxy.getOwnerUuid());
    }

    /**
     * Disconnect the JMX client
     * 
     * @throws IOException
     */
    public void disconnect() throws IOException {
        if (jmxConnector != null) {
            jmxConnector.close();
            jmxConnector = null;
        }
    }

    /**
     * Connect on the local JMX platform.
     * 
     * @return the new connection
     */
    private final MBeanServerConnection newLocalConnection() {
        return ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Connect on a remote JMX platform
     * 
     * @param serviceUrl
     *            the url of the JMX platform
     * 
     * @return the new connection
     * 
     * @throws MalformedURLException
     * @throws IOException
     * @throws SecurityException
     */
    private final MBeanServerConnection newRemoteConnection(final String serviceUrl) throws MalformedURLException,
            IOException, SecurityException {

        final JMXServiceURL jmxServerUrl = new JMXServiceURL(serviceUrl);
        jmxConnector = JMXConnectorFactory.connect(jmxServerUrl);
        return jmxConnector.getMBeanServerConnection();
    }
}
