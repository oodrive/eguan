package io.eguan.vold.rest.resources;

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

import io.eguan.vold.jmx.client.JmxClientConnectionFactory;
import io.eguan.vold.model.VvrManagerMXBean;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;

import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

/**
 * Helper class for JMX-specific test utility methods.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class JmxTestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxTestHelper.class);

    /**
     * Gets the set of server URLs for all detected local MBean servers that have a registered MBeans of a given class
     * and/or a provided ObjectName.
     * 
     * @param objName
     *            the {@link ObjectName} to search for or <code>null</code>
     * @param className
     *            the target class' canonical name or <code>null</code>
     * @return a possibly empty {@link Set} of valid server URLs
     * @throws NullPointerException
     *             if the provided {@link ObjectName} is <code>null</code>
     */
    public static final Set<String> getLocalMBeanServerUrls(final ObjectName objName, final String className)
            throws NullPointerException {

        final HashSet<String> result = new HashSet<String>();

        final List<VirtualMachineDescriptor> javaVms = VirtualMachine.list();

        final String mgmtAgentPath = System.getProperty("java.home")
                + String.format("%slib%smanagement-agent.jar", File.separator, File.separator);

        for (final VirtualMachineDescriptor currDescriptor : javaVms) {

            VirtualMachine currVm;
            try {
                currVm = VirtualMachine.attach(currDescriptor.id());

                String connectorAddress = currVm.getAgentProperties().getProperty(
                        "com.sun.management.jmxremote.localConnectorAddress");

                if (connectorAddress == null) {
                    try {
                        currVm.loadAgent(mgmtAgentPath);
                        connectorAddress = currVm.getAgentProperties().getProperty(
                                "com.sun.management.jmxremote.localConnectorAddress");
                    }
                    catch (AgentLoadException | AgentInitializationException e) {
                        LOGGER.warn("Failed to load Management Agent on process " + currVm.id(), e);
                    }
                }

                LOGGER.info("Connector address for PID=" + currDescriptor.id() + ": " + connectorAddress);

                if (connectorAddress == null) {
                    continue;
                }

                final MBeanServerConnection conn = JmxClientConnectionFactory.newConnection(connectorAddress);

                final Set<ObjectInstance> response = conn.queryMBeans(objName, Strings.isNullOrEmpty(className) ? null
                        : Query.isInstanceOf(Query.value(className)));

                if (response.size() > 0) {
                    result.add(connectorAddress);
                }
            }
            catch (AttachNotSupportedException | IOException e) {
                LOGGER.error("Exception", e);
            }

        }
        return result;
    }

    private String serverUrl;

    /**
     * Returns the configured {@link javax.management.MBeanServer} URL.
     * 
     * @return the serverUrl, <code>null</code> before {@link #setUp()} was called or for embedded
     *         {@link javax.management.MBeanServer}s.
     */
    final String getServerUrl() {
        return serverUrl;
    }

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if setup fails
     */
    public final void setUp() throws InitializationError {
        // set up fixture

        // gets a server URL from local process inspection
        final Set<String> serverUrls = getLocalMBeanServerUrls(null, VvrManagerMXBean.class.getCanonicalName());
        if (!serverUrls.isEmpty()) {
            if (serverUrls.size() > 1) {
                throw new InitializationError("More than one target MBean servers found!");
            }
            serverUrl = serverUrls.iterator().next();
        }

    }

    /**
     * Tears down common fixture.
     */
    public final void tearDown() {
        // tear down fixture
    }

    /**
     * Retrieves the owner {@link java.util.UUID} from the {@link VvrManagerMXBean}.
     * 
     * @return a non-empty {@link String} of the owner UUID
     * @throws IllegalStateException
     *             if querying the {@link VvrManagerMXBean} fails
     */
    public final String resolveVvrOwnerUuid() throws IllegalStateException {

        if (serverUrl == null) {
            try {
                this.setUp();
            }
            catch (final InitializationError e) {
                throw new IllegalStateException(e);
            }
        }

        MBeanServerConnection connection;
        Set<ObjectName> foundManagers;

        try {
            connection = JmxClientConnectionFactory.newConnection(serverUrl);
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
        return vvrMgrProxy.getOwnerUuid();

    }
}
