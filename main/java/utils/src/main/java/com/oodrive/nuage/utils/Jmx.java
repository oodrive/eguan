package com.oodrive.nuage.utils;

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

import java.lang.management.ManagementFactory;
import java.util.List;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotificationEmitter;
import javax.management.ObjectName;

/**
 * Utility class to access to JMX resources.
 * 
 * @author oodrive
 * @author llambert
 */
public final class Jmx {

    /**
     * No instance.
     */
    private Jmx() {
        throw new AssertionError("No instance");
    }

    /**
     * Gets a connection to the local MBean server.
     * 
     * @return {@link MBeanServerConnection} on the local server
     */
    public final static MBeanServerConnection getLocalMBeanServer() {
        final List<MBeanServer> servers = MBeanServerFactory.findMBeanServer(null);
        if (servers.size() == 0) {
            // Create the local server
            ManagementFactory.getPlatformMBeanServer();
            return getLocalMBeanServer();
        }
        assert 1 == servers.size();
        return servers.get(0);
    }

    /**
     * Creates a proxy for a local registered MBean. The returned proxy supports the methods of
     * {@link NotificationEmitter}.
     * 
     * @param mbeanName
     * @param classMBean
     * @return a proxy to the MBean
     * @throws MalformedObjectNameException
     *             if the mbeanName is not a valid {@link ObjectName}
     */
    public final static <T> T findLocalMBean(final String mbeanName, final Class<T> classMBean)
            throws MalformedObjectNameException {
        final MBeanServerConnection server = getLocalMBeanServer();
        return JMX.newMBeanProxy(server, new ObjectName(mbeanName), classMBean, true);
    }

}
