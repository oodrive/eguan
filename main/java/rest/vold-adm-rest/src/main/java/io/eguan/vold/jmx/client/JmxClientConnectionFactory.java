package io.eguan.vold.jmx.client;

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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;

import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.base.Strings;

/**
 * Static factory for {@link MBeanServerConnection}s.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class JmxClientConnectionFactory {

    private JmxClientConnectionFactory() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Constructs a new {@link MBeanServerConnection} from the provided URL.
     * 
     * If the given URL is <code>null</code> or empty, a connection to the embedded {@link MBeanServer} is returned.
     * 
     * @param serviceUrl
     *            the JMX service URL, a <code>null</code> or empty {@link String} will return a connection to the
     *            embedded {@link MBeanServer}
     * @return a functional {@link MBeanServerConnection}
     * @throws MalformedURLException
     *             if the provided URL is not well-formed (see {@link JMXServiceURL#JMXServiceURL(String)})
     * @throws IOException
     *             if establishing the connection fails
     * @throws SecurityException
     *             if establishing the connection fails for security reasons
     */
    public static final MBeanServerConnection newConnection(final String serviceUrl) throws MalformedURLException,
            IOException, SecurityException {

        if (Strings.isNullOrEmpty(serviceUrl)) {
            return ManagementFactory.getPlatformMBeanServer();
        }

        final JMXServiceURL jmxServerUrl = new JMXServiceURL(serviceUrl);

        final JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServerUrl);

        return jmxConnector.getMBeanServerConnection();
    }
}
