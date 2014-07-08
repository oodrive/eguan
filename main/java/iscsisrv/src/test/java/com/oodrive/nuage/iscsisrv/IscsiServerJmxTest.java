package com.oodrive.nuage.iscsisrv;

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
import java.net.InetAddress;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;

import com.oodrive.nuage.utils.Jmx;

/**
 * Tests access to the server from the JMX API.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class IscsiServerJmxTest extends IscsiServerAbstractTest {

    private String serverMBeanObjName;

    /**
     * Initialize iSCSI server MBean.
     * 
     * @throws Exception
     */
    @Before
    public void initServerMBean() throws Exception {
        serverOrig = new IscsiServer(InetAddress.getLoopbackAddress());
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        serverMBeanObjName = serverOrig.getClass().getPackage().getName() + ":type=Server";
        final ObjectName mbeanName = new ObjectName(serverMBeanObjName);
        mbs.registerMBean(serverOrig, mbeanName);

        server = Jmx.findLocalMBean(serverMBeanObjName, IscsiServerMXBean.class);
    }

    /**
     * Unregister server. The server should be stopped.
     * 
     * @throws Exception
     */
    @After
    public void finiServerMBean() throws Exception {
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final ObjectName mbeanName = new ObjectName(serverMBeanObjName);
            mbs.unregisterMBean(mbeanName);
        }
        finally {
            serverMBeanObjName = null;
        }
    }

    @Override
    protected final IscsiTargetAttributes[] getServerTargetAttributes() {
        // TODO build IscsiTargetAttributes[] from composite data returned by the proxy?
        return ((IscsiServer) serverOrig).getTargets();
    }

}
