package com.oodrive.nuage.utils;

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

import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Test;

public class TestJmx {

    public static interface TstMXBean {
        void start();

        void stop();

        boolean isStarted();

        int add(int x, int y);
    }

    static final class Tst implements TstMXBean {

        private boolean started = false;

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            started = false;
        }

        @Override
        public boolean isStarted() {
            return started;
        }

        @Override
        public int add(final int x, final int y) {
            return x + y;
        }

    }

    @Test(expected = MalformedObjectNameException.class)
    public void testMalformedName() throws MalformedObjectNameException {
        Jmx.findLocalMBean("toto", TstMXBean.class);
    }

    /**
     * Exception: UndeclaredThrowableException caused by javax.management.InstanceNotFoundException
     * 
     * @throws MalformedObjectNameException
     */
    @Test(expected = UndeclaredThrowableException.class)
    public void testProxyNotFound() throws MalformedObjectNameException {
        final TstMXBean tst = Jmx.findLocalMBean("toto:type=Tst", TstMXBean.class);
        tst.add(15, 65);
    }

    @Test
    public void testProxy() throws JMException {
        // Register MBean
        final String name = "toto:type=Tst";
        final ObjectName objectName = new ObjectName(name);
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final Tst tst = new Tst();
        server.registerMBean(tst, objectName);
        try {
            final TstMXBean tstProxy = Jmx.findLocalMBean(name, TstMXBean.class);
            Assert.assertNotSame(tst, tstProxy);
            Assert.assertEquals(80, tstProxy.add(15, 65));
            Assert.assertFalse(tst.isStarted());
            Assert.assertFalse(tstProxy.isStarted());
            tstProxy.start();
            Assert.assertTrue(tst.isStarted());
            Assert.assertTrue(tstProxy.isStarted());
            tstProxy.stop();
            Assert.assertFalse(tst.isStarted());
            Assert.assertFalse(tstProxy.isStarted());
        }
        finally {
            server.unregisterMBean(objectName);
        }
    }
}
