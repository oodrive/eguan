package com.oodrive.nuage.vold.model;

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
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.AttributeChangeNotification;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public abstract class AbstractMXBeanListener extends AbstractVoldTest {

    protected AbstractMXBeanListener() throws Exception {
        super();
    }

    /**
     * Dummy listener. Does nothing.
     */
    private static final NotificationListener DUMMY_LISTENER = new NotificationListener() {

        @Override
        public final void handleNotification(final Notification notification, final Object handback) {
            // No op
        }
    };

    protected MBeanServer server;
    protected ObjectName mbeanName;
    protected ObjectName vvrObjectName;

    public abstract void setName(final String name);

    public abstract void setDescription(final String description);

    @Before
    public void initVvr() throws Exception {
        vvrObjectName = helper.newVvrObjectName(vvrUuid);
        server = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Listener that expects some values.
     * 
     * @author
     */
    class NotificationListenerExpectation implements NotificationListener {
        private final Object handback;
        private final Object oldValue;
        private final Object newValue;
        final AtomicBoolean notified = new AtomicBoolean(false);

        NotificationListenerExpectation(final Object handback, final Object oldValue, final Object newValue) {
            super();
            this.handback = handback;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        @Override
        public final void handleNotification(final Notification notification, final Object handback) {
            Assert.assertEquals(mbeanName, notification.getSource());
            Assert.assertEquals(this.handback, handback);
            Assert.assertTrue(notification instanceof AttributeChangeNotification);
            final AttributeChangeNotification attributeChangeNotification = (AttributeChangeNotification) notification;
            Assert.assertEquals(this.oldValue, attributeChangeNotification.getOldValue());
            Assert.assertEquals(this.newValue, attributeChangeNotification.getNewValue());
            notified.set(true);
        }
    }

    @Test(expected = ListenerNotFoundException.class)
    public void removeListener1() throws ListenerNotFoundException, InstanceNotFoundException {
        server.removeNotificationListener(vvrObjectName, DUMMY_LISTENER);
    }

    @Test(expected = ListenerNotFoundException.class)
    public void removeListener3() throws ListenerNotFoundException, InstanceNotFoundException {
        server.removeNotificationListener(vvrObjectName, DUMMY_LISTENER, null, new Object());
    }

    @Test
    public void removeAddedListener1() throws ListenerNotFoundException, InstanceNotFoundException {
        server.addNotificationListener(vvrObjectName, DUMMY_LISTENER, null, new Object());
        server.removeNotificationListener(vvrObjectName, DUMMY_LISTENER);
    }

    @Test
    public void removeAddedListener3() throws ListenerNotFoundException, InstanceNotFoundException {
        final Object handback = new Object();
        server.addNotificationListener(vvrObjectName, DUMMY_LISTENER, null, handback);
        server.removeNotificationListener(vvrObjectName, DUMMY_LISTENER, null, handback);
    }

    @Test(expected = ListenerNotFoundException.class)
    public void removeAddedListener3Failed() throws ListenerNotFoundException, InstanceNotFoundException {
        server.addNotificationListener(vvrObjectName, DUMMY_LISTENER, null, new Object());
        server.removeNotificationListener(vvrObjectName, DUMMY_LISTENER, null, new Object());
    }

    @Test
    public void removeListenNameChange() throws ListenerNotFoundException, UnknownHostException,
            InstanceNotFoundException, InterruptedException {
        final Object handback = new Object();

        final NotificationListenerExpectation listener = new NotificationListenerExpectation(handback, "name0", "name1");
        server.addNotificationListener(vvrObjectName, listener, null, handback);
        try {
            setName("name1");
            Assert.assertTrue(listener.notified.get());
        }
        finally {
            server.removeNotificationListener(vvrObjectName, listener);
        }
    }

    @Test
    public void removeListenDescriptionChange() throws ListenerNotFoundException, UnknownHostException,
            InstanceNotFoundException, InterruptedException {
        final Object handback = new Object();

        final NotificationListenerExpectation listener = new NotificationListenerExpectation(handback, "desc0", "desc1");
        server.addNotificationListener(vvrObjectName, listener, null, handback);
        try {
            setDescription("desc1");
            Assert.assertTrue(listener.notified.get());
        }
        finally {
            server.removeNotificationListener(vvrObjectName, listener);
        }
    }
}
