package io.eguan.srv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.srv.AbstractServer;
import io.eguan.srv.AbstractServerConfig;
import io.eguan.srv.DeviceTarget;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.AttributeChangeNotification;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.junit.Assert;
import org.junit.Test;

public abstract class TestAbstractServerNotificationListener<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig> {
    /**
     * Dummy listener. Does nothing.
     */
    private static final NotificationListener DUMMY_LISTENER = new NotificationListener() {

        @Override
        public final void handleNotification(final Notification notification, final Object handback) {
            // No op
        }
    };

    /**
     * Listener that expects some values.
     * 
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
            Assert.assertEquals(server, notification.getSource());
            Assert.assertEquals(this.handback, handback);
            Assert.assertTrue(notification instanceof AttributeChangeNotification);
            final AttributeChangeNotification attributeChangeNotification = (AttributeChangeNotification) notification;
            Assert.assertEquals(this.oldValue, attributeChangeNotification.getOldValue());
            Assert.assertEquals(this.newValue, attributeChangeNotification.getNewValue());
            notified.set(true);
        }

    }

    protected AbstractServer<S, T, K> server;

    @Test(expected = ListenerNotFoundException.class)
    public void removeListener1() throws ListenerNotFoundException {
        server.removeNotificationListener(DUMMY_LISTENER);
    }

    @Test(expected = ListenerNotFoundException.class)
    public void removeListener3() throws ListenerNotFoundException {
        server.removeNotificationListener(DUMMY_LISTENER, null, new Object());
    }

    @Test
    public void removeAddedListener1() throws ListenerNotFoundException {
        server.addNotificationListener(DUMMY_LISTENER, null, new Object());
        server.removeNotificationListener(DUMMY_LISTENER);
    }

    @Test
    public void removeAddedListener3() throws ListenerNotFoundException {
        final Object handback = new Object();
        server.addNotificationListener(DUMMY_LISTENER, null, handback);
        server.removeNotificationListener(DUMMY_LISTENER, null, handback);
    }

    @Test(expected = ListenerNotFoundException.class)
    public void removeAddedListener3Failed() throws ListenerNotFoundException {
        server.addNotificationListener(DUMMY_LISTENER, null, new Object());
        server.removeNotificationListener(DUMMY_LISTENER, null, new Object());
    }

    @Test
    public void removeListenAddrChange() throws ListenerNotFoundException, UnknownHostException {
        final Object handback = new Object();
        final InetAddress newAddr = InetAddress.getByName("0.0.0.0");
        final NotificationListenerExpectation listener = new NotificationListenerExpectation(handback,
                InetAddress.getLoopbackAddress(), newAddr);
        server.addNotificationListener(listener, null, handback);
        try {
            server.setAddress(newAddr);
            Assert.assertTrue(listener.notified.get());
        }
        finally {
            server.removeNotificationListener(listener);
        }
    }

    @Test
    public void removeListenPortChange() throws ListenerNotFoundException {
        final Object handback = new Object();
        final Integer newPort = Integer.valueOf(5555);
        final NotificationListenerExpectation listener = new NotificationListenerExpectation(handback,
                Integer.valueOf(server.getPort()), newPort);
        server.addNotificationListener(listener, null, handback);
        try {
            server.setPort(newPort.intValue());
            Assert.assertTrue(listener.notified.get());
        }
        finally {
            server.removeNotificationListener(listener);
        }
    }

    @Test
    public void removeListenStartChange() throws ListenerNotFoundException {
        final Object handback = new Object();
        final Boolean newStart = Boolean.FALSE;
        final NotificationListenerExpectation listener = new NotificationListenerExpectation(handback, Boolean.TRUE,
                newStart);
        boolean started = true;
        server.start();
        try {
            server.addNotificationListener(listener, null, handback);
            try {
                server.stop();
                started = false;
                Assert.assertTrue(listener.notified.get());
            }
            finally {
                server.removeNotificationListener(listener);
            }
        }
        finally {
            if (started)
                server.stop();
        }
    }
}
