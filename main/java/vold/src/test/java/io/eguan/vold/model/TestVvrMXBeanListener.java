package io.eguan.vold.model;

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

import io.eguan.vold.model.VvrMXBean;

import java.net.UnknownHostException;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVvrMXBeanListener extends AbstractMXBeanListener {

    private VvrMXBean vvr;

    public TestVvrMXBeanListener() throws Exception {
        super();
    }

    @Before
    public void init() {
        mbeanName = helper.newVvrObjectName(vvrUuid);
        vvr = helper.getVvr(vvrUuid);
        vvr.setName("name0");
        vvr.setDescription("desc0");
    }

    @Override
    public void setName(final String name) {
        vvr.setName(name);
    }

    @Override
    public void setDescription(final String description) {
        vvr.setDescription(description);
    }

    @Test
    public void removeListenNameChangeVvrNotStarted() throws ListenerNotFoundException, UnknownHostException,
            InstanceNotFoundException, InterruptedException {

        // Stop the VVR
        vvr.stop();

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
