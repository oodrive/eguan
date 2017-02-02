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

import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;

import java.util.UUID;

import org.junit.Before;

public class TestDeviceMXBeanListener extends AbstractMXBeanListener {

    private DeviceMXBean device;

    public TestDeviceMXBeanListener() throws Exception {
        super();
    }

    @Before
    public void init() {
        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        final String deviceTaskUuid = rootSnapshot.createDevice("name0", 4096);
        device = helper.getDevice(vvrUuid, deviceTaskUuid);
        device.setDescription("desc0");
        mbeanName = helper.newDeviceObjectName(vvrUuid, UUID.fromString(device.getUuid()));
    }

    @Override
    public void setName(final String name) {
        device.setName(name);
    }

    @Override
    public void setDescription(final String description) {
        device.setDescription(description);
    }
}
