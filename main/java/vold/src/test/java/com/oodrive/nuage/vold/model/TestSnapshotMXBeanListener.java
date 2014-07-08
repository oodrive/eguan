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

import org.junit.Before;

public class TestSnapshotMXBeanListener extends AbstractMXBeanListener {

    private SnapshotMXBean snap;

    public TestSnapshotMXBeanListener() throws Exception {
        super();
    }

    @Before
    public void init() {
        snap = helper.getSnapshot(vvrUuid, rootUuid);
        snap.setName("name0");
        snap.setDescription("desc0");
        mbeanName = helper.newSnapshotObjectName(vvrUuid, rootUuid);
    }

    @Override
    public void setName(final String name) {
        snap.setName(name);
    }

    @Override
    public void setDescription(final String description) {
        snap.setDescription(description);
    }
}
