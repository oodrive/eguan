package io.eguan.vold.model;

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

import io.eguan.vold.model.VvrMXBean;

import java.util.UUID;

import javax.management.JMException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVvr extends TestVvrManagerAbstract {

    private final String VVR_NAME_TEST = "vvr test name";
    private final String VVR_DESCRIPTION_TEST = "vvr test description";

    private VvrMXBean vvr;

    public TestVvr(final Boolean vvrStarted) {
        super(vvrStarted);
    }

    @Before
    public void initTestVvr() throws Exception {
        vvr = createVvr("vvr test name", "vvr test description");
    }

    @After
    public void finiTestVvr() throws Exception {
        voldTestHelper.deleteVvr(dummyMbeanServer, UUID.fromString(vvr.getUuid()));
        vvr = null;
    }

    @Test
    public void testStopVvr() {
        if (voldTestHelper.isVvrStarted()) {
            vvr.stop();
        }
        checkVvr(vvr, VVR_NAME_TEST, VVR_DESCRIPTION_TEST, voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                false, voldTestHelper);
    }

    @Test
    public void testStartVvr() throws JMException {
        if (!voldTestHelper.isVvrStarted()) {
            vvr.start();
        }
        checkVvr(vvr, VVR_NAME_TEST, VVR_DESCRIPTION_TEST, voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE, true,
                voldTestHelper);
    }

    @Test
    public void testSetName() {
        vvr.setName("NEW NAME");
        checkVvr(vvr, "NEW NAME", VVR_DESCRIPTION_TEST, voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);
    }

    @Test
    public void testSetDescription() {
        vvr.setDescription("NEW DESCRIPTION");
        checkVvr(vvr, VVR_NAME_TEST, "NEW DESCRIPTION", voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);
    }
}
