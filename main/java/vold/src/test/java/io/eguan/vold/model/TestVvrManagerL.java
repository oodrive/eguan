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

import io.eguan.configuration.ConfigValidationException;
import io.eguan.utils.Files;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrManager;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestVvrManagerL extends TestVvrManagerAbstract {

    private VvrManagerHelper vvrManagerHelperTmp;
    private DummyMBeanServer serverTmp;

    private VvrMXBean vvrMXBean1;
    private VvrMXBean vvrMXBean2;

    public TestVvrManagerL(final Boolean startedKey) {
        super(startedKey);
    }

    @After
    public void deleteVvrs() throws Exception {

        if (vvrManagerHelperTmp != null) {
            vvrManagerHelperTmp.fini(serverTmp);
        }
        else {
            if (vvrMXBean1 != null) {
                voldTestHelper.deleteVvr(dummyMbeanServer, UUID.fromString(vvrMXBean1.getUuid()));
                vvrMXBean1 = null;
            }
            if (vvrMXBean2 != null) {
                voldTestHelper.deleteVvr(dummyMbeanServer, UUID.fromString(vvrMXBean2.getUuid()));
                vvrMXBean2 = null;
            }
        }
    }

    @Test
    public void testCreationOfVvrDir() throws NullPointerException, IllegalArgumentException, IOException,
            ConfigValidationException {
        Assert.assertTrue(voldTestHelper.getVvrsFile().isDirectory());
    }

    @Test
    public void testCreationOfOneVvr() throws Exception {
        vvrMXBean1 = voldTestHelper.createVvr(dummyMbeanServer, "name", "description");

        // VvrManager + VVR + root snapshot if started
        Assert.assertEquals(getDefaultMbeansNb(1), dummyMbeanServer.getNbMXBeans());
        checkVvr(vvrMXBean1, "name", "description", voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteVvr() throws Exception {

        vvrMXBean1 = voldTestHelper.createVvr(dummyMbeanServer, "name", "description");
        Assert.assertEquals(getDefaultMbeansNb(1), dummyMbeanServer.getNbMXBeans());

        checkVvr(vvrMXBean1, "name", "description", voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);

        if (voldTestHelper.isVvrStarted()) {
            vvrMXBean1.stop();
        }
        vvrManager.delete(vvrMXBean1.getUuid());

        Assert.assertTrue(Files.waitForFileDeletion(new File(voldTestHelper.getIbpFile(), vvrMXBean1.getUuid()), 5000));
        Assert.assertTrue(Files.waitForFileDeletion(new File(voldTestHelper.getIbpGenFile(), vvrMXBean1.getUuid()),
                5000));
        Assert.assertTrue(Files.waitForFileDeletion(new File(voldTestHelper.getVvrsFile(), vvrMXBean1.getUuid()), 5000));
        Assert.assertEquals(getDefaultMbeansNb(0), dummyMbeanServer.getNbMXBeans());

        vvrMXBean1 = null;

        vvrMXBean2 = voldTestHelper.createVvr(dummyMbeanServer, "name", "description");
        Assert.assertEquals(getDefaultMbeansNb(1), dummyMbeanServer.getNbMXBeans());

        checkVvr(vvrMXBean2, "name", "description", voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);

        if (!voldTestHelper.isVvrStarted()) {
            vvrMXBean2.start();
        }

        // The Vvr is started, should raise an exception
        vvrManager.delete(vvrMXBean2.getUuid());
        // should not go here
        vvrMXBean2 = null;
    }

    @Test
    public void testStopVvrUnregisterChildren() throws Exception {

        vvrMXBean1 = voldTestHelper.createVvr(dummyMbeanServer, "name", "description");

        Assert.assertEquals(getDefaultMbeansNb(1), dummyMbeanServer.getNbMXBeans());
        checkVvr(vvrMXBean1, "name", "description", voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);

        if (!voldTestHelper.isVvrStarted()) {
            vvrMXBean1.start();
        }

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, dummyMbeanServer.getNbMXBeans());
        vvrMXBean1.stop();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 1, dummyMbeanServer.getNbMXBeans());
    }

    private final DummyMBeanServer initVvrManagerHelperTmp() throws Exception {
        // use a temporary helper because vvrmanager must be fini during the test
        vvrManagerHelperTmp = new VvrManagerHelper();
        serverTmp = new DummyMBeanServer();
        // init as the current voldTestHelper
        vvrManagerHelperTmp.init(voldTestHelper.isVvrStarted(), serverTmp, 2);
        return serverTmp;
    }

    @Test
    public void testLoadPersistedVvrsWithParasites() throws Exception {

        final int NB_VVRS = 4;

        // use temporary helper
        final DummyMBeanServer server = initVvrManagerHelperTmp();
        final VoldTestHelper helper = vvrManagerHelperTmp.getVoldTestHelper();
        VvrManager vvrManagerTmp = vvrManagerHelperTmp.getVvrManager();

        final String[][] createVvrParams = { { "name1", "description1" }, { "name2", null }, { null, "description2" },
                { null, null } };

        final VvrMXBean[] vvrMXBeans = new VvrMXBean[NB_VVRS];
        for (int i = 0; i < NB_VVRS; i++) {
            vvrMXBeans[i] = helper.createVvr(server, createVvrParams[i][0], createVvrParams[i][1]);
            checkVvr(vvrMXBeans[i], createVvrParams[i][0], createVvrParams[i][1], helper.VOLD_OWNER_UUID_TEST_STR,
                    Boolean.TRUE, helper.isVvrStarted(), helper);
        }
        // The registration of 1 VVR into the MBean server implies the registration of 2 mbeans.
        Assert.assertEquals(getDefaultMbeansNb(NB_VVRS), server.getNbMXBeans());

        // Then terminate the VVR manager
        vvrManagerTmp.fini();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT - 1, server.getNbMXBeans());

        // Create parasites into the VVRs directory
        final File fakeDirectory1 = new File(helper.getVvrsFile(), "zorglub");
        Assert.assertTrue(fakeDirectory1.mkdir());

        final File fakeDirectory2 = new File(helper.getVvrsFile(), UUID.randomUUID().toString());
        Assert.assertTrue(fakeDirectory2.mkdir());

        final File fakeFileWithUUID = new File(helper.getVvrsFile(), UUID.randomUUID().toString());
        Assert.assertTrue(fakeFileWithUUID.createNewFile());

        // Reload VVRs trough VVR manager
        vvrManagerTmp = VvrManagerTestUtils.createVvrManager(helper, server);
        vvrManagerHelperTmp.setVvrManager(vvrManagerTmp);
        VvrManagerHelper.initDtxManagement(vvrManagerTmp, vvrManagerHelperTmp.getDtxManager());
        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server, getDefaultMbeansNb(NB_VVRS)));

        Assert.assertEquals(getDefaultMbeansNb(NB_VVRS), server.getNbMXBeans());

        // Check the loaded VVRs
        for (int i = 0; i < NB_VVRS; i++) {
            final ObjectName vvrCreated = VvrManagerTestUtils.getVvrObjectName(helper.VOLD_OWNER_UUID_TEST,
                    UUID.fromString(vvrMXBeans[i].getUuid()));
            vvrMXBeans[i] = (VvrMXBean) server.waitMXBean(vvrCreated);
            checkVvr(vvrMXBeans[i], createVvrParams[i][0], createVvrParams[i][1], helper.VOLD_OWNER_UUID_TEST_STR,
                    Boolean.TRUE, helper.isVvrStarted(), helper);
        }
    }

    @Test
    public void testLoadPersistedVvrWithCfgChanged() throws Exception {

        // use temporary helper
        final DummyMBeanServer server = initVvrManagerHelperTmp();
        final VoldTestHelper helper = vvrManagerHelperTmp.getVoldTestHelper();
        VvrManager vvrManagerTmp = vvrManagerHelperTmp.getVvrManager();

        // Create a VVR and modify its configuration
        VvrMXBean vvrMXBean = helper.createVvr(server, "name1", "description1");
        Assert.assertEquals(getDefaultMbeansNb(1), server.getNbMXBeans());
        checkVvr(vvrMXBean, "name1", "description1", helper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), helper);
        vvrMXBean.setName("name11");
        vvrMXBean.setDescription("description11");
        checkVvr(vvrMXBean, "name11", "description11", helper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), helper);

        if (!voldTestHelper.isVvrStarted()) {
            vvrMXBean.start();
        }
        checkVvr(vvrMXBean, "name11", "description11", helper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE, true, helper);
        vvrMXBean.stop();
        checkVvr(vvrMXBean, "name11", "description11", helper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE, false, helper);

        vvrManagerTmp.fini();
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT - 1, server.getNbMXBeans());

        // Reload VVRs
        vvrManagerTmp = VvrManagerTestUtils.createVvrManager(helper, server);
        vvrManagerHelperTmp.setVvrManager(vvrManagerTmp);
        VvrManagerHelper.initDtxManagement(vvrManagerTmp, vvrManagerHelperTmp.getDtxManager());
        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(server, VoldTestHelper.MXBEANS_NUMBER_INIT + 1));

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 1, server.getNbMXBeans());

        final ObjectName vvrCreated = VvrManagerTestUtils.getVvrObjectName(helper.VOLD_OWNER_UUID_TEST,
                UUID.fromString(vvrMXBean.getUuid()));
        vvrMXBean = (VvrMXBean) server.waitMXBean(vvrCreated);
        checkVvr(vvrMXBean, "name11", "description11", helper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE, false, helper);
    }
}
