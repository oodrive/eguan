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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.oodrive.nuage.configuration.ConfigValidationException;
import com.oodrive.nuage.dtx.DtxManager;
import com.oodrive.nuage.utils.Files;

@RunWith(value = Parameterized.class)
public class TestVvrManagerConfigL {

    protected final VoldTestHelper voldTestHelper;
    protected VvrManager vvrManager = null;
    protected DtxManager dtxManager = null;
    protected ObjectName dtxManagerObjectName = null;
    protected ObjectName dtxLocalNodeObjectName = null;

    public TestVvrManagerConfigL(final Boolean startedKey) {
        this.voldTestHelper = new VoldTestHelper(startedKey);
    }

    @Parameters
    public static Collection<Boolean[]> getStartedConfig() {
        final Boolean[][] vvrStarteds = new Boolean[][] { { Boolean.TRUE }, { Boolean.FALSE } };
        return Arrays.asList(vvrStarteds);
    }

    @Before
    public void initDtxManager() throws Exception {
        voldTestHelper.createTemporary();
        dtxManager = VvrManagerTestUtils.createDtxManagerStandAlone(voldTestHelper);
        dtxManagerObjectName = VvrManagerTestUtils.registerDtxManagerMXBean(DummyMBeanServer.getMBeanServer1(),
                voldTestHelper, dtxManager);
        dtxLocalNodeObjectName = VvrManagerTestUtils.registerDtxLocalNodeMXBean(DummyMBeanServer.getMBeanServer1(),
                voldTestHelper, dtxManager.new DtxLocalNode());

    }

    @After
    public void finiDtxManager() throws Exception {

        final DummyMBeanServer dummyMBeanServer = DummyMBeanServer.getMBeanServer1();

        if (dtxManagerObjectName != null) {
            dummyMBeanServer.unregisterMBean(dtxManagerObjectName);
            dtxManagerObjectName = null;
        }
        if (dtxLocalNodeObjectName != null) {
            dummyMBeanServer.unregisterMBean(dtxLocalNodeObjectName);
            dtxLocalNodeObjectName = null;
        }
        if (dtxManager != null) {
            dtxManager.stop();
            dtxManager.fini();
            dtxManager = null;
        }

        // All MXBeans are unregistered
        Assert.assertEquals(0, DummyMBeanServer.getMBeanServer1().getNbMXBeans());

        voldTestHelper.destroy();
        vvrManager = null;
    }

    @Test(expected = FileNotFoundException.class)
    public void testCreateVvrManagerWithoutVoldDir() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {
        Files.deleteRecursive(voldTestHelper.getVoldFile().toPath());
        VvrManagerTestUtils.createVvrManager(voldTestHelper);
    }

    @Test(expected = IllegalStateException.class)
    public void testInitVvrManagerWithVoldDirAsFile() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {
        Assert.assertTrue(voldTestHelper.getVvrsFile().delete());
        Assert.assertTrue(voldTestHelper.getVvrsFile().createNewFile());
        final VvrManager vvrManager = VvrManagerTestUtils.createVvrManager(voldTestHelper);
        VvrManagerHelper.initDtxManagement(vvrManager, dtxManager);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateVvrManagerWithVoldDirAsFile() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {
        Assert.assertTrue(voldTestHelper.getVvrsFile().delete());
        Assert.assertTrue(voldTestHelper.getVvrsFile().createNewFile());
        final VvrManager vvrManager = VvrManagerTestUtils.createVvrManager(voldTestHelper);
        VvrManagerHelper.initDtxManagement(vvrManager, dtxManager);
    }

    @Test(expected = FileNotFoundException.class)
    public void testCreateVvrManagerWithoutVoldCfg() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {

        java.nio.file.Files.delete(new File(voldTestHelper.getVoldFile(), VoldTestHelper.VOLD_CONFIG_FILE).toPath());
        VvrManagerTestUtils.createVvrManager(voldTestHelper);
    }

    @Test(expected = FileNotFoundException.class)
    public void testCreateVvrManagerWithoutVvrCfg() throws NullPointerException, IllegalArgumentException, IOException,
            ConfigValidationException {

        java.nio.file.Files.delete(new File(voldTestHelper.getVoldFile(), VoldTestHelper.VVR_TEMPLATE).toPath());
        VvrManagerTestUtils.createVvrManager(voldTestHelper);
    }

    @Test
    public void testCreateVvrManagerWithoutVvrDir() throws IOException, NullPointerException, IllegalArgumentException,
            ConfigValidationException {

        Assert.assertTrue(voldTestHelper.getVvrsFile().delete());
        final VvrManager vvrManager = VvrManagerTestUtils.createVvrManager(voldTestHelper);
        try {
            VvrManagerHelper.initDtxManagement(vvrManager, dtxManager);
            Assert.assertTrue(voldTestHelper.getVvrsFile().isDirectory());
        }
        finally {
            vvrManager.fini();
        }
    }
}
