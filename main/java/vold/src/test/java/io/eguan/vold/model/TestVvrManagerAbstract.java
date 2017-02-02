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

import io.eguan.dtx.DtxManager;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrManagementException;
import io.eguan.vold.model.VvrManager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.Arrays;
import java.util.Collection;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public abstract class TestVvrManagerAbstract {

    private static VvrManagerHelper vvrManagerHelperTrue = null;
    private static VvrManagerHelper vvrManagerHelperFalse = null;

    protected DummyMBeanServer dummyMbeanServer;
    protected final VoldTestHelper voldTestHelper;
    protected VvrManager vvrManager;
    protected final DtxManager dtxManager;
    protected final ObjectName dtxManagerObjectName;
    protected final ObjectName dtxLocalNodeObjectName;

    public TestVvrManagerAbstract(final Boolean vvrStarted) {
        if (vvrStarted.booleanValue()) {
            this.voldTestHelper = vvrManagerHelperTrue.getVoldTestHelper();
            this.vvrManager = vvrManagerHelperTrue.getVvrManager();
            this.dtxManager = vvrManagerHelperTrue.getDtxManager();
            this.dtxManagerObjectName = vvrManagerHelperTrue.getDtxManagerObjectName();
            this.dtxLocalNodeObjectName = vvrManagerHelperTrue.getDtxLocalNodeObjectName();
            this.dummyMbeanServer = DummyMBeanServer.getMBeanServer1();
        }
        else {
            this.voldTestHelper = vvrManagerHelperFalse.getVoldTestHelper();
            this.vvrManager = vvrManagerHelperFalse.getVvrManager();
            this.dtxManager = vvrManagerHelperFalse.getDtxManager();
            this.dtxManagerObjectName = vvrManagerHelperFalse.getDtxManagerObjectName();
            this.dtxLocalNodeObjectName = vvrManagerHelperFalse.getDtxLocalNodeObjectName();
            this.dummyMbeanServer = DummyMBeanServer.getMBeanServer2();
        }
    }

    @Parameters
    public static Collection<Boolean[]> getStartedConfig() {
        final Boolean[][] vvrStarteds = new Boolean[][] { { Boolean.TRUE }, { Boolean.FALSE } };
        return Arrays.asList(vvrStarteds);
    }

    @BeforeClass
    public static void initVvrManagers() throws Exception {
        vvrManagerHelperTrue = new VvrManagerHelper();
        vvrManagerHelperTrue.init(true, DummyMBeanServer.getMBeanServer1(), 0);
        vvrManagerHelperFalse = new VvrManagerHelper();
        vvrManagerHelperFalse.init(false, DummyMBeanServer.getMBeanServer2(), 1);
    }

    @AfterClass
    public static void finiVvrManagers() throws Exception {
        vvrManagerHelperTrue.fini(DummyMBeanServer.getMBeanServer1());
        vvrManagerHelperFalse.fini(DummyMBeanServer.getMBeanServer2());
    }

    /**
     * Returns the number of mbeans registered according to the started key of the VVR template during the creation of a
     * new VVR.
     * 
     * @param nbVvrCreated
     * @return
     */
    final protected int getDefaultMbeansNb(final int nbVvrCreated) {
        if (voldTestHelper.isVvrStarted()) {
            return (nbVvrCreated * 2) + VoldTestHelper.MXBEANS_NUMBER_INIT; // n * (VVR + root) + VVR Mgr + DTX Mgr +
                                                                            // Dtx LocalNode
        }
        else {
            return nbVvrCreated + VoldTestHelper.MXBEANS_NUMBER_INIT;// n * VVR + VVR Mgr + DTX Mgr + Dtx LocalNode
        }
    }

    final VvrMXBean createVvr(final String name, final String description) throws VvrManagementException,
            MalformedObjectNameException {
        final VvrMXBean vvrMXBean = voldTestHelper.createVvr(dummyMbeanServer, name, description);
        checkVvr(vvrMXBean, name, description, voldTestHelper.VOLD_OWNER_UUID_TEST_STR, Boolean.TRUE,
                voldTestHelper.isVvrStarted(), voldTestHelper);
        return vvrMXBean;
    }

    final static void checkVvr(final VvrMXBean vvr, final String name, final String description,
            final String ownerUuid, final Boolean isInitialized, final boolean isStarted, final VoldTestHelper helper) {
        Assert.assertNotNull(vvr);
        Assert.assertEquals(ownerUuid, vvr.getOwnerUuid());

        // TODO fix it with the new configuration
        if (name == null || "".equals(name)) {
            Assert.assertTrue(vvr.getName() == null || "".equals(vvr.getName()));
        }
        else {
            Assert.assertEquals(name, vvr.getName());
        }

        if (description == null || "".equals(description)) {
            Assert.assertTrue(vvr.getDescription() == null || "".equals(vvr.getDescription()));
        }
        else {
            Assert.assertEquals(description, vvr.getDescription());
        }

        Assert.assertEquals(isInitialized, Boolean.valueOf(vvr.isInitialized()));
        Assert.assertEquals(Boolean.valueOf(isStarted), Boolean.valueOf(vvr.isStarted()));
        Assert.assertTrue(Files.isDirectory(new File(helper.getVvrsFile(), vvr.getUuid()).toPath(),
                LinkOption.NOFOLLOW_LINKS));
        Assert.assertTrue(Files.isDirectory(new File(helper.getIbpFile(), vvr.getUuid()).toPath(),
                LinkOption.NOFOLLOW_LINKS));
        Assert.assertTrue(Files.isDirectory(new File(helper.getIbpGenFile(), vvr.getUuid()).toPath(),
                LinkOption.NOFOLLOW_LINKS));
    }
}
