package io.eguan.vold;

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

import io.eguan.srv.BasicIopsTestHelper;
import io.eguan.srv.ClientBasicIops;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.DummyMBeanServer;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VoldTestHelper;
import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrManagerTestUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public abstract class TestMultiVoldBasicIopsOnTargetAbstract extends TestMultiVoldAbstract {

    private ClientBasicIops client1;
    private ClientBasicIops client2;
    private ClientBasicIops client3;
    private final BasicIopsTestHelper ioHelper;
    private final String deviceName = "dev0";
    private final long deviceSize = 8192 * 1024L * 1024L;

    private VvrManagerMXBean vvrManager1;
    private DummyMBeanServer server1;
    private DummyMBeanServer server2;
    private DummyMBeanServer server3;
    private VoldTestHelper voldTestHelper1;
    private VoldTestHelper voldTestHelper2;
    private VoldTestHelper voldTestHelper3;

    private DeviceMXBean d1;

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { Integer.valueOf(BLOCKSIZE), Integer.valueOf(NUMBLOCKS) },
                { Integer.valueOf(BLOCKSIZE), Integer.valueOf(NUMBLOCKS_SINGLE) },
                { Integer.valueOf(BLOCKSIZE_PARTIAL), Integer.valueOf(NUMBLOCKS_SINGLE) } };
        return Arrays.asList(data);
    }

    // 3 nodes, vvr started automatically
    public TestMultiVoldBasicIopsOnTargetAbstract(final int blockSize, final int numBlocks) {
        ioHelper = new BasicIopsTestHelper(blockSize, numBlocks, LENGTH);
    }

    @BeforeClass
    public static final void init() throws Exception {
        setUpVolds(3, 3, true);
    }

    /**
     * Create a new client on a given server.
     * 
     * @param numServer
     *            the index of the server. Start from 0
     * 
     * @return the new client
     */
    protected abstract ClientBasicIops createClient(final int serverIndex);

    @Before
    public void initialize() throws Exception {
        server1 = getDummyMBeanServer(0);
        Assert.assertNotNull(server1);
        voldTestHelper1 = getVoldTestHelper(0);
        Assert.assertNotNull(voldTestHelper1);
        vvrManager1 = VvrManagerTestUtils.getVvrManagerMXBean(server1, VOLD_OWNER);
        Assert.assertNotNull(vvrManager1);

        server2 = getDummyMBeanServer(1);
        Assert.assertNotNull(server2);
        voldTestHelper2 = getVoldTestHelper(1);
        Assert.assertNotNull(voldTestHelper2);

        server3 = getDummyMBeanServer(2);
        Assert.assertNotNull(server3);
        voldTestHelper3 = getVoldTestHelper(2);
        Assert.assertNotNull(voldTestHelper3);

        // Create a vvr on node 1
        final SnapshotMXBean rootSnapshot = createVvrStarted();

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server1.getNbMXBeans());
        // Check if that VVR is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server2.getNbMXBeans());
        // Check if that VVR is created into the third VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server3.getNbMXBeans());

        // Create and activate a RW device on node 1
        d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, deviceName, deviceSize);
        d1.setIscsiBlockSize(ioHelper.getBlockSize());
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d1);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());
        // Check if that device is created into the third VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server3.getNbMXBeans());

        // Create client for the two first servers
        client1 = createClient(0);
        client2 = createClient(1);
        client3 = createClient(2);
    }

    @Test
    public void testWriteReadAfterStopNode() throws Exception {

        // Stop server2
        stopNode(1);

        // Write data on device from node 1
        final File data = ioHelper.initiatorReadWriteData(client1, deviceName, deviceSize);

        try {
            // Deactivate device on node 1
            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);

            // Stop server1
            stopNode(0);

            // Restart server2
            startNode(1);
            server2 = getDummyMBeanServer(1);
            Assert.assertNotNull(server2);
            voldTestHelper2 = getVoldTestHelper(1);
            Assert.assertNotNull(voldTestHelper2);

            // Read data on node 2
            setDeviceRO(server2, voldTestHelper2, vvrUuid, waitDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));
            ioHelper.initiatorReadData(data, client2, deviceName, deviceSize);

            // Read data on node 3
            setDeviceRO(server3, voldTestHelper3, vvrUuid, waitDeviceMXBeanOnOtherServer(vvrUuid, d1, server3));
            ioHelper.initiatorReadData(data, client3, deviceName, deviceSize);

        }
        finally {
            data.delete();
        }
    }

}
