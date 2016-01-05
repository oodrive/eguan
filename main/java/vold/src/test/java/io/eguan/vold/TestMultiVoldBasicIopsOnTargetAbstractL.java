package io.eguan.vold;

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

import io.eguan.srv.BasicIopsTestHelper;
import io.eguan.srv.ClientBasicIops;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.DummyMBeanServer;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VoldTestHelper;
import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrManagerTestUtils;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMultiVoldBasicIopsOnTargetAbstractL extends TestMultiVoldAbstract {

    // 3 nodes, vvr started automatically
    public TestMultiVoldBasicIopsOnTargetAbstractL() {
        ioHelper = new BasicIopsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);
    }

    private ClientBasicIops client1;
    private ClientBasicIops client2;
    private final BasicIopsTestHelper ioHelper;
    private final String deviceName1 = "dev0";
    private final String deviceName2 = "dev1";
    private final String anotherDeviceName1 = "anotherDev0";
    private final String anotherDeviceName2 = "anotherDev1";

    private final long deviceSize = 8192 * 1024L * 1024L;
    private final long anotherDeviceSize = 8192 * 1024L;

    private VvrManagerMXBean vvrManager1;
    private DummyMBeanServer server1;
    private DummyMBeanServer server2;
    private DummyMBeanServer server3;
    private VoldTestHelper voldTestHelper1;
    private VoldTestHelper voldTestHelper2;
    private VoldTestHelper voldTestHelper3;

    private DeviceMXBean d1;
    private SnapshotMXBean rootSnapshot;

    @BeforeClass
    public static final void init() throws Exception {
        setUpVolds(3, 3, true);
    }

    /**
     * Create a new client.
     * 
     * @param serverIndex
     * 
     * @return the new client
     */
    public abstract ClientBasicIops createClient(final int serverIndex);

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
        rootSnapshot = createVvrStarted();

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server1.getNbMXBeans());
        // Check if that VVR is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server2.getNbMXBeans());
        // Check if that VVR is created into the third VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server3.getNbMXBeans());

        // Create and activate a RW device on node 1
        d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, deviceName1, deviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d1);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());
        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());
        // Check if that device is created into the third VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server3.getNbMXBeans());

        // Create clients for the two first servers
        client1 = createClient(0);
        client2 = createClient(1);
    }

    @Test
    public void testWriteReadByTwoNodes() throws Exception {

        // Write data on device from node 1
        final File data = ioHelper.initiatorReadWriteData(client1, deviceName1, deviceSize);
        try {

            // Deactivate device on node 1
            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
            setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);
            setDeviceRO(server2, voldTestHelper2, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));

            // Read data on node 1 and 2
            ioHelper.initiatorReadData(data, client1, deviceName1, deviceSize);
            ioHelper.initiatorReadData(data, client2, deviceName1, deviceSize);
        }
        finally {
            data.delete();
        }

    }

    @Test
    public void testWriteReadAfterStopNodeTwoDevices() throws Exception {

        // Stop server2
        stopNode(1);

        // Create and activate a RW device on node 1
        final DeviceMXBean d2 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid,
                deviceName2, deviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d2);

        // Write data on device0 from node 1
        final File data0 = ioHelper.initiatorReadWriteData(client1, deviceName1, deviceSize);
        try {

            // Deactivate device0 on node 1
            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
            setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);

            // Write data on device1 from node 1
            final File data1 = ioHelper.initiatorReadWriteData(client1, deviceName2, deviceSize);
            try {

                // Restart server2
                startNode(1);
                server2 = getDummyMBeanServer(1);
                Assert.assertNotNull(server2);
                voldTestHelper2 = getVoldTestHelper(1);
                Assert.assertNotNull(voldTestHelper2);

                setDeviceRO(server2, voldTestHelper2, vvrUuid, waitDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));

                // Deactivate device1 on node 1
                setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d2);
                setDeviceRO(server1, voldTestHelper1, vvrUuid, d2);
                setDeviceRO(server2, voldTestHelper2, vvrUuid, waitDeviceMXBeanOnOtherServer(vvrUuid, d2, server2));

                // Read data0 on node 2
                ioHelper.initiatorReadData(data0, client2, deviceName1, deviceSize);

                // Stop the first node, to read from node 3
                stopNode(0);

                // Read data on node 2
                ioHelper.initiatorReadData(data1, client2, deviceName2, deviceSize);

                // Stop the last node, to read from local Ibs
                stopNode(2);

                // Read data on node 2
                ioHelper.initiatorReadData(data0, client2, deviceName1, deviceSize);
                ioHelper.initiatorReadData(data1, client2, deviceName2, deviceSize);
            }
            finally {
                data1.delete();
            }
        }
        finally {
            data0.delete();
        }

    }

    @Test
    public void testWriteReadTwoDevices() throws Exception {
        // Create and activate a RW device on node 1
        final DeviceMXBean d2 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid,
                deviceName2, deviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d2);

        // Create another client on the server 1
        final ClientBasicIops anotherClient1 = createClient(0);

        // Write on the two devices on the server1
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<File> future1 = ioHelper.multiThreadRW(executor, deviceName1, client1, deviceSize);
            final Future<File> future2 = ioHelper.multiThreadRW(executor, deviceName2, anotherClient1, deviceSize);

            final File dataDump1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File dataDump2 = future2.get(1, TimeUnit.MINUTES);
                try {
                    // Set the device 2 read only on the two servers
                    setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d2);
                    setDeviceRO(server1, voldTestHelper1, vvrUuid, d2);
                    setDeviceRO(server2, voldTestHelper2, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2));

                    // Set the device 1 read only on the two servers
                    setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
                    setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);
                    setDeviceRO(server2, voldTestHelper2, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));

                    // Check files on server 2
                    ioHelper.initiatorReadData(dataDump1, client2, deviceName1, deviceSize);
                    ioHelper.initiatorReadData(dataDump2, client2, deviceName2, deviceSize);

                    // Check them on the third node
                    final ClientBasicIops client3 = createClient(2);
                    setDeviceRO(server3, voldTestHelper3, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d1, server3));
                    setDeviceRO(server3, voldTestHelper3, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d2, server3));
                    ioHelper.initiatorReadData(dataDump1, client3, deviceName1, deviceSize);
                    ioHelper.initiatorReadData(dataDump2, client3, deviceName2, deviceSize);

                }
                finally {
                    dataDump2.delete();
                }
            }
            finally {
                dataDump1.delete();
            }
        }
        finally {
            executor.shutdownNow();
        }

    }

    @Test
    public void testWriteReadOneDeviceOnTwoNodes() throws Exception {
        // Create and activate a RW device on node 2
        final DeviceMXBean d2 = VvrManagerTestUtils.createDevice(server2, voldTestHelper2, rootSnapshot, vvrUuid,
                deviceName2, deviceSize);
        setDeviceRW(server2, voldTestHelper2, vvrUuid, d2);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            // Write on the first device on the server1
            final Future<File> future1 = ioHelper.multiThreadRW(executor, deviceName1, client1, deviceSize);
            // Write on the second device on the server2
            final Future<File> future2 = ioHelper.multiThreadRW(executor, deviceName2, client2, deviceSize);

            final File dataDump1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File dataDump2 = future2.get(1, TimeUnit.MINUTES);
                try {
                    // Set the device 1 read only on the two servers
                    setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
                    setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);
                    setDeviceRO(server2, voldTestHelper2, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));

                    // Set the device 2 read only on the two servers
                    setDeviceDeActivated(server2, voldTestHelper2, vvrUuid, d2);
                    setDeviceRO(server2, voldTestHelper1, vvrUuid, d2);
                    setDeviceRO(server1, voldTestHelper1, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d2, server1));

                    // Check device 1 on server 2
                    ioHelper.initiatorReadData(dataDump1, client2, deviceName1, deviceSize);
                    // Check device 2 on server 1
                    ioHelper.initiatorReadData(dataDump2, client1, deviceName2, deviceSize);

                    // Check them on the third node
                    final ClientBasicIops client3 = createClient(2);
                    setDeviceRO(server3, voldTestHelper3, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d1, server3));
                    setDeviceRO(server3, voldTestHelper3, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d2, server3));
                    ioHelper.initiatorReadData(dataDump1, client3, deviceName1, deviceSize);
                    ioHelper.initiatorReadData(dataDump2, client3, deviceName2, deviceSize);

                }
                finally {
                    dataDump2.delete();
                }
            }
            finally {
                dataDump1.delete();
            }
        }
        finally {
            executor.shutdownNow();
        }

    }

    @Test
    public void testWriteReadTwoDevicesOnTwoNodes() throws Exception {

        // Create and activate a RW device on node 1
        final DeviceMXBean anotherD1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot,
                vvrUuid, anotherDeviceName1, anotherDeviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, anotherD1);

        // Create and activate two RW device on node 2
        final DeviceMXBean d2 = VvrManagerTestUtils.createDevice(server2, voldTestHelper2, rootSnapshot, vvrUuid,
                deviceName2, deviceSize);
        setDeviceRW(server2, voldTestHelper2, vvrUuid, d2);

        final DeviceMXBean anotherD2 = VvrManagerTestUtils.createDevice(server2, voldTestHelper2, rootSnapshot,
                vvrUuid, anotherDeviceName2, anotherDeviceSize);
        setDeviceRW(server2, voldTestHelper2, vvrUuid, anotherD2);

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            // Write on the first device on the server1
            final Future<File> future1 = ioHelper.multiThreadRW(executor, deviceName1, client1, deviceSize);
            // Write on the second device on the server1
            final ClientBasicIops anotherClient1 = createClient(0);
            final Future<File> anotherFuture1 = ioHelper.multiThreadRW(executor, anotherDeviceName1, anotherClient1,
                    anotherDeviceSize);
            // Write on the first device on the server2
            final Future<File> future2 = ioHelper.multiThreadRW(executor, deviceName2, client2, deviceSize);
            // Write on the second device on the server2
            final ClientBasicIops anotherClient2 = createClient(1);
            final Future<File> anotherFuture2 = ioHelper.multiThreadRW(executor, anotherDeviceName2, anotherClient2,
                    anotherDeviceSize);

            final File dataDump1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File anotherDataDump1 = anotherFuture1.get(1, TimeUnit.MINUTES);
                try {
                    final File dataDump2 = future2.get(1, TimeUnit.MINUTES);
                    try {
                        final File anotherDataDump2 = anotherFuture2.get(1, TimeUnit.MINUTES);
                        try {
                            // Set the device 1 read only on the two servers
                            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
                            setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);
                            setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));
                            // Set the other device 1 read only on the two servers
                            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, anotherD1);
                            setDeviceRO(server1, voldTestHelper1, vvrUuid, anotherD1);
                            setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, anotherD1, server2));

                            // Set the device 2 read only on the two servers
                            setDeviceDeActivated(server2, voldTestHelper2, vvrUuid, d2);
                            setDeviceRO(server2, voldTestHelper2, vvrUuid, d2);
                            setDeviceRO(server1, voldTestHelper1, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, d2, server1));
                            // Set the other device 2 read only on the two servers
                            setDeviceDeActivated(server2, voldTestHelper2, vvrUuid, anotherD2);
                            setDeviceRO(server2, voldTestHelper2, vvrUuid, anotherD2);
                            setDeviceRO(server1, voldTestHelper1, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, anotherD2, server1));

                            // Check device 1 and other device 1 on server 2
                            ioHelper.initiatorReadData(dataDump1, client2, deviceName1, deviceSize);
                            ioHelper.initiatorReadData(anotherDataDump1, client2, anotherDeviceName1, anotherDeviceSize);
                            // Check device 2 and other device 2 on server 1
                            ioHelper.initiatorReadData(dataDump2, client1, deviceName2, deviceSize);
                            ioHelper.initiatorReadData(anotherDataDump2, client1, anotherDeviceName2, anotherDeviceSize);

                            // Check them on the third node
                            final ClientBasicIops client3 = createClient(2);
                            setDeviceRO(server3, voldTestHelper3, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, d1, server3));
                            setDeviceRO(server3, voldTestHelper3, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, d2, server3));
                            setDeviceRO(server3, voldTestHelper3, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, anotherD1, server3));
                            setDeviceRO(server3, voldTestHelper3, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, anotherD2, server3));
                            ioHelper.initiatorReadData(dataDump1, client3, deviceName1, deviceSize);
                            ioHelper.initiatorReadData(dataDump2, client3, deviceName2, deviceSize);
                            ioHelper.initiatorReadData(anotherDataDump1, client3, anotherDeviceName1, anotherDeviceSize);
                            ioHelper.initiatorReadData(anotherDataDump2, client3, anotherDeviceName2, anotherDeviceSize);
                        }
                        finally {
                            anotherDataDump2.delete();
                        }
                    }
                    finally {
                        dataDump2.delete();
                    }
                }
                finally {
                    anotherDataDump1.delete();
                }
            }
            finally {
                dataDump1.delete();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }
}
