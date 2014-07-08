package com.oodrive.nuage.vold;

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
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oodrive.nuage.srv.FsOpsTestHelper;
import com.oodrive.nuage.utils.unix.UnixMount;
import com.oodrive.nuage.utils.unix.UnixTarget;
import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.model.DummyMBeanServer;
import com.oodrive.nuage.vold.model.SnapshotMXBean;
import com.oodrive.nuage.vold.model.VoldTestHelper;
import com.oodrive.nuage.vold.model.VvrManagerMXBean;
import com.oodrive.nuage.vold.model.VvrManagerTestUtils;

public abstract class TestMultiVoldFsOpsOnTargetAbstract extends TestMultiVoldAbstract {

    // 2 Nodes, vvr started automatically
    public TestMultiVoldFsOpsOnTargetAbstract() {
        ioHelper = new FsOpsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);
    }

    private final String deviceName1 = "dev0";
    private final String deviceName2 = "dev1";

    private final long deviceSize = 8192 * 1024L * 1024L;

    private final FsOpsTestHelper ioHelper;

    private VvrManagerMXBean vvrManager1;
    private DummyMBeanServer server1;
    private DummyMBeanServer server2;
    private VoldTestHelper voldTestHelper1;
    private VoldTestHelper voldTestHelper2;

    private DeviceMXBean d1;
    private SnapshotMXBean rootSnapshot;

    @BeforeClass
    public static void init() throws Exception {
        setUpVolds(2, 2, true);
    }

    /**
     * Create a target to connect
     * 
     * @param d1
     *            the device on which the target will be connected
     * 
     * @param serverIndex
     *            the index of the server on which the target is connected. Start from 0
     * 
     * @param numDevice
     *            the index of the first free unix device on which the target can be connected. Start from 0
     * @return the new target
     * @throws IOException
     */
    protected abstract UnixTarget createTarget(final DeviceMXBean d1, final int serverIndex, final int deviceIndex)
            throws IOException;

    @Before
    public void createHelperAndDevice() throws Exception {

        server1 = getDummyMBeanServer(0);
        Assert.assertNotNull(server1);

        server2 = getDummyMBeanServer(1);
        Assert.assertNotNull(server2);

        voldTestHelper1 = getVoldTestHelper(0);
        Assert.assertNotNull(voldTestHelper1);

        voldTestHelper2 = getVoldTestHelper(1);
        Assert.assertNotNull(voldTestHelper2);

        vvrManager1 = VvrManagerTestUtils.getVvrManagerMXBean(server1, VOLD_OWNER);
        Assert.assertNotNull(vvrManager1);

        // Create a vvr on node 1
        rootSnapshot = createVvrStarted();

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server1.getNbMXBeans());
        // Check if that VVR is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 2, server2.getNbMXBeans());

        // Create and activate a RW device on node 1
        d1 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid, deviceName1, deviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d1);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server1.getNbMXBeans());

        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 3, server2.getNbMXBeans());

    }

    private final DeviceMXBean createAnotherDevice() throws Exception {

        // Create and activate a RW device on node 1
        final DeviceMXBean d2 = VvrManagerTestUtils.createDevice(server1, voldTestHelper1, rootSnapshot, vvrUuid,
                deviceName2, deviceSize);
        setDeviceRW(server1, voldTestHelper1, vvrUuid, d2);

        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server1.getNbMXBeans());

        // Check if that device is created into the second VVR manager
        Assert.assertEquals(VoldTestHelper.MXBEANS_NUMBER_INIT + 4, server2.getNbMXBeans());
        return d2;
    }

    @Test
    public void testMultiVoldFsOps() throws Throwable {
        final File mountPoint1 = Files.createTempDirectory("mount1").toFile();
        boolean isMount1 = false;

        // Create target on server index 0, device index 0
        final UnixTarget unixTarget1 = createTarget(d1, 0, 0);
        try {
            final File mountPoint2 = Files.createTempDirectory("mount2").toFile();
            try {
                ioHelper.loginTarget(unixTarget1);

                try {
                    final File targetDevice = FsOpsTestHelper.createAndWaitTargetDevice(unixTarget1);
                    FsOpsTestHelper.partitionCreate(unixTarget1);
                    final File targetPart1 = FsOpsTestHelper.createAndWaitTargetPart1(unixTarget1);

                    final String targetPart1Str = unixTarget1.getDeviceFilePath() + unixTarget1.getDevicePart1Suffix();
                    FsOpsTestHelper.formatFileSystem(unixTarget1, targetPart1Str);
                    FsOpsTestHelper.sync(unixTarget1);
                    final UnixMount unixMount1 = FsOpsTestHelper.mountDiscOnTarget(unixTarget1, targetPart1Str,
                            mountPoint1, null);
                    isMount1 = true;

                    try {
                        final File tmpFile = ioHelper.writeTmpFile();
                        try {
                            FsOpsTestHelper.writeFileOnTarget(unixTarget1, tmpFile, mountPoint1);
                            unixMount1.umount();
                            isMount1 = false;
                            ioHelper.logoutTarget(unixTarget1);

                            // Deactivate device on node 1
                            setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);

                            // Set the devices RO
                            setDeviceRO(server1, voldTestHelper1, vvrUuid, d1);
                            setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                    getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));

                            ioHelper.loginTarget(unixTarget1);

                            // Need to wait for the device to be created (udev)
                            FsOpsTestHelper.waitForFile(targetDevice);
                            FsOpsTestHelper.waitForFile(targetPart1);

                            // mount & umount
                            unixMount1.mount();
                            isMount1 = true;
                            Assert.assertTrue(new File(mountPoint1, "lost+found").isDirectory());

                            // Read/compare file again
                            FsOpsTestHelper.compareFileOnTarget(unixTarget1, tmpFile, mountPoint1);
                            unixMount1.umount();
                            isMount1 = false;

                            // Check file on server2
                            final UnixTarget target2 = createTarget(getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2),
                                    1, 1);
                            checkFileOnOtherServer(target2, mountPoint2, tmpFile);
                        }
                        finally {
                            tmpFile.delete();
                        }
                    }
                    finally {
                        if (isMount1) {
                            unixMount1.umount();
                            isMount1 = false;
                        }
                    }
                }
                finally {
                    ioHelper.logoutTarget(unixTarget1);
                }
            }
            finally {
                mountPoint2.delete();
            }
        }
        finally {
            mountPoint1.delete();
        }
    }

    /**
     * Compare files on a given server with a given device.
     * 
     * @param device
     *            the device on which the files are compared
     * @param server
     *            the server on which the connection is done
     * @param mountPoint
     *            the directory used as mount point
     * @param fileToCompare
     *            the file to compare
     * @throws Throwable
     *             if something goes wrong
     */
    private final void checkFileOnOtherServer(final UnixTarget unixTarget, final File mountPoint,
            final File fileToCompare) throws Throwable {

        // Login on target
        ioHelper.loginTarget(unixTarget);
        try {
            FsOpsTestHelper.createAndWaitTargetDevice(unixTarget);
            FsOpsTestHelper.createAndWaitTargetPart1(unixTarget);

            final String targetPart1Str = unixTarget.getDeviceFilePath() + unixTarget.getDevicePart1Suffix();

            // Mount target
            final UnixMount unixMount = FsOpsTestHelper.mountDiscOnTarget(unixTarget, targetPart1Str, mountPoint, null);
            try {
                // Read/compare file again
                FsOpsTestHelper.compareFileOnTarget(unixTarget, fileToCompare, mountPoint);
            }
            finally {
                unixMount.umount();
            }
        }
        finally {
            ioHelper.logoutTarget(unixTarget);
        }
    }

    @Test
    public void testWriteReadOneDeviceTwoServers() throws Throwable {

        final DeviceMXBean d2 = createAnotherDevice();

        // Activate it on server 2
        setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d2);
        setDeviceRW(server2, voldTestHelper2, vvrUuid, getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2));

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {

            // Create a new target on the server 1 on the device index 0
            final UnixTarget unixTarget1 = createTarget(d1, 0, 0);
            // Create a new target on the server 2 on the device index 1
            final UnixTarget unixTarget2 = createTarget(getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2), 1, 1);

            // Write and read on each target
            final Future<File> future1 = ioHelper.multiThreadRW(executor, unixTarget1);
            final Future<File> future2 = ioHelper.multiThreadRW(executor, unixTarget2);

            final File file1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File file2 = future2.get(1, TimeUnit.MINUTES);
                setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
                setDeviceDeActivated(server2, voldTestHelper2, vvrUuid,
                        getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2));
                try {
                    // Check file1 on server2
                    final File mountPoint2 = Files.createTempDirectory("mount2").toFile();
                    try {
                        // Server index 1 and use the first free device index (0)
                        setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));
                        final UnixTarget target2 = createTarget(getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2), 1,
                                0);
                        checkFileOnOtherServer(target2, mountPoint2, file1);
                    }
                    finally {
                        mountPoint2.delete();
                    }
                    // Check file2 on server1
                    final File mountPoint1 = Files.createTempDirectory("mount1").toFile();
                    try {
                        // Server index 0 and use the first free device index (0)
                        setDeviceRO(server1, voldTestHelper1, vvrUuid, d2);
                        final UnixTarget target1 = createTarget(d2, 0, 0);
                        checkFileOnOtherServer(target1, mountPoint1, file2);
                    }
                    finally {
                        mountPoint1.delete();
                    }
                }
                finally {
                    file2.delete();
                }
            }
            finally {
                file1.delete();
            }
        }
        finally {
            executor.shutdownNow();
        }

    }

    @Test
    public void testWriteReadTwoDevices() throws Throwable {

        final DeviceMXBean d2 = createAnotherDevice();

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            // Create a new target on the server 1 on the device index 0
            final UnixTarget unixTarget1 = createTarget(d1, 0, 0);
            // Create a new target on the server 1 on the device index 1
            final UnixTarget unixTarget2 = createTarget(d2, 0, 1);

            // Write and read on each target
            final Future<File> future1 = ioHelper.multiThreadRW(executor, unixTarget1);
            final Future<File> future2 = ioHelper.multiThreadRW(executor, unixTarget2);

            final File file1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File file2 = future2.get(1, TimeUnit.MINUTES);
                setDeviceDeActivated(server1, voldTestHelper1, vvrUuid, d1);
                setDeviceDeActivated(server2, voldTestHelper2, vvrUuid, d2);
                try {
                    // Check file1 and file2 on server2
                    final File mountPoint = Files.createTempDirectory("mount2").toFile();
                    try {
                        // Use the first free device index
                        setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2));
                        UnixTarget target = createTarget(getDeviceMXBeanOnOtherServer(vvrUuid, d1, server2), 1, 0);
                        checkFileOnOtherServer(target, mountPoint, file1);

                        // Use the first free device index
                        setDeviceRO(server2, voldTestHelper2, vvrUuid,
                                getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2));
                        target = createTarget(getDeviceMXBeanOnOtherServer(vvrUuid, d2, server2), 1, 0);
                        checkFileOnOtherServer(target, mountPoint, file2);
                    }
                    finally {
                        mountPoint.delete();
                    }
                }
                finally {
                    file2.delete();
                }
            }
            finally {
                file1.delete();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }
}
