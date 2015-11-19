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

import io.eguan.hash.HashAlgorithm;
import io.eguan.srv.FsOpsTestHelper;
import io.eguan.utils.RunCmdErrorException;
import io.eguan.utils.unix.UnixMount;
import io.eguan.utils.unix.UnixTarget;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.VoldTestHelper.CompressionType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public abstract class TestVoldFsOpsOnTargetAbstract extends AbstractVoldIopsOnTarget {

    protected FsOpsTestHelper fsOpsHelper;

    public TestVoldFsOpsOnTargetAbstract(final CompressionType compressionType, final HashAlgorithm hash,
            final Integer blockSize, final Integer numBlocks) throws Exception {
        super(compressionType, hash);
        fsOpsHelper = new FsOpsTestHelper(blockSize.intValue(), numBlocks.intValue(), LENGTH);
    }

    @Override
    public int getBlockSize() {
        return fsOpsHelper.getBlockSize();
    }

    /**
     * Create a new Unix Target
     * 
     * @return the target
     * 
     * @throws IOException
     */
    protected abstract UnixTarget createTarget(final DeviceMXBean device, final int number) throws IOException;

    /**
     * Update the target
     * 
     * @param target
     *            the target to update
     * @throws Throwable
     */
    protected abstract void updateTarget(final UnixTarget target) throws Throwable;

    @Test
    public void testVoldFsOps() throws Throwable {
        // Create the target
        final UnixTarget unixTarget = createTarget(device, 0);
        final File file = fsOpsHelper.testReadWriteFile(unixTarget);
        file.delete();
    }

    @Test(expected = RunCmdErrorException.class)
    public void testResizeDevice() throws Throwable {
        final UnixTarget unixTarget = createTarget(device, 0);

        fsOpsHelper.loginTarget(unixTarget);
        try {
            FsOpsTestHelper.createAndWaitTargetDevice(unixTarget);
            // reduce device size
            device.setSize(4096);

            // Try to write more than new size
            FsOpsTestHelper.writeData(unixTarget, 8192, 1);
        }
        finally {
            fsOpsHelper.logoutTarget(unixTarget);
        }

    }

    @Test
    public void testIncreaseDeviceSizeFsWithPartition() throws Throwable {
        final UnixTarget unixTarget = createTarget(device, 0);

        final File mountPoint = Files.createTempDirectory("mount").toFile();
        try {
            fsOpsHelper.loginTarget(unixTarget);

            try {
                FsOpsTestHelper.createAndWaitTargetDevice(unixTarget);

                FsOpsTestHelper.partitionCreate(unixTarget);

                final File targetPart1 = FsOpsTestHelper.createAndWaitTargetPart1(unixTarget);
                final String targetPart1Str = unixTarget.getDeviceFilePath() + unixTarget.getDevicePart1Suffix();
                FsOpsTestHelper.formatFileSystem(unixTarget, targetPart1Str);
                FsOpsTestHelper.sync(unixTarget);
                final UnixMount unixMount = FsOpsTestHelper.mountDiscOnTarget(unixTarget, targetPart1Str, mountPoint,
                        null);
                boolean isMount = true;
                try {
                    // Create temp file, cp file, read/compare file
                    final File tmpFile = fsOpsHelper.writeTmpFile();
                    try {
                        FsOpsTestHelper.writeFileOnTarget(unixTarget, tmpFile, mountPoint);
                        // umount
                        unixMount.umount();
                        isMount = false;

                        // Increase size in the device
                        final long size = device.getSize();
                        device.setSize(size + 4096 * 256 * 512);

                        // Update Target
                        updateTarget(unixTarget);

                        // update the partition with the new size
                        FsOpsTestHelper.partitionUpdate(unixTarget);
                        FsOpsTestHelper.waitForFile(targetPart1);

                        // Check the partition
                        FsOpsTestHelper.fsCheck(unixTarget);

                        // Resize the file system
                        FsOpsTestHelper.fsResize(unixTarget, targetPart1Str);

                        // Mount
                        unixMount.mount();
                        isMount = true;
                        Assert.assertTrue(new File(mountPoint, "lost+found").isDirectory());

                        // Read/compare file again
                        FsOpsTestHelper.compareFileOnTarget(unixTarget, tmpFile, mountPoint);

                    }
                    finally {
                        tmpFile.delete();
                    }
                }
                finally {
                    if (isMount) {
                        unixMount.umount();
                    }
                }
            }
            finally {
                fsOpsHelper.logoutTarget(unixTarget);
            }
        }
        finally {
            mountPoint.delete();
        }
    }

    @Test
    public void testReadTwoDevices() throws Exception {

        final DeviceMXBean device2 = createAnotherDevice();

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {

            final UnixTarget unixTarget1 = createTarget(device, 0);
            final UnixTarget unixTarget2 = createTarget(device2, 1);

            final Future<File> future1 = fsOpsHelper.multiThreadRW(executor, unixTarget1);
            final Future<File> future2 = fsOpsHelper.multiThreadRW(executor, unixTarget2);

            final File file1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File file2 = future2.get(1, TimeUnit.MINUTES);
                file2.delete();
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
