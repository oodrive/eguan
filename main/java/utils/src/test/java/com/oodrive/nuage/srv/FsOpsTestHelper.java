package com.oodrive.nuage.srv;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.utils.RunCmdUtils;
import com.oodrive.nuage.utils.unix.UnixMount;
import com.oodrive.nuage.utils.unix.UnixNbdTarget;
import com.oodrive.nuage.utils.unix.UnixTarget;

public class FsOpsTestHelper extends AbstractIopsTestHelper {

    public FsOpsTestHelper(final int blockSize, final int numBlocks, final int length) {
        super(blockSize, numBlocks, length);
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(FsOpsTestHelper.class);

    final class SafetyBeltTask extends TimerTask {
        private final UnixTarget unixTarget;
        private boolean cancelled;
        Throwable runThrowable;

        public SafetyBeltTask(final UnixTarget unixTarget) {
            this.unixTarget = unixTarget;
            cancelled = false;
        }

        @Override
        public final void run() {
            try {
                this.unixTarget.logout();
                LOGGER.warn("Logout forced for " + this.unixTarget);
            }
            catch (final Throwable t) {
                this.runThrowable = t;
            }
        }

    }

    private static String FS_TYPE = "ext4";

    private final Map<UnixTarget, SafetyBeltTask> safetyBelts = new HashMap<UnixTarget, SafetyBeltTask>();

    private final void createSafetyBelt(final UnixTarget unixTarget) {
        final SafetyBeltTask safetyBeltTask = new SafetyBeltTask(unixTarget);
        safetyBelts.put(unixTarget, safetyBeltTask);
        final Timer timer = new Timer("FailSafe");
        timer.schedule(safetyBeltTask, 240 * 1000); // Force logout after 240s
    }

    private final void cancelSafetyBelt(final UnixTarget unixTarget) {
        final SafetyBeltTask safetyBeltTask = safetyBelts.get(unixTarget);
        if (safetyBeltTask != null) {
            // Cancel logout
            safetyBeltTask.cancel();
            safetyBeltTask.cancelled = true;
        }
    }

    private final void checkExceptionSafetyBelt(final UnixTarget unixTarget) throws Throwable {
        final SafetyBeltTask safetyBeltTask = safetyBelts.get(unixTarget);
        if (safetyBeltTask != null) {
            // Throws safety belt exception if any
            if (!safetyBeltTask.cancelled && (safetyBeltTask.runThrowable != null)) {
                safetyBelts.remove(unixTarget);
                throw safetyBeltTask.runThrowable;
            }
            safetyBelts.remove(unixTarget);
        }
    }

    /**
     * Login on a target
     * 
     * @throws IOException
     *             if login failed
     */
    public void loginTarget(final UnixTarget unixTarget) throws IOException {
        unixTarget.login();
        createSafetyBelt(unixTarget);
    }

    /**
     * Logout on a target
     * 
     * @throws Throwable
     *             if logout failed or if safety belt has thrown an exception
     */
    public void logoutTarget(final UnixTarget unixTarget) throws Throwable {
        unixTarget.logout();
        cancelSafetyBelt(unixTarget);
        checkExceptionSafetyBelt(unixTarget);
    }

    /**
     * Write a file with random values, size = blockSize * numBlocks * length.
     * 
     * @return The file which has been created.
     */
    public final File writeTmpFile() throws IOException {

        final ByteBuffer writeData = ByteBuffer.allocate(blockSize * numBlocks);
        final Random random = new Random(System.currentTimeMillis());

        final File dataDump = File.createTempFile("test", "vold");

        try (final FileOutputStream fos = new FileOutputStream(dataDump.getAbsolutePath())) {
            final FileChannel out = fos.getChannel();
            for (int i = 0; i < length; i++) {
                random.nextBytes(writeData.array());
                out.write(writeData);
                writeData.rewind();
            }
        }
        return dataDump;
    }

    /**
     * Wait for a file to be created
     * 
     * @param file
     *            the waited file
     */
    public static void waitForFile(final File file) {
        Assert.assertTrue(com.oodrive.nuage.utils.Files.waitForFile(file, 60 * 1000));
    }

    /**
     * construct file and wait for the -part1 to be created
     * 
     * @param unixTarget
     *            target
     * @return the new file created
     */
    public static File createAndWaitTargetPart1(final UnixTarget unixTarget) {
        // Need to wait for the -part1 to be created (udev)
        final String targetPart1Str = unixTarget.getDeviceFilePath() + unixTarget.getDevicePart1Suffix();
        final File targetPart1 = new File(targetPart1Str);
        waitForFile(targetPart1);
        return targetPart1;
    }

    /**
     * construct file and wait for the device to be created
     * 
     * @param unixTarget
     *            iscsi target
     * 
     * @return the new file created
     */
    public static File createAndWaitTargetDevice(final UnixTarget unixTarget) {
        // Need to wait for the device to be created (udev)
        final File targetDevice = new File(unixTarget.getDeviceFilePath());
        waitForFile(targetDevice);
        return targetDevice;
    }

    /**
     * create a partition on the target with fdisk command
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void partitionCreate(final UnixTarget unixTarget) throws IOException {
        // fdisk
        final String[] fdisk = new String[] { "sudo", "fdisk", unixTarget.getDeviceFilePath() };
        RunCmdUtils.runCmd(fdisk, unixTarget, "n \n p \n 1 \n \n \n w\n q\n", new String[] { "LANG", "C", "LANGUAGE",
                "C" });
    }

    /**
     * delete and create a new partition on the target with fdisk command
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void partitionUpdate(final UnixTarget unixTarget) throws IOException {
        // fdisk
        final String[] fdisk = new String[] { "sudo", "fdisk", unixTarget.getDeviceFilePath() };
        RunCmdUtils.runCmd(fdisk, unixTarget, "d \n n \n p \n 1 \n \n \n w\n q\n", new String[] { "LANG", "C",
                "LANGUAGE", "C" });
    }

    /**
     * Format the filesystem on the target
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void formatFileSystem(final UnixTarget unixTarget, final String targetStr) throws IOException {
        final String fs = FS_TYPE;
        // mkfs
        final String[] mkfs = new String[] { "sudo", "/sbin/mkfs", "-t", fs, targetStr };
        RunCmdUtils.runCmd(mkfs, unixTarget, "y\n", new String[] { "LANG", "C", "LANGUAGE", "C" });
    }

    /**
     * wait for flush of IOs
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void sync(final UnixTarget unixTarget) throws IOException {
        // sync: wait for flush of IOs
        final String[] sync = new String[] { "sudo", "/bin/sync" };
        RunCmdUtils.runCmd(sync, unixTarget, true);
    }

    /**
     * Check the file system
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void fsCheck(final UnixTarget unixTarget) throws IOException {
        // sync: wait for flush of IOs
        final String targetPart1Str = unixTarget.getDeviceFilePath() + unixTarget.getDevicePart1Suffix();
        final String[] sync = new String[] { "sudo", "e2fsck", "-f", "-p", targetPart1Str };
        RunCmdUtils.runCmd(sync, unixTarget, true, true);
    }

    /**
     * Resize the file system
     * 
     * @param unixTarget
     *            The target.
     * 
     */
    public static void fsResize(final UnixTarget unixTarget, final String targetStr) throws IOException {
        // sync: wait for flush of IOs
        final String[] sync = new String[] { "sudo", "resize2fs", targetStr };
        RunCmdUtils.runCmd(sync, unixTarget, true);
    }

    /**
     * mount the new remote disk
     * 
     * @param unixTarget
     *            The target.
     * @param mountPoint
     *            mountPoint
     * 
     * @return the new file created from the mount point
     */
    public static UnixMount mountDiscOnTarget(final UnixTarget unixTarget, final String targetStr,
            final File mountPoint, final String option) throws IOException {
        final String fs = FS_TYPE;
        // mount
        final UnixMount unixMount = new UnixMount(mountPoint, targetStr, option, fs);
        unixMount.mount();

        // Directory lost+found of ext partitions
        Assert.assertTrue(new File(mountPoint, "lost+found").isDirectory());
        return unixMount;
    }

    /**
     * Compare a file with a another file on the target
     * 
     * @param unixTarget
     *            the target
     * @param File
     *            the file to compare
     * @param mountPoint
     *            the mount point directory
     * @throws IOException
     *             if the two files are different
     * 
     */
    public static void compareFileOnTarget(final UnixTarget unixTarget, final File file, final File mountPoint)
            throws IOException {
        final String[] cmp = new String[] { "sudo", "cmp", file.getAbsolutePath(),
                new File(mountPoint, file.getName()).getAbsolutePath() };
        RunCmdUtils.runCmd(cmp, unixTarget, true);
    }

    /**
     * Write a file on the target and compare the two files
     * 
     * @param unixTarget
     *            the target
     * @param file
     *            the file which must be copied
     * @param mountPoint
     *            the mount point on which the file must be copied
     * 
     * @throws IOException
     *             if the two files are different
     */
    public static void writeFileOnTarget(final UnixTarget unixTarget, final File file, final File mountPoint)
            throws IOException {
        final String[] cp = new String[] { "sudo", "cp", file.getAbsolutePath(), mountPoint.getAbsolutePath() };
        RunCmdUtils.runCmd(cp, unixTarget, true);

        compareFileOnTarget(unixTarget, file, mountPoint);
    }

    /**
     * Remove a file on the target.
     * 
     * @param unixTarget
     *            the target
     * @param file
     *            the file which must be removed
     * @param mountPoint
     *            the mount point on which the file must be removed
     * 
     * @throws IOException
     *             if the two files are different
     */
    public static void removeFileOnTarget(final UnixTarget unixTarget, final File file, final File mountPoint)
            throws IOException {
        final String[] rm = new String[] { "sudo", "rm", new File(mountPoint, file.getName()).getAbsolutePath() };
        RunCmdUtils.runCmd(rm, unixTarget, true);
    }

    /**
     * Get current kernel version.
     * 
     * @param unixTarget
     * 
     * @return the kernel version
     * 
     * @throws IOException
     */
    public static StringBuilder getKernelVersion(final UnixTarget unixTarget) throws IOException {
        final String[] rm = new String[] { "uname", "-r" };
        return RunCmdUtils.runCmd(rm, unixTarget, true);
    }

    /**
     * Write some data on a dev file.
     * 
     * @param unixTarget
     *            the target
     * @param size
     *            the number of bytes to write
     * @param count
     * 
     * @return
     * @throws IOException
     */
    public static StringBuilder writeData(final UnixTarget unixTarget, final int size, final int count)
            throws IOException {
        final String[] dd = new String[] { "sudo", "dd", "if=/dev/zero", "of=" + unixTarget.getDeviceFilePath(),
                "bs=" + size, "count=" + count, "oflag=sync" };
        return RunCmdUtils.runCmd(dd, unixTarget, true, true);
    }

    /**
     * Test to read/write a file on a target.
     * 
     * @param unixTarget
     *            the target
     * @throws Throwable
     */
    public File testReadWriteFile(final UnixTarget unixTarget) throws Throwable {

        final File mountPoint = Files.createTempDirectory("mount").toFile();
        try {
            loginTarget(unixTarget);

            try {
                final File targetDevice = FsOpsTestHelper.createAndWaitTargetDevice(unixTarget);

                partitionCreate(unixTarget);

                final File targetPart1 = createAndWaitTargetPart1(unixTarget);
                final String targetPart1Str = unixTarget.getDeviceFilePath() + unixTarget.getDevicePart1Suffix();
                formatFileSystem(unixTarget, targetPart1Str);
                sync(unixTarget);
                final UnixMount unixMount = mountDiscOnTarget(unixTarget, targetPart1Str, mountPoint, null);
                boolean isMount = true;
                try {
                    // Create temp file, cp file, read/compare file
                    final File tmpFile = writeTmpFile();
                    try {
                        writeFileOnTarget(unixTarget, tmpFile, mountPoint);

                        // umount
                        unixMount.umount();
                        isMount = false;

                        unixTarget.logout();
                        unixTarget.login();

                        // Need to wait for the device to be created (udev)
                        waitForFile(targetDevice);
                        waitForFile(targetPart1);

                        // mount & umount
                        unixMount.mount();
                        isMount = true;

                        Assert.assertTrue(new File(mountPoint, "lost+found").isDirectory());

                        // Read/compare file again
                        compareFileOnTarget(unixTarget, tmpFile, mountPoint);
                        return tmpFile;
                    }
                    catch (final Throwable t) {
                        tmpFile.delete();
                        throw t;
                    }
                }
                finally {
                    if (isMount) {
                        unixMount.umount();
                    }
                }
            }
            finally {
                logoutTarget(unixTarget);
            }
        }
        finally {
            mountPoint.delete();
        }
    }

    /**
     * Test trim command.
     * 
     * @param unixNbdTarget
     *            the target
     * @param checkSrvCmd
     *            interface to check commands
     * @throws Throwable
     */
    public void testNbdTrim(final UnixNbdTarget unixNbdTarget, final CheckSrvCommand checkSrvCmd) throws Throwable {

        final File mountPoint = Files.createTempDirectory("mount").toFile();
        try {
            loginTarget(unixNbdTarget);
            try {

                createAndWaitTargetDevice(unixNbdTarget);
                partitionCreate(unixNbdTarget);
                createAndWaitTargetPart1(unixNbdTarget);

                final String targetPart1Str = unixNbdTarget.getDeviceFilePath() + unixNbdTarget.getDevicePart1Suffix();
                formatFileSystem(unixNbdTarget, targetPart1Str);
                sync(unixNbdTarget);

                final UnixMount unixMount = FsOpsTestHelper.mountDiscOnTarget(unixNbdTarget, targetPart1Str,
                        mountPoint, "data=journal,discard");

                try {
                    if (checkSrvCmd != null) {
                        checkSrvCmd.checkTrim(unixNbdTarget, 0, false);
                    }
                    // Create temp file, cp file, read/compare file
                    final File tmpFile = writeTmpFile();
                    try {
                        writeFileOnTarget(unixNbdTarget, tmpFile, mountPoint);
                        sync(unixNbdTarget);
                        // A second sync is necessary to flush all the data
                        sync(unixNbdTarget);
                        removeFileOnTarget(unixNbdTarget, tmpFile, mountPoint);
                        sync(unixNbdTarget);

                        if (checkSrvCmd != null) {
                            checkSrvCmd.checkTrim(unixNbdTarget, blockSize * numBlocks * length, true);
                        }
                    }
                    finally {
                        tmpFile.delete();
                    }
                }
                finally {
                    unixMount.umount();
                }
            }
            finally {
                logoutTarget(unixNbdTarget);
            }
        }
        finally {
            mountPoint.delete();
        }
    }

    /**
     * R/W multi thread method.
     * 
     * @param executor
     * @param unixTarget
     * 
     * @return the Future to wait the result
     */
    public Future<File> multiThreadRW(final ExecutorService executor, final UnixTarget unixTarget) {

        final Future<File> future = executor.submit(new Callable<File>() {
            @Override
            public File call() throws Exception {
                try {
                    return testReadWriteFile(unixTarget);
                }
                catch (final Throwable t) {
                    throw new Exception(t);
                }
            }
        });
        return future;
    }
}
