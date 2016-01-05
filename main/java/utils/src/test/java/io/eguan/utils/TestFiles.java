package io.eguan.utils;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.eguan.utils.RunCmdUtils;
import io.eguan.utils.unix.UnixFsFile;
import io.eguan.utils.unix.UnixMount;
import io.eguan.utils.unix.UnixUser;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.AssertionFailedError;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the class {@link Files}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class TestFiles {

    private static final String TEMP_PREFIX = "files-";
    private static final String TEMP_SUFFIX = ".tmp";

    /*
     * Recursive delete.
     */

    /**
     * Delete success.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursive() throws IOException {
        // Create file tree
        final Path tempD1 = Files.createTempDirectory(TEMP_PREFIX);
        final Path tempF1 = Files.createTempFile(tempD1, TEMP_PREFIX, TEMP_SUFFIX);
        final Path tempD2 = Files.createTempDirectory(tempD1, TEMP_PREFIX);
        final Path tempF21 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
        final Path tempF22 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);

        // Create various links
        final Path tempF23 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(Files.deleteIfExists(tempF23));
        final Path tempL21 = Files.createSymbolicLink(new File(tempD2.toFile(), "l1").toPath(), tempF22);
        final Path tempL22 = Files.createSymbolicLink(new File(tempD2.toFile(), "l2").toPath(), tempF23.getFileName());
        final Path tempL23 = Files.createLink(new File(tempD2.toFile(), "l3").toPath(), tempF22);

        // Delete and check
        io.eguan.utils.Files.deleteRecursive(tempD1);
        Assert.assertFalse(tempD1.toFile().exists());
        Assert.assertFalse(tempF1.toFile().exists());
        Assert.assertFalse(tempD2.toFile().exists());
        Assert.assertFalse(tempF21.toFile().exists());
        Assert.assertFalse(tempF22.toFile().exists());
        Assert.assertFalse(tempF23.toFile().exists());
        Assert.assertFalse(tempL21.toFile().exists());
        Assert.assertFalse(tempL22.toFile().exists());
        Assert.assertFalse(tempL23.toFile().exists());
    }

    /**
     * Delete failure.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveFailure() throws IOException {
        // Create file tree
        final Path tempD1 = Files.createTempDirectory(TEMP_PREFIX);
        final Path tempF1 = Files.createTempFile(tempD1, TEMP_PREFIX, TEMP_SUFFIX);
        final Path tempD2 = Files.createTempDirectory(tempD1, TEMP_PREFIX);
        final Path tempD3 = Files.createTempDirectory(tempD2, TEMP_PREFIX);
        final Path tempF31 = Files.createTempFile(tempD3, TEMP_PREFIX, TEMP_SUFFIX);

        // Change directory mode to disable delete
        final Set<PosixFilePermission> tempD3Attr = Files.getPosixFilePermissions(tempD3);
        final Set<PosixFilePermission> tempD3AttrOwnerRX = new HashSet<PosixFilePermission>();
        tempD3AttrOwnerRX.add(PosixFilePermission.OWNER_READ);
        tempD3AttrOwnerRX.add(PosixFilePermission.OWNER_EXECUTE);
        Files.setPosixFilePermissions(tempD3, tempD3AttrOwnerRX);

        // Delete and fail
        // 2012-07-24: with the JVM 'OpenJDK Runtime Environment (IcedTea6 1.11.3) (6b24-1.11.3-1ubuntu0.12.04.1)
        // OpenJDK 64-Bit Server VM (build 20.0-b12, mixed mode)', the postVisitDirectory() is not called with
        // (exc != null) when the deleting a file in a directory fails!
        try {
            io.eguan.utils.Files.deleteRecursive(tempD1);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IOException e) {
            // Ok
            Assert.assertTrue(tempD3.toFile().exists());
        }

        // Restore default mode, delete and check
        Files.setPosixFilePermissions(tempD3, tempD3Attr);
        io.eguan.utils.Files.deleteRecursive(tempD1);
        Assert.assertFalse(tempD1.toFile().exists());
        Assert.assertFalse(tempF1.toFile().exists());
        Assert.assertFalse(tempD2.toFile().exists());
        Assert.assertFalse(tempD3.toFile().exists());
        Assert.assertFalse(tempF31.toFile().exists());
    }

    /**
     * Deletes a path that does not exist.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveNotExisting() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempFile.delete());
        io.eguan.utils.Files.deleteRecursive(tempFile.toPath());
        Assert.assertFalse(tempFile.exists());
    }

    /**
     * Recursive delete of a file.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveFile() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        io.eguan.utils.Files.deleteRecursive(tempFile.toPath());
        Assert.assertFalse(tempFile.exists());
    }

    /**
     * Recursive delete of a symbolic link.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveSymbolicLink() throws IOException {
        final File tempName = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempName.delete());
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempLink = Files.createSymbolicLink(tempName.toPath(), tempFile.toPath());
            io.eguan.utils.Files.deleteRecursive(tempLink);
            Assert.assertFalse(tempName.exists());
            Assert.assertFalse(tempLink.toFile().exists());
            Assert.assertTrue(tempFile.exists());
        }
        finally {
            tempFile.delete();
        }
    }

    /**
     * Delete the whole tree.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveProgress() throws IOException {
        deleteRecursiveProgress(false);
    }

    /**
     * Delete the whole tree, except the starting directory.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveProgressKeep() throws IOException {
        deleteRecursiveProgress(true);
    }

    private void deleteRecursiveProgress(final boolean keepPath) throws IOException {
        final Set<Path> exists = new HashSet<>();
        final Set<Path> deleted = new HashSet<>();

        // Create file tree
        final Path tempD1 = Files.createTempDirectory(TEMP_PREFIX);
        try {
            final Path tempF1 = Files.createTempFile(tempD1, TEMP_PREFIX, TEMP_SUFFIX);
            final Path tempD2 = Files.createTempDirectory(tempD1, TEMP_PREFIX);
            final Path tempF21 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
            final Path tempF22 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);

            // Create various links
            final Path tempF23 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
            Assert.assertTrue(Files.deleteIfExists(tempF23));
            final Path tempL21 = Files.createSymbolicLink(new File(tempD2.toFile(), "l1").toPath(), tempF22);
            final Path tempL22 = Files.createSymbolicLink(new File(tempD2.toFile(), "l2").toPath(),
                    tempF23.getFileName());
            final Path tempL23 = Files.createLink(new File(tempD2.toFile(), "l3").toPath(), tempF22);

            // Fill collection
            exists.add(tempD1);
            exists.add(tempF1);
            exists.add(tempD2);
            exists.add(tempF21);
            exists.add(tempF22);
            exists.add(tempL21);
            exists.add(tempL22);
            exists.add(tempL23);

            final int count = exists.size();

            // Delete and check
            io.eguan.utils.Files.deleteRecursive(tempD1, keepPath,
                    new io.eguan.utils.Files.DeleteRecursiveProgress() {

                        @Override
                        public final FileVisitResult notify(final Path path) {
                            Assert.assertTrue(exists.remove(path));
                            deleted.add(path);
                            return FileVisitResult.CONTINUE;
                        }
                    });

            Assert.assertEquals(keepPath ? 1 : 0, exists.size());
            Assert.assertEquals(count - exists.size(), deleted.size());

            Assert.assertEquals(Boolean.valueOf(keepPath), Boolean.valueOf(tempD1.toFile().exists()));
            Assert.assertFalse(tempF1.toFile().exists());
            Assert.assertFalse(tempD2.toFile().exists());
            Assert.assertFalse(tempF21.toFile().exists());
            Assert.assertFalse(tempF22.toFile().exists());
            Assert.assertFalse(tempF23.toFile().exists());
            Assert.assertFalse(tempL21.toFile().exists());
            Assert.assertFalse(tempL22.toFile().exists());
            Assert.assertFalse(tempL23.toFile().exists());
        }
        finally {
            // Cleanup remaining elements
            io.eguan.utils.Files.deleteRecursive(tempD1);
        }
    }

    /**
     * Delete partial tree.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveProgressPartial() throws IOException {
        deleteRecursiveProgressPartial(false);
    }

    /**
     * Delete partial tree.
     * 
     * @throws IOException
     */
    @Test
    public void testDeleteRecursiveProgressPartialKeep() throws IOException {
        deleteRecursiveProgressPartial(true);
    }

    private void deleteRecursiveProgressPartial(final boolean keepPath) throws IOException {
        final Set<Path> exists = new HashSet<>();
        final Set<Path> deleted = new HashSet<>();

        // Create file tree
        final Path tempD1 = Files.createTempDirectory(TEMP_PREFIX);
        try {
            final Path tempF1 = Files.createTempFile(tempD1, TEMP_PREFIX, TEMP_SUFFIX);
            final Path tempD2 = Files.createTempDirectory(tempD1, TEMP_PREFIX);
            final Path tempF21 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
            final Path tempF22 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);

            // Create various links
            final Path tempF23 = Files.createTempFile(tempD2, TEMP_PREFIX, TEMP_SUFFIX);
            Assert.assertTrue(Files.deleteIfExists(tempF23));
            final Path tempL21 = Files.createSymbolicLink(new File(tempD2.toFile(), "l1").toPath(), tempF22);
            final Path tempL22 = Files.createSymbolicLink(new File(tempD2.toFile(), "l2").toPath(),
                    tempF23.getFileName());
            final Path tempL23 = Files.createLink(new File(tempD2.toFile(), "l3").toPath(), tempF22);

            // Fill collection
            exists.add(tempD1);
            exists.add(tempF1);
            exists.add(tempD2);
            exists.add(tempF21);
            exists.add(tempF22);
            exists.add(tempL21);
            exists.add(tempL22);
            exists.add(tempL23);

            final int count = exists.size();
            final int delCount = count / 2;
            final AtomicInteger toDel = new AtomicInteger(delCount);

            // Partial delete and check
            io.eguan.utils.Files.deleteRecursive(tempD1, keepPath,
                    new io.eguan.utils.Files.DeleteRecursiveProgress() {

                        @Override
                        public final FileVisitResult notify(final Path path) {
                            Assert.assertTrue(exists.remove(path));
                            deleted.add(path);
                            return toDel.decrementAndGet() > 0 ? FileVisitResult.CONTINUE : FileVisitResult.TERMINATE;
                        }
                    });

            Assert.assertEquals(count - delCount, exists.size());
            Assert.assertEquals(delCount, deleted.size());
            for (final Path path : exists) {
                Assert.assertTrue(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
            }
            for (final Path path : deleted) {
                Assert.assertFalse(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
            }
        }
        finally {
            // Cleanup remaining elements
            io.eguan.utils.Files.deleteRecursive(tempD1);
        }
    }

    @Test
    public void testDeleteRecursiveProgressNotExisting() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempFile.delete());
        io.eguan.utils.Files.deleteRecursive(tempFile.toPath(), false,
                new io.eguan.utils.Files.DeleteRecursiveProgress() {
                    @Override
                    public final FileVisitResult notify(final Path path) {
                        return FileVisitResult.CONTINUE;
                    }
                });
        Assert.assertFalse(tempFile.exists());
    }

    @Test
    public void testDeleteRecursiveProgressFile() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        deleteRecursiveProgressSingle(tempFile.toPath(), false, true);
    }

    @Test
    public void testDeleteRecursiveProgressFileKeep() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        deleteRecursiveProgressSingle(tempFile.toPath(), true, true);
    }

    @Test
    public void testDeleteRecursiveSymbolicLinkProgress() throws IOException {
        final File tempName = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempName.delete());
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempLink = Files.createSymbolicLink(tempName.toPath(), tempFile.toPath());
            deleteRecursiveProgressSingle(tempLink, false, false);
            Assert.assertFalse(tempName.exists());
            Assert.assertFalse(tempLink.toFile().exists());
            Assert.assertTrue(tempFile.exists());
        }
        finally {
            tempFile.delete();
        }
    }

    @Test
    public void testDeleteRecursiveSymbolicLinkProgressKeep() throws IOException {
        final File tempName = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempName.delete());
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            final Path tempLink = Files.createSymbolicLink(tempName.toPath(), tempFile.toPath());
            try {
                deleteRecursiveProgressSingle(tempLink, true, false);
                Assert.assertTrue(tempName.exists());
                Assert.assertTrue(tempLink.toFile().exists());
                Assert.assertTrue(tempFile.exists());
            }
            finally {
                tempLink.toFile().delete();
            }
        }
        finally {
            tempFile.delete();
        }
    }

    @Test
    public void testDeleteRecursiveSymbolicLinkOnDirProgress() throws IOException {
        final File tempName = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempName.delete());
        final Path tempDir = Files.createTempDirectory(TEMP_PREFIX);
        try {
            final Path tempLink = Files.createSymbolicLink(tempName.toPath(), tempDir);
            deleteRecursiveProgressSingle(tempLink, false, false);
            Assert.assertFalse(tempName.exists());
            Assert.assertFalse(tempLink.toFile().exists());
            Assert.assertTrue(tempDir.toFile().exists());
        }
        finally {
            tempDir.toFile().delete();
        }
    }

    @Test
    public void testDeleteRecursiveSymbolicLinkOnDirProgressKeep() throws IOException {
        final File tempName = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        Assert.assertTrue(tempName.delete());
        final Path tempDir = Files.createTempDirectory(TEMP_PREFIX);
        try {
            final Path tempLink = Files.createSymbolicLink(tempName.toPath(), tempDir);
            try {
                deleteRecursiveProgressSingle(tempLink, true, false);
                Assert.assertTrue(tempName.exists());
                Assert.assertTrue(Files.isSymbolicLink(tempLink));
                Assert.assertTrue(Files.isDirectory(tempDir));
            }
            finally {
                tempLink.toFile().delete();
            }
        }
        finally {
            tempDir.toFile().delete();
        }
    }

    private void deleteRecursiveProgressSingle(final Path toDel, final boolean keepPath, final boolean cleanup)
            throws IOException {

        try {
            // Delete and check
            io.eguan.utils.Files.deleteRecursive(toDel, keepPath,
                    new io.eguan.utils.Files.DeleteRecursiveProgress() {

                        @Override
                        public final FileVisitResult notify(final Path path) {
                            return FileVisitResult.CONTINUE;
                        }
                    });

            Assert.assertEquals(Boolean.valueOf(keepPath), Boolean.valueOf(toDel.toFile().exists()));
        }
        finally {
            // Cleanup remaining elements
            if (cleanup)
                io.eguan.utils.Files.deleteRecursive(toDel);
        }
    }

    /**
     * Test file existence.<br>
     * TODO add test with file created in another thread
     * 
     * @throws IOException
     */
    @Test
    public void testFileExistsTimeout() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            Assert.assertTrue(io.eguan.utils.Files.waitForFile(tempFile, 100));
        }
        finally {
            Assert.assertTrue(tempFile.delete());
        }
        Assert.assertFalse(io.eguan.utils.Files.waitForFile(tempFile, 100));
    }

    /**
     * Test file nonexistence.<br>
     * TODO add test with file created in another thread
     * 
     * @throws IOException
     */
    @Test
    public void testFileNotExistsTimeout() throws IOException {
        final File tempFile = File.createTempFile(TEMP_PREFIX, TEMP_SUFFIX);
        try {
            Assert.assertFalse(io.eguan.utils.Files.waitForFileDeletion(tempFile, 100));
        }
        finally {
            Assert.assertTrue(tempFile.delete());
        }
        Assert.assertTrue(io.eguan.utils.Files.waitForFileDeletion(tempFile, 100));
    }

    /*
     * User-defined attributes
     */

    private static File ext2fs;
    private static File vfatfs; // vfat does not support user defined attributes
    private static File mountPoint;

    @BeforeClass
    public static void createFileFs() throws IOException {
        ext2fs = File.createTempFile("tst-fs-", ".ext2");
        UnixFsFile fsFile = new UnixFsFile(ext2fs, 20 * 1024 * 1024, "ext2", null);
        fsFile.create();

        vfatfs = File.createTempFile("tst-fs-", ".vfat");
        fsFile = new UnixFsFile(vfatfs, 20 * 1024 * 1024, "vfat", null);
        fsFile.create();

        mountPoint = Files.createTempDirectory("tst-mnt-").toFile();
    }

    @AfterClass
    public static void deleteFileFs() {
        if (ext2fs != null) {
            ext2fs.delete();
            ext2fs = null;
        }
        if (vfatfs != null) {
            vfatfs.delete();
            vfatfs = null;
        }
        if (mountPoint != null) {
            mountPoint.delete();
            mountPoint = null;
        }
    }

    @Test
    public void userAttrExt2() throws IOException {
        final File aa;
        final String ATTR1 = "MyAttr";
        final String ATTR2 = "SomeLongAttribute,But not that long...";
        final String ATTR3 = "NeverSet";
        final String VALUE2 = "SomeLong value, with some special chars: \u20AC\u00E7\u00E0 ~ &\u00EF";

        { // Create file and check attr supported
            final UnixFsFile fsFile = new UnixFsFile(ext2fs, 20 * 1024 * 1024, "ext2", null);

            final UnixMount unixMount = fsFile.newUnixMount(mountPoint);
            unixMount.mount();
            try {
                // Create a file
                aa = new File(mountPoint, "all-access");
                final String aaAbsPath = aa.getAbsolutePath();
                final String[] touchArray = new String[] { "sudo", "touch", aaAbsPath };
                RunCmdUtils.runCmd(touchArray, aa);
                final String[] chmodArray = new String[] { "sudo", "chmod", "777", aaAbsPath };
                RunCmdUtils.runCmd(chmodArray, aa);

                assertTrue(aa.isFile());
                final Path aaPath = aa.toPath();

                Assert.assertEquals(0, io.eguan.utils.Files.listUserAttr(aaPath).length);
                io.eguan.utils.Files.checkUserAttrSupported(aaPath);

                // Set an attribute
                io.eguan.utils.Files.setUserAttr(aaPath, ATTR1);
                assertTrue(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR1));
                Assert.assertEquals("y", io.eguan.utils.Files.getUserAttr(aaPath, ATTR1));

                io.eguan.utils.Files.setUserAttr(aaPath, ATTR2, "");
                assertTrue(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR2));
                Assert.assertEquals("", io.eguan.utils.Files.getUserAttr(aaPath, ATTR2));

                io.eguan.utils.Files.setUserAttr(aaPath, ATTR2, VALUE2);
                assertTrue(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR2));
                Assert.assertEquals(VALUE2, io.eguan.utils.Files.getUserAttr(aaPath, ATTR2));

                Assert.assertEquals(2, io.eguan.utils.Files.listUserAttr(aaPath).length);
                io.eguan.utils.Files.unsetUserAttr(aaPath, ATTR1);
                Assert.assertEquals(1, io.eguan.utils.Files.listUserAttr(aaPath).length);
                Assert.assertArrayEquals(new String[] { ATTR2 }, io.eguan.utils.Files.listUserAttr(aaPath));

                // Does nothing if the attribute is not set
                io.eguan.utils.Files.unsetUserAttr(aaPath, ATTR1);

                assertFalse(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR1));
                assertTrue(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR2));
                assertFalse(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR3));
                Assert.assertNull(io.eguan.utils.Files.getUserAttr(aaPath, ATTR3));
            }
            finally {
                unixMount.umount();
            }
        }
        { // Mount FS read-only and check attr not supported
            final UnixFsFile fsFile = new UnixFsFile(ext2fs, 20 * 1024 * 1024, "ext2", "ro");

            final UnixMount unixMount = fsFile.newUnixMount(mountPoint);
            unixMount.mount();
            final Path aaPath = aa.toPath();
            try {
                // Can not set attr
                assertTrue(aa.isFile());
                try {
                    io.eguan.utils.Files.checkUserAttrSupported(aaPath);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // Ok
                }

                // Should be able to read attr
                assertFalse(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR1));
                assertTrue(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR2));
                assertFalse(io.eguan.utils.Files.isUserAttrSet(aaPath, ATTR3));
                Assert.assertNull(io.eguan.utils.Files.getUserAttr(aaPath, ATTR1));
                Assert.assertEquals(VALUE2, io.eguan.utils.Files.getUserAttr(aaPath, ATTR2));
                Assert.assertNull(io.eguan.utils.Files.getUserAttr(aaPath, ATTR3));

                Assert.assertEquals(1, io.eguan.utils.Files.listUserAttr(aaPath).length);
                Assert.assertArrayEquals(new String[] { ATTR2 }, io.eguan.utils.Files.listUserAttr(aaPath));

                // Can not delete
                try {
                    io.eguan.utils.Files.unsetUserAttr(aaPath, ATTR2);
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // Ok
                }
                // Can not set
                try {
                    io.eguan.utils.Files.setUserAttr(aaPath, ATTR3, "");
                    throw new AssertionFailedError("Should not be reached");
                }
                catch (final IOException e) {
                    // Ok
                }

                Assert.assertEquals(VALUE2, io.eguan.utils.Files.getUserAttr(aaPath, ATTR2));
                Assert.assertNull(io.eguan.utils.Files.getUserAttr(aaPath, ATTR3));
            }
            finally {
                unixMount.umount();
            }
        }
    }

    @Test(expected = IOException.class)
    public void userAttrExt2NoAccess() throws IOException {
        final UnixFsFile fsFile = new UnixFsFile(ext2fs, 20 * 1024 * 1024, "ext2", null);

        final UnixMount unixMount = fsFile.newUnixMount(mountPoint);
        unixMount.mount();
        try {
            // Must have a lost+found directory: current user has no access
            final File lf = new File(mountPoint, "lost+found");
            assertTrue(lf.isDirectory());
            io.eguan.utils.Files.checkUserAttrSupported(lf.toPath());
        }
        finally {
            unixMount.umount();
        }
    }

    @Test
    public void userAttrVfat() throws IOException {
        final UnixUser user = UnixUser.getCurrentUser();
        final UnixFsFile fsFile = new UnixFsFile(vfatfs, 20 * 1024 * 1024, "vfat", "utf8,uid=" + user.getUid()
                + ",gid=" + user.getGid());

        final UnixMount unixMount = fsFile.newUnixMount(mountPoint);
        unixMount.mount();
        try {
            // Check we can create a file
            final File tf = new File(mountPoint, "test-file");
            assertTrue(tf.createNewFile());
            assertTrue(tf.isFile());

            try {
                io.eguan.utils.Files.checkUserAttrSupported(tf.toPath());
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final IOException e) {
                // Ok
            }
        }
        finally {
            unixMount.umount();
        }
    }
}
