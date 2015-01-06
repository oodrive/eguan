package com.oodrive.nuage.nrs;

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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.oodrive.nuage.configuration.MetaConfiguration;

@RunWith(value = Parameterized.class)
public final class TestNrsFileJanitor extends TestNrsFileJanitorAbstract {

    @Parameters
    public static Collection<Object[]> testParameters() {
        final Object[][] params = new Object[][] { { Boolean.FALSE }, { Boolean.TRUE } };
        return Arrays.asList(params);
    }

    /** true if the NRS file has an associated block file */
    private final boolean blocks;

    public TestNrsFileJanitor(final boolean blocks) {
        super();
        this.blocks = blocks;
    }

    @Test
    public void testNrsFileJanitor() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);
        janitor.init();
        try {

            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);
            final int clusterSize = janitor.newNrsFileHeaderBuilder().clusterSize();

            // Check file
            Assert.assertEquals(parent, nrsFile.getDescriptor().getParentId());
            Assert.assertEquals(device, nrsFile.getDescriptor().getDeviceId());
            Assert.assertEquals(node, nrsFile.getDescriptor().getNodeId());
            Assert.assertEquals(file, nrsFile.getDescriptor().getFileId());
            Assert.assertEquals(size, nrsFile.getDescriptor().getSize());
            Assert.assertEquals(blockSize, nrsFile.getDescriptor().getBlockSize());
            Assert.assertEquals(hashSize, nrsFile.getDescriptor().getHashSize());
            Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getClusterSize());
            Assert.assertEquals(now, nrsFile.getDescriptor().getTimestamp());
            Assert.assertFalse(nrsFile.getDescriptor().isRoot());
            Assert.assertTrue(nrsFile.getDescriptor().isPartial());
            Assert.assertFalse(nrsFile.isOpened());
            Assert.assertTrue(nrsFile.isWriteable());
            Assert.assertTrue(nrsFile.getFile().toFile().isFile());
            Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getH1Address());
            Assert.assertEquals(0, nrsFile.getFile().toFile().length() % clusterSize);

            if (blocks) {
                // Compare NrsFile and NrsFileBlock: equals, except for the cluster size and the flag block
                final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                Assert.assertEquals(nrsFile.getDescriptor().getParentId(), fileBlock.getDescriptor().getParentId());
                Assert.assertEquals(nrsFile.getDescriptor().getDeviceId(), fileBlock.getDescriptor().getDeviceId());
                Assert.assertEquals(nrsFile.getDescriptor().getNodeId(), fileBlock.getDescriptor().getNodeId());
                Assert.assertEquals(nrsFile.getDescriptor().getFileId(), fileBlock.getDescriptor().getFileId());
                Assert.assertEquals(nrsFile.getDescriptor().getSize(), fileBlock.getDescriptor().getSize());
                Assert.assertEquals(nrsFile.getDescriptor().getBlockSize(), fileBlock.getDescriptor().getBlockSize());
                Assert.assertEquals(nrsFile.getDescriptor().getHashSize(), fileBlock.getDescriptor().getHashSize());
                Assert.assertFalse(nrsFile.getDescriptor().getClusterSize() == fileBlock.getDescriptor()
                        .getClusterSize());
                Assert.assertEquals(nrsFile.getDescriptor().getTimestamp(), fileBlock.getDescriptor().getTimestamp());
                Assert.assertEquals(nrsFile.getDescriptor().isRoot(), fileBlock.getDescriptor().isRoot());
                Assert.assertEquals(nrsFile.getDescriptor().isPartial(), fileBlock.getDescriptor().isPartial());
                Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                Assert.assertEquals(nrsFile.getFile().toFile().isFile(), fileBlock.getFile().toFile().isFile());

                Assert.assertEquals(fileBlock.getDescriptor().getClusterSize(), fileBlock.getDescriptor()
                        .getH1Address());
                Assert.assertEquals(0, fileBlock.getFile().toFile().length()
                        % fileBlock.getDescriptor().getClusterSize());
            }
            else {
                Assert.assertNull(nrsFile.getFileBlock());
            }

            NrsFile lastInstance = nrsFile;

            // Load entity from UUID
            {
                final NrsFile nrsFile2 = janitor.loadNrsFile(file);
                Assert.assertSame(lastInstance, nrsFile2);
                janitor.clearCache();

                final NrsFile nrsFile3 = janitor.loadNrsFile(file);
                Assert.assertNotSame(nrsFile, nrsFile3);
                lastInstance = nrsFile3;

                Assert.assertEquals(parent, nrsFile3.getDescriptor().getParentId());
                Assert.assertEquals(device, nrsFile3.getDescriptor().getDeviceId());
                Assert.assertEquals(file, nrsFile3.getDescriptor().getFileId());
                Assert.assertEquals(size, nrsFile3.getDescriptor().getSize());
                Assert.assertEquals(blockSize, nrsFile3.getDescriptor().getBlockSize());
                Assert.assertEquals(hashSize, nrsFile3.getDescriptor().getHashSize());
                Assert.assertEquals(clusterSize, nrsFile3.getDescriptor().getClusterSize());
                Assert.assertEquals(now, nrsFile3.getDescriptor().getTimestamp());
                Assert.assertFalse(nrsFile3.getDescriptor().isRoot());
                Assert.assertTrue(nrsFile3.getDescriptor().isPartial());
                Assert.assertFalse(nrsFile3.isOpened());
                Assert.assertTrue(nrsFile3.isWriteable());
                Assert.assertTrue(nrsFile3.getFile().toFile().isFile());
                Assert.assertEquals(clusterSize, nrsFile3.getDescriptor().getH1Address());
                Assert.assertEquals(0, nrsFile3.getFile().toFile().length() % clusterSize);
                Assert.assertTrue(nrsFile3.getFile().toFile().isFile());
                Assert.assertEquals(nrsFile.getFile(), nrsFile3.getFile());

                if (blocks) {
                    // Compare NrsFile and NrsFileBlock: equals, except for the cluster size and the flag block
                    final NrsFileBlock fileBlock = nrsFile2.getFileBlock();
                    Assert.assertEquals(nrsFile.getDescriptor().getParentId(), fileBlock.getDescriptor().getParentId());
                    Assert.assertEquals(nrsFile.getDescriptor().getDeviceId(), fileBlock.getDescriptor().getDeviceId());
                    Assert.assertEquals(nrsFile.getDescriptor().getNodeId(), fileBlock.getDescriptor().getNodeId());
                    Assert.assertEquals(nrsFile.getDescriptor().getFileId(), fileBlock.getDescriptor().getFileId());
                    Assert.assertEquals(nrsFile.getDescriptor().getSize(), fileBlock.getDescriptor().getSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getBlockSize(), fileBlock.getDescriptor()
                            .getBlockSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getHashSize(), fileBlock.getDescriptor().getHashSize());
                    Assert.assertFalse(nrsFile.getDescriptor().getClusterSize() == fileBlock.getDescriptor()
                            .getClusterSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getTimestamp(), fileBlock.getDescriptor()
                            .getTimestamp());
                    Assert.assertEquals(nrsFile.getDescriptor().isRoot(), fileBlock.getDescriptor().isRoot());
                    Assert.assertEquals(nrsFile.getDescriptor().isPartial(), fileBlock.getDescriptor().isPartial());
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                    Assert.assertEquals(nrsFile.getFile().toFile().isFile(), fileBlock.getFile().toFile().isFile());

                    Assert.assertEquals(fileBlock.getDescriptor().getClusterSize(), fileBlock.getDescriptor()
                            .getH1Address());
                    Assert.assertEquals(0, fileBlock.getFile().toFile().length()
                            % fileBlock.getDescriptor().getClusterSize());
                }
                else {
                    Assert.assertNull(nrsFile2.getFileBlock());
                }
            }

            // Load entity from Path
            {
                final NrsFile nrsFile2 = janitor.loadNrsFile(nrsFile.getFile());
                Assert.assertSame(lastInstance, nrsFile2);
                janitor.clearCache();

                final NrsFile nrsFile3 = janitor.loadNrsFile(nrsFile.getFile());
                Assert.assertNotSame(nrsFile, nrsFile3);
                Assert.assertEquals(parent, nrsFile3.getDescriptor().getParentId());
                Assert.assertEquals(device, nrsFile3.getDescriptor().getDeviceId());
                Assert.assertEquals(file, nrsFile3.getDescriptor().getFileId());
                Assert.assertEquals(size, nrsFile3.getDescriptor().getSize());
                Assert.assertEquals(blockSize, nrsFile3.getDescriptor().getBlockSize());
                Assert.assertEquals(hashSize, nrsFile3.getDescriptor().getHashSize());
                Assert.assertEquals(clusterSize, nrsFile3.getDescriptor().getClusterSize());
                Assert.assertEquals(now, nrsFile3.getDescriptor().getTimestamp());
                Assert.assertFalse(nrsFile3.getDescriptor().isRoot());
                Assert.assertTrue(nrsFile3.getDescriptor().isPartial());
                Assert.assertFalse(nrsFile3.isOpened());
                Assert.assertTrue(nrsFile3.isWriteable());
                Assert.assertTrue(nrsFile3.getFile().toFile().isFile());
                Assert.assertEquals(clusterSize, nrsFile3.getDescriptor().getH1Address());
                Assert.assertEquals(0, nrsFile3.getFile().toFile().length() % clusterSize);
                Assert.assertTrue(nrsFile3.getFile().toFile().isFile());
                Assert.assertEquals(nrsFile.getFile(), nrsFile3.getFile());

                if (blocks) {
                    // Compare NrsFile and NrsFileBlock: equals, except for the cluster size and the flag block
                    final NrsFileBlock fileBlock = nrsFile2.getFileBlock();
                    Assert.assertEquals(nrsFile.getDescriptor().getParentId(), fileBlock.getDescriptor().getParentId());
                    Assert.assertEquals(nrsFile.getDescriptor().getDeviceId(), fileBlock.getDescriptor().getDeviceId());
                    Assert.assertEquals(nrsFile.getDescriptor().getNodeId(), fileBlock.getDescriptor().getNodeId());
                    Assert.assertEquals(nrsFile.getDescriptor().getFileId(), fileBlock.getDescriptor().getFileId());
                    Assert.assertEquals(nrsFile.getDescriptor().getSize(), fileBlock.getDescriptor().getSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getBlockSize(), fileBlock.getDescriptor()
                            .getBlockSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getHashSize(), fileBlock.getDescriptor().getHashSize());
                    Assert.assertFalse(nrsFile.getDescriptor().getClusterSize() == fileBlock.getDescriptor()
                            .getClusterSize());
                    Assert.assertEquals(nrsFile.getDescriptor().getTimestamp(), fileBlock.getDescriptor()
                            .getTimestamp());
                    Assert.assertEquals(nrsFile.getDescriptor().isRoot(), fileBlock.getDescriptor().isRoot());
                    Assert.assertEquals(nrsFile.getDescriptor().isPartial(), fileBlock.getDescriptor().isPartial());
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                    Assert.assertEquals(nrsFile.getFile().toFile().isFile(), fileBlock.getFile().toFile().isFile());

                    Assert.assertEquals(fileBlock.getDescriptor().getClusterSize(), fileBlock.getDescriptor()
                            .getH1Address());
                    Assert.assertEquals(0, fileBlock.getFile().toFile().length()
                            % fileBlock.getDescriptor().getClusterSize());
                }
                else {
                    Assert.assertNull(nrsFile2.getFileBlock());
                }
            }

            // Visit directory: get the only file
            {
                final Boolean[] found = new Boolean[] { Boolean.FALSE };
                janitor.visitImages(new SimpleFileVisitor<Path>() {

                    @Override
                    public final FileVisitResult visitFile(final Path headerPath, final BasicFileAttributes attrs)
                            throws IOException {
                        final File headerFile = headerPath.toFile();
                        Assert.assertTrue(headerFile.isFile());
                        Assert.assertEquals(nrsFile.getFile(), headerPath);
                        found[0] = Boolean.TRUE;
                        return FileVisitResult.CONTINUE;
                    }
                });
                Assert.assertTrue(found[0].booleanValue());
            }

            // Delete entity
            final NrsFileBlock fileBlock = nrsFile.getFileBlock();
            Assert.assertTrue(nrsFile.getFile().toFile().exists());
            if (blocks) {
                Assert.assertTrue(fileBlock.getFile().toFile().exists());
            }
            janitor.deleteNrsFile(nrsFile);
            Assert.assertFalse(nrsFile.getFile().toFile().exists());
            if (blocks) {
                Assert.assertFalse(fileBlock.getFile().toFile().exists());
            }

            // Visit directory: no file
            {
                final Boolean[] found = new Boolean[] { Boolean.FALSE };
                janitor.visitImages(new SimpleFileVisitor<Path>() {

                    @Override
                    public final FileVisitResult visitFile(final Path headerPath, final BasicFileAttributes attrs)
                            throws IOException {
                        found[0] = Boolean.TRUE;
                        return FileVisitResult.CONTINUE;
                    }
                });
                Assert.assertFalse(found[0].booleanValue());
            }
        }
        finally {
            janitor.fini();
        }
    }

    @Test(expected = NrsException.class)
    public void testNrsFileTooSmall() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);
            janitor.clearCache();

            // Reduce file size (less than the header length): NrsFile or blocks file
            final Path path = blocks ? nrsFile.getFileBlock().getFile() : nrsFile.getFile();
            final File file = path.toFile();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.setLength(50);
            }

            janitor.loadNrsFile(nrsFile.getFile());
        }
        finally {
            janitor.fini();
        }
    }

    @Test(expected = NrsException.class)
    public void testNrsFileEraseMagic() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);
            janitor.clearCache();

            // Reduce file size (less than the header length): NrsFile or blocks file
            final Path path = blocks ? nrsFile.getFileBlock().getFile() : nrsFile.getFile();
            final File file = path.toFile();
            try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                raf.write('f');
            }

            janitor.loadNrsFile(nrsFile.getFile());
        }
        finally {
            janitor.fini();
        }
    }

    @Test(expected = NrsException.class)
    public void testNrsFileDeleted() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);
            janitor.clearCache();

            // Delete file: NrsFile or blocks file
            final Path path = blocks ? nrsFile.getFileBlock().getFile() : nrsFile.getFile();
            Assert.assertTrue(path.toFile().delete());

            janitor.loadNrsFile(path);
        }
        finally {
            janitor.fini();
        }
    }

    @Test
    public void testNrsFileOpening() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        boolean initDone = false;
        janitor.init();
        try {
            initDone = true;

            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);

            // Open the file RO
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertTrue(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Open the file RW
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), false);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }
            // Close the file once: should be still opened
            {
                janitor.closeNrsFile(nrsFile, false);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }
            // Close the file twice
            {
                janitor.closeNrsFile(nrsFile, false);
                Assert.assertFalse(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Open the file RW
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), false);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Open the file RO
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }
            // Unlock the file: should be opened
            {
                janitor.unlockNrsFile(nrsFile);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }
            // Unlock the file: should be opened
            {
                janitor.unlockNrsFile(nrsFile);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }

                // Unlock again: should not fail (just get a warning)
                janitor.unlockNrsFile(nrsFile);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Open the file RO again
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Close the file: should be closed
            {
                janitor.closeNrsFile(nrsFile, false);
                Assert.assertFalse(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Open the file RO
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertSame(nrsFile, nrsFileReopen);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertTrue(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Flush: should be opened
            {
                janitor.flushNrsFile(nrsFile);
                Assert.assertTrue(nrsFile.isOpened());
                Assert.assertTrue(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            // fini janitor: close all files
            janitor.fini();
            initDone = false;
            {
                Assert.assertFalse(nrsFile.isOpened());
                Assert.assertFalse(nrsFile.isOpenedReadOnly());
                Assert.assertTrue(nrsFile.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFile.getFileBlock();
                    Assert.assertEquals(nrsFile.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFile.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFile.isWriteable(), fileBlock.isWriteable());
                }
            }

            janitor.init();
            initDone = true;

            // Open the file RO
            final NrsFile nrsFileNew;
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertNotSame(nrsFile, nrsFileReopen); // Cache reset during fini()/init()
                nrsFileNew = nrsFileReopen;
                Assert.assertTrue(nrsFileNew.isOpened());
                Assert.assertTrue(nrsFileNew.isOpenedReadOnly());
                Assert.assertTrue(nrsFileNew.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                    Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
                }
            }

            // Unlock: should be opened
            {
                janitor.unlockNrsFile(nrsFileNew);
                Assert.assertTrue(nrsFileNew.isOpened());
                Assert.assertTrue(nrsFileNew.isOpenedReadOnly());
                Assert.assertTrue(nrsFileNew.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                    Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
                }

            }

            // Flush: should be closed
            {
                janitor.flushNrsFile(nrsFileNew);
                Assert.assertFalse(nrsFileNew.isOpened());
                Assert.assertFalse(nrsFileNew.isOpenedReadOnly());
                Assert.assertTrue(nrsFileNew.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                    Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
                }

            }

            // Open the file RW
            {
                final NrsFile nrsFileReopen = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
                Assert.assertNotSame(nrsFile, nrsFileReopen);
                Assert.assertSame(nrsFileNew, nrsFileReopen);
                Assert.assertTrue(nrsFileNew.isOpened());
                Assert.assertTrue(nrsFileNew.isOpenedReadOnly());
                Assert.assertTrue(nrsFileNew.isWriteable());
                if (blocks) {
                    // Compare NrsFile with the fileBlock
                    final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                    Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                    Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                    Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
                }

            }

            // Close and set read-only: should be closed and not writable
            janitor.closeNrsFile(nrsFileNew, true);
            Assert.assertFalse(nrsFileNew.isOpened());
            Assert.assertFalse(nrsFileNew.isOpenedReadOnly());
            Assert.assertFalse(janitor.isNrsFileWritable(nrsFileNew));
            Assert.assertTrue(nrsFileNew.isWriteable());
            if (blocks) {
                // Compare NrsFile with the fileBlock
                final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
            }

            // Set read-only in file system
            janitor.setNrsFileNoWritable(nrsFileNew);
            Assert.assertFalse(janitor.isNrsFileWritable(nrsFileNew));
            Assert.assertFalse(nrsFileNew.isWriteable());
            if (blocks) {
                // Compare NrsFile with the fileBlock
                final NrsFileBlock fileBlock = nrsFileNew.getFileBlock();
                Assert.assertEquals(nrsFileNew.isOpened(), fileBlock.isOpened());
                Assert.assertEquals(nrsFileNew.isOpenedReadOnly(), fileBlock.isOpenedReadOnly());
                Assert.assertEquals(nrsFileNew.isWriteable(), fileBlock.isWriteable());
            }
        }
        finally {
            if (initDone) {
                janitor.fini();
            }
        }
    }

    @Test
    public void testNrsFileSealing() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, blocks);
            Assert.assertFalse(janitor.isSealed(nrsFile));
            janitor.sealNrsFile(nrsFile);
            Assert.assertTrue(janitor.isSealed(nrsFile));
        }
        finally {
            janitor.fini();
        }
    }
}
