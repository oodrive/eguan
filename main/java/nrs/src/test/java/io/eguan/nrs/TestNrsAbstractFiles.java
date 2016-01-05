package io.eguan.nrs;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.nrs.NrsAbstractFile;
import io.eguan.nrs.NrsException;
import io.eguan.nrs.NrsFileFlag;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.nrs.NrsMsgPostOffice;
import io.eguan.nrs.NrsStorageConfigKey;
import io.eguan.utils.SimpleIdentifierProvider;
import io.eguan.utils.UuidT;
import io.eguan.utils.mapper.FileMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for {@link NrsAbstractFile}.
 *
 * @author oodrive
 * @author llambert
 *
 */
public abstract class TestNrsAbstractFiles<T, U> extends AbstractNrsTestFixture {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestNrsAbstractFiles.class);

    /** Base dir */
    private File tempBaseDir;
    protected FileMapper fileMapper;

    /**
     * Factory of {@link NrsAbstractFile}.
     *
     * @return a new {@link NrsAbstractFile}.
     */
    abstract NrsAbstractFile<T, U> newNrsAbstractFile(final FileMapper fileMapper, final NrsFileHeader<U> header,
            final NrsMsgPostOffice postOffice);

    abstract int getWriteSize(int hashSize, int blockSize);

    abstract T getTrimElement();

    abstract T newRandomElement(int size);

    abstract void nextRandomElement(T e, Random random);

    abstract T newEmptyElement(int size);

    abstract void releaseElement(T e);

    abstract void assertEqualsElements(T e1, T e2);

    @Before
    public void setUp() {
        final MetaConfiguration config = getConfiguration();
        tempBaseDir = NrsStorageConfigKey.getInstance().getTypedValue(config);
        Assert.assertTrue(tempBaseDir.isDirectory());
        fileMapper = FileMapper.Type.FLAT.newInstance(tempBaseDir, 32, config);
    }

    /**
     * Create and delete a NrsAbstractFile.
     *
     * @throws IOException
     */
    @Test
    public void testCreateNrsAbstractFile() throws IOException {
        testCreateNrsAbstractFile(false, false);
    }

    @Test
    public void testCreateNrsAbstractFileRoot() throws IOException {
        testCreateNrsAbstractFile(true, false);
    }

    @Test
    public void testCreateNrsAbstractFilePartial() throws IOException {
        testCreateNrsAbstractFile(false, true);
    }

    @Test
    public void testCreateNrsAbstractFileRootPartial() throws IOException {
        testCreateNrsAbstractFile(true, true);
    }

    /**
     * Creating of an empty {@link NrsAbstractFile}.
     *
     * @throws IOException
     */
    @Test
    public void testNrsAbstractFileEmpty() throws IOException {
        testCreateNrsAbstractFile(true, false, true, false, false);
    }

    @Test
    public void testNrsAbstractFileSmallCluster() throws IOException {
        testCreateNrsAbstractFile(true, false, false, true, false);
    }

    @Test
    public void testNrsAbstractFileRounded() throws IOException {
        testCreateNrsAbstractFile(false, true, false, false, true);
    }

    @Test
    public void testNrsAbstractFileSmallClusterRounded() throws IOException {
        testCreateNrsAbstractFile(true, false, false, true, true);
    }

    @Test
    public void testNrsAbstractFileSmallClusterEmpty() throws IOException {
        testCreateNrsAbstractFile(true, false, true, true, false);
    }

    private void testCreateNrsAbstractFile(final boolean root, final boolean partial) throws IOException {
        testCreateNrsAbstractFile(root, partial, false, false, false);
    }

    private void testCreateNrsAbstractFile(final boolean root, final boolean partial, final boolean empty,
            final boolean smallCluster, final boolean roundedBlockCount) throws IOException {
        final int blockSize = 3510;
        final int hashSize = smallCluster ? 4 : 150;
        // Must put 2 elements in a cluster (for test purpose, small cluster size)
        final int clusterSize = getWriteSize(hashSize, blockSize) * (smallCluster ? 4 : 123);

        final int writeSize = getWriteSize(hashSize, blockSize);
        final int size = empty ? 0 : blockSize
                * (roundedBlockCount ? (smallCluster ? (5 * clusterSize) : 15) * (hashSize + 1) : 56370);

        final NrsFileHeader.Builder<U> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<U> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<U> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        if (root) {
            headerBuilder.addFlags(NrsFileFlag.ROOT);
        }
        if (partial) {
            headerBuilder.addFlags(NrsFileFlag.PARTIAL);
        }

        final NrsAbstractFile<T, U> nrsFile;
        {
            final NrsFileHeader<U> header = headerBuilder.build();
            LOGGER.info("Header " + header);

            nrsFile = newNrsAbstractFile(fileMapper, header, null);
        }

        // Not created yet
        Assert.assertEquals(parent, nrsFile.getDescriptor().getParentId());
        Assert.assertEquals(device, nrsFile.getDescriptor().getDeviceId());
        Assert.assertEquals(node, nrsFile.getDescriptor().getNodeId());
        Assert.assertEquals(fileId, nrsFile.getDescriptor().getFileId());
        Assert.assertEquals(size, nrsFile.getDescriptor().getSize());
        Assert.assertEquals(blockSize, nrsFile.getDescriptor().getBlockSize());
        Assert.assertEquals(hashSize, nrsFile.getDescriptor().getHashSize());
        Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getClusterSize());
        Assert.assertEquals(now, nrsFile.getDescriptor().getTimestamp());
        Assert.assertTrue(root == nrsFile.getDescriptor().isRoot());
        Assert.assertTrue(partial == nrsFile.getDescriptor().isPartial());
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertFalse(nrsFile.isWriteable());
        Assert.assertEquals(0, nrsFile.getFile().toFile().length());
        Assert.assertFalse(nrsFile.getFile().toFile().exists());

        if (empty) {
            Assert.assertEquals(0, nrsFile.getVersion());
        }
        else {
            try {
                nrsFile.getVersion();
                throw new AssertionError("Not reachable");
            }
            catch (final NoSuchFileException e) {
                // ok
            }
        }

        // Create file
        nrsFile.create();
        LOGGER.info("Create " + nrsFile);

        // Try to create twice
        try {
            nrsFile.create();
            throw new AssertionError("Not reachable");
        }
        catch (final NrsException e) {
            // ok
        }

        Assert.assertEquals(parent, nrsFile.getDescriptor().getParentId());
        Assert.assertEquals(device, nrsFile.getDescriptor().getDeviceId());
        Assert.assertEquals(node, nrsFile.getDescriptor().getNodeId());
        Assert.assertEquals(fileId, nrsFile.getDescriptor().getFileId());
        Assert.assertEquals(size, nrsFile.getDescriptor().getSize());
        Assert.assertEquals(blockSize, nrsFile.getDescriptor().getBlockSize());
        Assert.assertEquals(hashSize, nrsFile.getDescriptor().getHashSize());
        Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getClusterSize());
        Assert.assertEquals(now, nrsFile.getDescriptor().getTimestamp());
        Assert.assertTrue(root == nrsFile.getDescriptor().isRoot());
        Assert.assertTrue(partial == nrsFile.getDescriptor().isPartial());
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertTrue(nrsFile.isWriteable());
        Assert.assertTrue(nrsFile.getFile().toFile().isFile());
        Assert.assertEquals(0L, nrsFile.getVersion());
        if (empty) {
            Assert.assertEquals(NrsFileHeader.HEADER_LENGTH, nrsFile.getFile().toFile().length());
        }
        else if (smallCluster) {
            Assert.assertEquals(0, nrsFile.getDescriptor().getH1Address() % clusterSize);
            Assert.assertEquals(0, nrsFile.getFile().toFile().length() % clusterSize);
        }
        else {
            Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getH1Address());
            Assert.assertEquals(0, nrsFile.getFile().toFile().length() % clusterSize);
        }

        // Init block for read/write with random data
        final T block = newRandomElement(writeSize);

        // Version reached after IOs
        final long version;

        // Open RW
        nrsFile.open(false);
        try {
            Assert.assertTrue(nrsFile.isOpened());
            Assert.assertTrue(nrsFile.isWriteable());

            // Try to open twice
            try {
                nrsFile.open(false);
                throw new AssertionError("Not reachable");
            }
            catch (final IllegalStateException e) {
                // ok
            }

            // Try to delete
            try {
                nrsFile.delete();
                throw new AssertionError("Not reachable");
            }
            catch (final IllegalStateException e) {
                // ok
            }

            // Try to set not writable
            try {
                nrsFile.setNotWritable();
                throw new AssertionError("Not reachable");
            }
            catch (final IllegalStateException e) {
                // ok
            }

            // Read/write blocks
            // -----------------
            final int blockCount = size / blockSize;
            try {
                nrsFile.read(-1);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }
            try {
                nrsFile.write(-1, block);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }
            try {
                nrsFile.read(blockCount);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }
            try {
                nrsFile.write(blockCount, block);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }
            try {
                nrsFile.read(blockCount + 1);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }
            try {
                nrsFile.write(blockCount + 1, block);
                throw new AssertionError("Not reachable");
            }
            catch (final IndexOutOfBoundsException e) {
                // ok
            }

            // Version should not have changed yet
            Assert.assertEquals(0L, nrsFile.getVersion());

            if (empty) {
                // Check we can not read/write anything from empty file (prev tests)
                Assert.assertEquals(0, blockCount);
                version = 0L;
            }
            else {
                // Write invalid element size
                {// reduce scope of variable
                    final T elem = newEmptyElement(writeSize - 1);
                    try {
                        nrsFile.write(0, elem);
                        throw new AssertionError("Not reachable");
                    }
                    catch (final IllegalArgumentException e) {
                        // ok
                    }
                    finally {
                        releaseElement(elem);
                    }
                }
                {
                    final T elem = newEmptyElement(writeSize + 1);
                    try {
                        nrsFile.write(0, elem);
                        throw new AssertionError("Not reachable");
                    }
                    catch (final IllegalArgumentException e) {
                        // ok
                    }
                    finally {
                        releaseElement(elem);
                    }
                }
                {
                    final T elem = newEmptyElement(0);
                    try {
                        nrsFile.write(0, elem);
                        throw new AssertionError("Not reachable");
                    }
                    catch (final IllegalArgumentException e) {
                        // ok
                    }
                    finally {
                        releaseElement(elem);
                    }
                }

                // Version should not have changed yet
                Assert.assertEquals(0L, nrsFile.getVersion());

                final long fileSize = nrsFile.getFile().toFile().length();
                Assert.assertEquals(0, fileSize % clusterSize);
                // Read at the beginning (no write yet)
                Assert.assertNull(nrsFile.read(0));
                // Write a block at the beginning
                nrsFile.write(0, block);
                final T read0 = nrsFile.read(0);
                assertEqualsElements(block, read0);
                releaseElement(read0);
                Assert.assertNull(nrsFile.read(1));
                Assert.assertEquals(1L, nrsFile.getVersion());

                // New cluster allocated
                Assert.assertEquals(fileSize + clusterSize, nrsFile.getFile().toFile().length());

                // New block, same cluster
                nrsFile.write(2, block);
                final T read2 = nrsFile.read(2);
                assertEqualsElements(block, read2);
                releaseElement(read2);
                Assert.assertNull(nrsFile.read(1));
                Assert.assertEquals(fileSize + clusterSize, nrsFile.getFile().toFile().length());
                Assert.assertEquals(2L, nrsFile.getVersion());

                // New block, new cluster
                final int newClusterBlock = nrsFile.computeL2Capacity() + 1;
                nrsFile.write(newClusterBlock, block);
                final T readNew = nrsFile.read(newClusterBlock);
                assertEqualsElements(block, readNew);
                releaseElement(readNew);
                Assert.assertNull(nrsFile.read(1));
                Assert.assertEquals(fileSize + 2 * clusterSize, nrsFile.getFile().toFile().length());
                Assert.assertEquals(3L, nrsFile.getVersion());

                // Revert last block
                nrsFile.reset(newClusterBlock);
                Assert.assertNull(nrsFile.read(newClusterBlock));
                Assert.assertNull(nrsFile.read(1));
                Assert.assertEquals(fileSize + 2 * clusterSize, nrsFile.getFile().toFile().length());
                Assert.assertEquals(4L, nrsFile.getVersion());

                version = 4L;
            }
        }
        finally {
            nrsFile.close();
        }

        // close() does nothing if the file is close
        nrsFile.close();

        // Can get the version if the file is closed
        Assert.assertEquals(version, nrsFile.getVersion());

        // Can not read or write if closed
        try {
            nrsFile.read(0);
            throw new AssertionError("Not reachable");
        }
        catch (final IllegalStateException e) {
            // ok
        }
        {// reduce scope of variable
            final T elem = newEmptyElement(writeSize);
            try {
                nrsFile.write(0, elem);
                throw new AssertionError("Not reachable");
            }
            catch (final IllegalStateException e) {
                // ok
            }
            finally {
                releaseElement(elem);
            }
        }

        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertTrue(nrsFile.isWriteable());

        // Open RO
        nrsFile.open(true);
        try {
            Assert.assertTrue(nrsFile.isOpened());
            Assert.assertTrue(nrsFile.isWriteable());
            // Can get the version if the file is opened
            Assert.assertEquals(version, nrsFile.getVersion());
        }
        finally {
            nrsFile.close();
        }
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertTrue(nrsFile.isWriteable());

        // File not more writable
        nrsFile.setNotWritable();
        Assert.assertFalse(nrsFile.isWriteable());

        // Can get the version if the file is closed, not writable
        Assert.assertEquals(version, nrsFile.getVersion());

        // Open RW
        try {
            nrsFile.open(false);
            throw new AssertionError("Not reachable");
        }
        catch (final AccessDeniedException e) {
            // ok
        }
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertFalse(nrsFile.isWriteable());

        // Open RO
        nrsFile.open(true);
        try {
            Assert.assertTrue(nrsFile.isOpened());
            Assert.assertFalse(nrsFile.isWriteable());
            // Can get the version if the file is opened, not writable
            Assert.assertEquals(version, nrsFile.getVersion());

            // Can not write
            if (empty) {
                final T elem = newEmptyElement(writeSize);
                try {
                    nrsFile.write(0, elem);
                    throw new AssertionError("Not reachable");
                }
                catch (final IndexOutOfBoundsException e) {
                    // ok
                }
                finally {
                    releaseElement(elem);
                }
            }
            else {
                final T elem = newEmptyElement(writeSize);
                try {
                    nrsFile.write(0, elem);
                    throw new AssertionError("Not reachable");
                }
                catch (final IllegalStateException e) {
                    // ok
                }
                finally {
                    releaseElement(elem);
                }

                final T read0 = nrsFile.read(0);
                assertEqualsElements(block, read0);
                releaseElement(read0);
                Assert.assertNull(nrsFile.read(1));
            }
        }
        finally {
            nrsFile.close();
        }
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertFalse(nrsFile.isWriteable());

        // Read file header
        final File file = nrsFile.getFile().toFile();
        {
            final byte[] contents = new byte[(int) file.length()];
            try (FileInputStream fis = new FileInputStream(file)) {
                fis.read(contents);
            }
            final ByteBuffer buffer = ByteBuffer.wrap(contents);
            final NrsFileHeader<U> headerRead = NrsFileHeader.readFromBuffer(buffer);
            Assert.assertEquals(parent, headerRead.getParentId());
            Assert.assertEquals(device, headerRead.getDeviceId());
            Assert.assertEquals(node, headerRead.getNodeId());
            Assert.assertEquals(fileId, headerRead.getFileId());
            Assert.assertEquals(size, headerRead.getSize());
            Assert.assertEquals(blockSize, headerRead.getBlockSize());
            Assert.assertEquals(hashSize, headerRead.getHashSize());
            Assert.assertEquals(clusterSize, headerRead.getClusterSize());
            Assert.assertEquals(now, headerRead.getTimestamp());
            Assert.assertTrue(root == headerRead.isRoot());
            Assert.assertTrue(partial == headerRead.isPartial());
            if (empty) {
                Assert.assertEquals(NrsFileHeader.HEADER_LENGTH, nrsFile.getFile().toFile().length());
            }

            // New NrsAbstractFile instance
            {
                final NrsAbstractFile<T, U> nrsFileNew = newNrsAbstractFile(fileMapper, headerRead, null);

                Assert.assertEquals(parent, nrsFileNew.getDescriptor().getParentId());
                Assert.assertEquals(device, nrsFileNew.getDescriptor().getDeviceId());
                Assert.assertEquals(node, nrsFileNew.getDescriptor().getNodeId());
                Assert.assertEquals(fileId, nrsFileNew.getDescriptor().getFileId());
                Assert.assertEquals(size, nrsFileNew.getDescriptor().getSize());
                Assert.assertEquals(blockSize, nrsFileNew.getDescriptor().getBlockSize());
                Assert.assertEquals(hashSize, nrsFileNew.getDescriptor().getHashSize());
                Assert.assertEquals(clusterSize, nrsFileNew.getDescriptor().getClusterSize());
                Assert.assertEquals(now, nrsFileNew.getDescriptor().getTimestamp());
                Assert.assertTrue(root == nrsFileNew.getDescriptor().isRoot());
                Assert.assertTrue(partial == nrsFileNew.getDescriptor().isPartial());
                Assert.assertFalse(nrsFileNew.isOpened());
                Assert.assertFalse(nrsFileNew.isWriteable());
                if (empty) {
                    Assert.assertEquals(NrsFileHeader.HEADER_LENGTH, nrsFileNew.getFile().toFile().length());
                }
                Assert.assertTrue(nrsFileNew.getFile().toFile().exists());
                Assert.assertEquals(version, nrsFileNew.getVersion());
            }
        }

        // Release block
        releaseElement(block);

        // Delete file
        nrsFile.delete();

        Assert.assertEquals(parent, nrsFile.getDescriptor().getParentId());
        Assert.assertEquals(device, nrsFile.getDescriptor().getDeviceId());
        Assert.assertEquals(node, nrsFile.getDescriptor().getNodeId());
        Assert.assertEquals(fileId, nrsFile.getDescriptor().getFileId());
        Assert.assertEquals(size, nrsFile.getDescriptor().getSize());
        Assert.assertEquals(blockSize, nrsFile.getDescriptor().getBlockSize());
        Assert.assertEquals(hashSize, nrsFile.getDescriptor().getHashSize());
        Assert.assertEquals(clusterSize, nrsFile.getDescriptor().getClusterSize());
        Assert.assertEquals(now, nrsFile.getDescriptor().getTimestamp());
        Assert.assertTrue(root == nrsFile.getDescriptor().isRoot());
        Assert.assertTrue(partial == nrsFile.getDescriptor().isPartial());
        Assert.assertFalse(nrsFile.isOpened());
        Assert.assertFalse(nrsFile.isWriteable());
        Assert.assertEquals(0, nrsFile.getFile().toFile().length());
        Assert.assertFalse(nrsFile.getFile().toFile().exists());
    }

    /**
     * Test access to the file from 2 instances.
     *
     * @throws IOException
     */
    @Test
    public void testConcurrentFileAccess() throws IOException {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = (int) (getWriteSize(hashSize, blockSize) * 2.33);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        final NrsFileHeader.Builder<U> headerBuilder = newHeaderBuilder(size, blockSize, hashSize, clusterSize, flags);
        final NrsFileHeader<U> header = headerBuilder.build();
        Assert.assertTrue(header.isRoot());
        Assert.assertFalse(header.isPartial());

        // Create file1
        final NrsAbstractFile<T, U> file1 = newNrsAbstractFile(fileMapper, header, null);
        file1.create();
        try {

            // Create file2
            final NrsAbstractFile<T, U> file2 = newNrsAbstractFile(fileMapper, header, null);

            // Open file1 RW
            file1.open(false);
            try {
                try {
                    file2.open(true);
                    throw new AssertionError("Not reachable");
                }
                catch (final OverlappingFileLockException e) {
                    // ok
                }
                try {
                    file2.open(false);
                    throw new AssertionError("Not reachable");
                }
                catch (final OverlappingFileLockException e) {
                    // ok
                }
            }
            finally {
                file1.close();
            }

            // Open file2 RO
            file2.open(true);
            try {
                try {
                    // Can not get two shared lock from the same JVM (yet?)
                    file1.open(true);
                    throw new AssertionError("Not reachable");
                }
                catch (final OverlappingFileLockException e) {
                    // ok
                }

                try {
                    file1.open(false);
                    throw new AssertionError("Not reachable");
                }
                catch (final OverlappingFileLockException e) {
                    // ok
                }
            }
            finally {
                file2.close();
            }
        }
        finally {
            file1.delete();
        }

    }

    /**
     * Read, write and reset some elements in a NrsAbstractFile.
     *
     */
    final class NrsAbstractFilePartReadWrite implements Callable<Void> {
        private final NrsAbstractFile<T, U> file;
        private final T element;
        private final int blockCount;
        private final int firstBlockIndex;
        final AtomicBoolean goOn = new AtomicBoolean(true);

        NrsAbstractFilePartReadWrite(final NrsAbstractFile<T, U> file, final int writeSize, final int blockCount,
                final int firstBlockIndex) {
            super();
            this.file = file;
            this.element = newEmptyElement(writeSize);
            this.blockCount = blockCount;
            this.firstBlockIndex = firstBlockIndex;
        }

        @Override
        public final Void call() throws Exception {
            int blockIndex = firstBlockIndex;
            final int blockIndexMax = firstBlockIndex + blockCount;
            final Random rand = new Random();
            while (goOn.get()) {
                Assert.assertNull(file.read(blockIndex));

                nextRandomElement(element, rand);
                file.write(blockIndex, element);
                final T hashRead = file.read(blockIndex);
                assertEqualsElements(element, hashRead);
                releaseElement(hashRead);

                file.trim(blockIndex);
                Assert.assertSame(getTrimElement(), file.read(blockIndex));

                file.reset(blockIndex);
                Assert.assertNull(file.read(blockIndex));

                blockIndex++;
                if (blockIndex >= blockIndexMax) {
                    blockIndex = firstBlockIndex;
                }
            }
            releaseElement(element);
            return null;
        }
    }

    /**
     * Multi threaded read/write access to a {@link NrsAbstractFile}.
     *
     * @throws Exception
     */
    @Test
    public void testMultiThreadAccess() throws Exception {
        final int threadCount = Runtime.getRuntime().availableProcessors();
        final int blockSize = 4 * 1024;
        final int blockCount = 512; // per thread
        final long size = threadCount * blockSize * blockCount;
        final int hashSize = 20;
        final int writeSize = getWriteSize(hashSize, blockSize);
        final int clusterSize = writeSize * 11;
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);

        // Create NrsAbstractFile
        final NrsFileHeader.Builder<U> headerBuilder = newHeaderBuilder(size, blockSize, hashSize, clusterSize, flags);
        final NrsFileHeader<U> header = headerBuilder.build();
        final NrsAbstractFile<T, U> nrsFile = newNrsAbstractFile(fileMapper, header, null);
        nrsFile.create();
        nrsFile.open(false);
        try {

            final ExecutorService exec = Executors.newFixedThreadPool(threadCount);
            final List<NrsAbstractFilePartReadWrite> tasks = new ArrayList<>(threadCount);
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new NrsAbstractFilePartReadWrite(nrsFile, writeSize, blockCount, i * blockCount));
            }

            // Start tasks
            final List<Future<Void>> threads = new ArrayList<>(threadCount);
            for (final NrsAbstractFilePartReadWrite task : tasks) {
                threads.add(exec.submit(task));
            }
            Thread.sleep(8 * 1000);

            // Shutdown tasks
            for (final NrsAbstractFilePartReadWrite task : tasks) {
                task.goOn.set(false);
            }

            // Check tasks run. Make sure all the threads are done before closing the file
            Exception testException = null;
            for (final Future<Void> thread : threads) {
                try {
                    thread.get();
                }
                catch (final Exception e) {
                    testException = e;
                }
            }
            if (testException != null) {
                throw testException;
            }
        }
        finally {
            nrsFile.close();
        }
    }

    protected final NrsFileHeader.Builder<U> newHeaderBuilder(final long size, final int blockSize, final int hashSize,
            final int clusterSize, final Set<NrsFileFlag> flags) {
        final NrsFileHeader.Builder<U> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<U> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<U> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.flags(flags);
        return headerBuilder;
    }
}
