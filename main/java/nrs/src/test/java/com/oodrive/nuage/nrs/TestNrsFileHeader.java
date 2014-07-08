package com.oodrive.nuage.nrs;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.utils.SimpleIdentifierProvider;
import com.oodrive.nuage.utils.UuidT;

/**
 * Unit tests for {@link NrsFileHeader}s.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestNrsFileHeader {

    @Test(expected = NrsException.class)
    public void testNrsFileHeaderReadSmall() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH / 2);
        NrsFileHeader.readFromBuffer(buffer);
    }

    @Test(expected = NrsException.class)
    public void testNrsFileHeaderReadWrongMagic() throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH);
        buffer.put((byte) 'N').put((byte) 'R').put((byte) 'S').put((byte) '2').put((byte) '3');
        buffer.rewind();
        NrsFileHeader.readFromBuffer(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNrsFileHeaderWriteSmall() throws IOException {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        final ByteBuffer buffer = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH / 2);
        header.writeToBuffer(buffer);
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderInvalidBlockSize() {
        final int blockSize = 0;
        final int size = 56370 * blockSize;
        final int hashSize = 550;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderInvalidSize() {
        final int blockSize = 6541;
        final int size = -1;
        final int hashSize = 550;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    /**
     * Size not a number of blocks.
     */
    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderInvalidSize2() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize - 2;
        final int hashSize = 550;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderHashSize() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 0;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    /**
     * Hash size smaller than cluster size.
     */
    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderHashSize2() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 12;
        final int clusterSize = 11;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderClusterSizeZero() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 0;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test
    public void testNrsFileHeaderMiniClusterSize() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 6;
        final int clusterSize = 20;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        flags.add(NrsFileFlag.BLOCKS);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();

        // Check that the H1 address is a fixed number of clusters
        Assert.assertEquals(0, header.getH1Address() % header.getClusterSize());
    }

    @Test(expected = NullPointerException.class)
    public void testNrsFileHeaderNullParent() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = null;
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = NullPointerException.class)
    public void testNrsFileHeaderNullDevice() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = null;
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = NullPointerException.class)
    public void testNrsFileHeaderNullNode() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = null;
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test(expected = NullPointerException.class)
    public void testNrsFileHeaderNullSnapshot() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = null;
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.build();
    }

    @Test
    public void testNrsFileHeaderNoFlags() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertFalse(header.isRoot());
        Assert.assertFalse(header.isPartial());
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderWrongLOneAddr() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertFalse(header.isRoot());
        Assert.assertFalse(header.isPartial());
        headerBuilder.addFlags(NrsFileFlag.ROOT);

        headerBuilder.hOneAddress(545);

        headerBuilder.build();
    }

    @Test
    public void testNrsFileHeaderFlagsSet() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertTrue(header.isRoot());
        Assert.assertFalse(header.isPartial());
        Assert.assertFalse(header.isBlocks());
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileHeaderFlagsBlockNotSet() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertTrue(header.isRoot());
        Assert.assertFalse(header.isPartial());
        Assert.assertFalse(header.isBlocks());
        header.newBlocksHeader();
    }

    @Test
    public void testNrsFileHeaderFlagsBlockSet() {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        flags.add(NrsFileFlag.BLOCKS);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertTrue(header.isRoot());
        Assert.assertFalse(header.isPartial());
        Assert.assertTrue(header.isBlocks());

        // Get header for file of blocks
        final NrsFileHeader<NrsFileBlock> headerBlocks = header.newBlocksHeader();
        Assert.assertTrue(headerBlocks.isRoot());
        Assert.assertFalse(headerBlocks.isPartial());
        Assert.assertFalse(headerBlocks.isBlocks());

        Assert.assertEquals(header.getParentId(), headerBlocks.getParentId());
        Assert.assertEquals(header.getDeviceId(), headerBlocks.getDeviceId());
        Assert.assertEquals(header.getNodeId(), headerBlocks.getNodeId());
        Assert.assertEquals(header.getFileId(), headerBlocks.getFileId());
        Assert.assertEquals(header.getSize(), headerBlocks.getSize());
        Assert.assertEquals(header.getBlockSize(), headerBlocks.getBlockSize());
        Assert.assertEquals(header.getHashSize(), headerBlocks.getHashSize());
        Assert.assertEquals(header.getTimestamp(), headerBlocks.getTimestamp());

        // The new cluster size depends on the block size
        Assert.assertEquals(0, headerBlocks.getClusterSize() % headerBlocks.getBlockSize());
        Assert.assertTrue(header.getClusterSize() < headerBlocks.getClusterSize());

        // Not the same H1 address (depends on the cluster size)
        Assert.assertFalse(header.getH1Address() == headerBlocks.getH1Address());
        Assert.assertEquals(headerBlocks.getClusterSize(), headerBlocks.getH1Address());
    }

    @Test
    public void testNrsFileHeaderWriteRead() throws NrsException {
        final int blockSize = 6541;
        final int size = 56370 * blockSize;
        final int hashSize = 664;
        final int clusterSize = 1240;
        final NrsFileHeader.Builder<NrsFile> headerBuilder = new NrsFileHeader.Builder<>();
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        headerBuilder.parent(parent);
        final UUID device = UUID.randomUUID();
        headerBuilder.device(device);
        final UUID node = UUID.randomUUID();
        headerBuilder.node(node);
        final UuidT<NrsFile> fileId = SimpleIdentifierProvider.newId();
        headerBuilder.file(fileId);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.clusterSize(clusterSize);
        final long now = System.currentTimeMillis();
        headerBuilder.timestamp(now);
        final Set<NrsFileFlag> flags = EnumSet.noneOf(NrsFileFlag.class);
        flags.add(NrsFileFlag.ROOT);
        flags.add(NrsFileFlag.BLOCKS);
        headerBuilder.flags(flags);

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        Assert.assertTrue(header.isRoot());
        Assert.assertFalse(header.isPartial());
        Assert.assertTrue(header.isBlocks());

        // Write to ByteBuffer and create a new header
        final ByteBuffer tmp = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH);
        header.writeToBuffer(tmp);
        tmp.clear();
        final NrsFileHeader<NrsFile> headerRead = NrsFileHeader.readFromBuffer(tmp);

        // Compare to original buffer
        Assert.assertEquals(header.getParentId(), headerRead.getParentId());
        Assert.assertEquals(header.getDeviceId(), headerRead.getDeviceId());
        Assert.assertEquals(header.getNodeId(), headerRead.getNodeId());
        Assert.assertEquals(header.getFileId(), headerRead.getFileId());
        Assert.assertEquals(header.getSize(), headerRead.getSize());
        Assert.assertEquals(header.getBlockSize(), headerRead.getBlockSize());
        Assert.assertEquals(header.getHashSize(), headerRead.getHashSize());
        Assert.assertEquals(header.getTimestamp(), headerRead.getTimestamp());
        Assert.assertEquals(header.getClusterSize(), headerRead.getClusterSize());
        Assert.assertEquals(header.getH1Address(), headerRead.getH1Address());
    }
}
