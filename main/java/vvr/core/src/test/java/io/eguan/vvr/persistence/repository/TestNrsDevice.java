package io.eguan.vvr.persistence.repository;

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

import io.eguan.nrs.NrsFile;
import io.eguan.utils.ByteArrays;
import io.eguan.utils.SimpleIdentifierProvider;
import io.eguan.utils.UuidT;
import io.eguan.vvr.persistence.repository.NrsDevice;
import io.eguan.vvr.persistence.repository.NrsRepository;
import io.eguan.vvr.persistence.repository.NrsSnapshot;
import io.eguan.vvr.repository.core.api.BlockKeyLookupEx;
import io.eguan.vvr.repository.core.api.TestDeviceAbstract;

import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

/**
 * Some unit tests on {@link NrsDevice}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class TestNrsDevice extends TestDeviceAbstract {

    public TestNrsDevice() {
        super(true);
    }

    /**
     * Test write and read of keys in {@link NrsDevice}. Check NRS hierarchy too.
     * 
     * @throws Exception
     */
    @Test
    public void testBlockKey() throws Exception {

        final NrsDevice nrsDevice = (NrsDevice) device;
        final long sizeOrig = nrsDevice.getSize();
        final NrsRepository vvr = nrsDevice.getVvr();
        final NrsSnapshot rootSnapshot = (NrsSnapshot) vvr.getRootSnapshot();
        final int hashLen = vvr.getHashLength();
        final byte[] key11 = new byte[hashLen];
        final byte[] key12 = new byte[hashLen];
        final byte[] key13 = new byte[hashLen];
        final byte[] key14 = new byte[hashLen];
        final byte[] key21 = new byte[hashLen];
        final byte[] key22 = new byte[hashLen];
        final long offset1 = 0;
        final long offset2 = 55;
        final long offset3 = 12;
        {
            final Random ran = new Random();
            ran.nextBytes(key11);
            ran.nextBytes(key12);
            ran.nextBytes(key13);
            ran.nextBytes(key14);
            ran.nextBytes(key21);
            ran.nextBytes(key22);
        }

        // Write key11 and key2 in current NrsFile
        nrsDevice.writeBlockHash(offset1, key11);
        nrsDevice.writeBlockHash(offset2, key21);

        // Init hierarchy check
        final UuidT<NrsFile> nrs1 = nrsDevice.getParentFile();
        Assert.assertEquals(rootSnapshot.getNrsFileId(), nrs1);
        Assert.assertEquals(rootSnapshot.getNrsFileId(), SimpleIdentifierProvider.fromUUID(rootSnapshot.getUuid()));
        Assert.assertEquals(rootSnapshot.getUuid(), nrsDevice.getParent());

        // Write key22 in new NrsFile
        final NrsSnapshot snapshot = (NrsSnapshot) nrsDevice.createSnapshot().get();
        nrsDevice.writeBlockHash(offset2, key22);

        // Check hierarchy
        final UuidT<NrsFile> nrs2 = nrsDevice.getParentFile();
        final UuidT<NrsFile> nrs3 = nrsDevice.getNrsFileId();
        Assert.assertEquals(snapshot.getNrsFileId(), nrs2);
        Assert.assertEquals(snapshot.getUuid(), vvr.getSnapshotFromFile(nrs2).getUuid());
        Assert.assertEquals(snapshot.getParent(), vvr.getSnapshotFromFile(nrs1).getUuid());
        Assert.assertEquals(snapshot.getParentFile(), vvr.getSnapshotFromFile(nrs1).getNrsFileId());
        Assert.assertEquals(snapshot.getUuid(), nrsDevice.getParent());

        // Clone device and activate it
        final NrsDevice cloneNrsDevice = (NrsDevice) nrsDevice.clone("clone1", "description1").get();
        cloneNrsDevice.activate().get();

        Assert.assertEquals(nrs3, cloneNrsDevice.getParentFile());
        Assert.assertFalse(nrs3 == nrsDevice.getNrsFileId());
        Assert.assertEquals(nrs3, nrsDevice.getParentFile());

        nrsDevice.writeBlockHash(offset1, key12);

        snapshot.openNrsFileTest();
        try {
            { // Read from SNAPSHOT (before the DEVICE: implicit open of NrsFile of the snapshot)
              // Read hash - not recursive
                ByteArrays.assertEqualsByteArrays(key11, (byte[]) snapshot.readHash(offset1, false, false));
                ByteArrays.assertEqualsByteArrays(key21, (byte[]) snapshot.readHash(offset2, false, false));
                Assert.assertNull(snapshot.readHash(offset3, false, false));

                // Read hash - recursive
                ByteArrays.assertEqualsByteArrays(key11, (byte[]) snapshot.readHash(offset1, true, false));
                ByteArrays.assertEqualsByteArrays(key21, (byte[]) snapshot.readHash(offset2, true, false));
                Assert.assertNull(snapshot.readHash(offset3, true, false));

                // Read hash - not recursive, extended
                { // offset1
                    final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) snapshot.readHash(offset1, false, true);
                    ByteArrays.assertEqualsByteArrays(key11, lookup1.getKey());
                    Assert.assertTrue(lookup1.isSourceCurrent());
                }
                { // offset2
                    final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) snapshot.readHash(offset2, false, true);
                    ByteArrays.assertEqualsByteArrays(key21, lookup2.getKey());
                    Assert.assertTrue(lookup2.isSourceCurrent());
                }
                { // offset3
                    final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) snapshot.readHash(offset3, false, true);
                    Assert.assertNull(lookup3);
                }

                // Read hash - recursive, extended
                { // offset1
                    final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) snapshot.readHash(offset1, true, true);
                    ByteArrays.assertEqualsByteArrays(key11, lookup1.getKey());
                    Assert.assertTrue(lookup1.isSourceCurrent());
                }
                { // offset2
                    final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) snapshot.readHash(offset2, true, true);
                    ByteArrays.assertEqualsByteArrays(key21, lookup2.getKey());
                    Assert.assertTrue(lookup2.isSourceCurrent());
                }
                { // offset3
                    final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) snapshot.readHash(offset3, true, true);
                    Assert.assertNull(lookup3);
                }
            }
        }
        finally {
            snapshot.closeNrsFile();
        }

        { // Read from DEVICE
          // Read hash - not recursive
            ByteArrays.assertEqualsByteArrays(key12, (byte[]) nrsDevice.readHash(offset1, false, false));
            Assert.assertNull(nrsDevice.readHash(offset2, false, false));
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            Assert.assertNull(cloneNrsDevice.readHash(offset1, false, false));
            Assert.assertNull(cloneNrsDevice.readHash(offset2, false, false));
            Assert.assertNull(cloneNrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key12, (byte[]) nrsDevice.readHash(offset1, true, false));
            ByteArrays.assertEqualsByteArrays(key22, (byte[]) nrsDevice.readHash(offset2, true, false));
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            ByteArrays.assertEqualsByteArrays(key11, (byte[]) cloneNrsDevice.readHash(offset1, true, false));
            ByteArrays.assertEqualsByteArrays(key22, (byte[]) cloneNrsDevice.readHash(offset2, true, false));
            Assert.assertNull(cloneNrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, false, true);
                ByteArrays.assertEqualsByteArrays(key12, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());

                final BlockKeyLookupEx clonelookup1 = (BlockKeyLookupEx) cloneNrsDevice.readHash(offset1, false, true);
                Assert.assertNull(clonelookup1);
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, false, true);
                Assert.assertNull(lookup2);

                final BlockKeyLookupEx clonelookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, false, true);
                Assert.assertNull(clonelookup2);
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);

                final BlockKeyLookupEx clonelookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(clonelookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key12, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());

                final BlockKeyLookupEx clonelookup1 = (BlockKeyLookupEx) cloneNrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key11, clonelookup1.getKey());
                Assert.assertFalse(clonelookup1.isSourceCurrent());
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, true, true);
                ByteArrays.assertEqualsByteArrays(key22, lookup2.getKey());
                Assert.assertFalse(lookup2.isSourceCurrent());

                final BlockKeyLookupEx clonelookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, true, true);
                ByteArrays.assertEqualsByteArrays(key22, clonelookup2.getKey());
                Assert.assertFalse(clonelookup2.isSourceCurrent());
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);

                final BlockKeyLookupEx clonelookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(clonelookup3);
            }
        }

        final UuidT<NrsFile> nrs4 = nrsDevice.getNrsFileId();
        // Reduce size
        final long newSize = (offset2 - 1) * deviceBlockSize;
        nrsDevice.setSize(newSize).get();

        // Check hierarchy
        final UuidT<NrsFile> nrs5 = nrsDevice.getParentFile();
        final UuidT<NrsFile> nrs6 = nrsDevice.getNrsFileId();
        Assert.assertFalse(snapshot.getUuid().equals(nrs5));
        Assert.assertEquals(nrs4, nrs5);
        Assert.assertEquals(snapshot.getUuid(), vvr.getSnapshotFromFile(nrs2).getUuid());
        Assert.assertEquals(snapshot.getParent(), vvr.getSnapshotFromFile(nrs1).getUuid());
        Assert.assertEquals(snapshot.getParentFile(), vvr.getSnapshotFromFile(nrs1).getNrsFileId());
        Assert.assertEquals(snapshot.getUuid(), nrsDevice.getParent());

        { // Read from DEVICE
          // Read hash - not recursive
            Assert.assertNull(nrsDevice.readHash(offset1, false, false));
            try {
                nrsDevice.readHash(offset2, false, false);
                throw new AssertionFailedError("Reached");
            }
            catch (final IndexOutOfBoundsException e) {
                // Ok
            }
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key12, (byte[]) nrsDevice.readHash(offset1, true, false));
            try {
                nrsDevice.readHash(offset2, true, false);
                throw new AssertionFailedError("Reached");
            }
            catch (final IndexOutOfBoundsException e) {
                // Ok
            }
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                Assert.assertNull(nrsDevice.readHash(offset1, false, true));
            }
            { // offset2
                try {
                    nrsDevice.readHash(offset2, false, true);
                    throw new AssertionFailedError("Reached");
                }
                catch (final IndexOutOfBoundsException e) {
                    // Ok
                }
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key12, lookup1.getKey());
                Assert.assertFalse(lookup1.isSourceCurrent());
            }
            { // offset2
                try {
                    nrsDevice.readHash(offset2, true, true);
                    throw new AssertionFailedError("Reached");
                }
                catch (final IndexOutOfBoundsException e) {
                    // Ok
                }
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);
            }
        }

        // Write key13 in new NrsFile
        nrsDevice.writeBlockHash(offset1, key13);

        { // Read from DEVICE
          // Read hash - not recursive
            ByteArrays.assertEqualsByteArrays(key13, (byte[]) nrsDevice.readHash(offset1, false, false));
            try {
                nrsDevice.readHash(offset2, false, false);
                throw new AssertionFailedError("Reached");
            }
            catch (final IndexOutOfBoundsException e) {
                // Ok
            }
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key13, (byte[]) nrsDevice.readHash(offset1, true, false));
            try {
                nrsDevice.readHash(offset2, true, false);
                throw new AssertionFailedError("Reached");
            }
            catch (final IndexOutOfBoundsException e) {
                // Ok
            }
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, false, true);
                ByteArrays.assertEqualsByteArrays(key13, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                try {
                    nrsDevice.readHash(offset2, false, true);
                    throw new AssertionFailedError("Reached");
                }
                catch (final IndexOutOfBoundsException e) {
                    // Ok
                }
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key13, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                try {
                    nrsDevice.readHash(offset2, true, true);
                    throw new AssertionFailedError("Reached");
                }
                catch (final IndexOutOfBoundsException e) {
                    // Ok
                }
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);
            }
        }

        // Restore previous size
        nrsDevice.setSize(sizeOrig).get();

        // Check hierarchy
        final UuidT<NrsFile> nrs7 = nrsDevice.getParentFile();
        final UuidT<NrsFile> nrs8 = nrsDevice.getNrsFileId();
        Assert.assertFalse(snapshot.getUuid().equals(nrs7));
        Assert.assertEquals(nrs6, nrs7);
        Assert.assertEquals(snapshot.getUuid(), vvr.getSnapshotFromFile(nrs2).getUuid());
        Assert.assertEquals(snapshot.getParent(), vvr.getSnapshotFromFile(nrs1).getUuid());
        Assert.assertEquals(snapshot.getParentFile(), vvr.getSnapshotFromFile(nrs1).getNrsFileId());
        Assert.assertEquals(snapshot.getUuid(), nrsDevice.getParent());

        { // Read from DEVICE
          // Read hash - not recursive
            Assert.assertNull(nrsDevice.readHash(offset1, false, false));
            Assert.assertNull(nrsDevice.readHash(offset2, false, false));
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key13, (byte[]) nrsDevice.readHash(offset1, true, false));
            Assert.assertNull(nrsDevice.readHash(offset2, true, false));
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                Assert.assertNull(nrsDevice.readHash(offset1, false, true));
            }
            { // offset2
                Assert.assertNull(nrsDevice.readHash(offset2, false, true));
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key13, lookup1.getKey());
                Assert.assertFalse(lookup1.isSourceCurrent());
            }
            { // offset2
                Assert.assertNull(nrsDevice.readHash(offset2, true, true));
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);
            }
        }

        // Write key14 in new NrsFile
        nrsDevice.writeBlockHash(offset1, key14);

        { // Read from DEVICE
          // Read hash - not recursive
            ByteArrays.assertEqualsByteArrays(key14, (byte[]) nrsDevice.readHash(offset1, false, false));
            Assert.assertNull(nrsDevice.readHash(offset2, false, false));
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key14, (byte[]) nrsDevice.readHash(offset1, true, false));
            Assert.assertNull(nrsDevice.readHash(offset2, true, false));
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, false, true);
                ByteArrays.assertEqualsByteArrays(key14, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, false, true);
                Assert.assertNull(lookup2);
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key14, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, true, true);
                Assert.assertNull(lookup2);
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);
            }
        }

        // Activate device: same node, keep NrsFile
        nrsDevice.deactivate().get();
        nrsDevice.activate().get();

        // Check hierarchy
        final UuidT<NrsFile> nrs9 = nrsDevice.getParentFile();
        final UuidT<NrsFile> nrs10 = nrsDevice.getNrsFileId();
        Assert.assertFalse(snapshot.getUuid().equals(nrs9));
        Assert.assertEquals(nrs6, nrs9);
        Assert.assertEquals(nrs8, nrs10);
        Assert.assertEquals(snapshot.getUuid(), vvr.getSnapshotFromFile(nrs2).getUuid());
        Assert.assertEquals(snapshot.getParent(), vvr.getSnapshotFromFile(nrs1).getUuid());
        Assert.assertEquals(snapshot.getParentFile(), nrs1);
        Assert.assertEquals(snapshot.getUuid(), nrsDevice.getParent());

        { // Read from DEVICE
          // Read hash - not recursive
            ByteArrays.assertEqualsByteArrays(key14, (byte[]) nrsDevice.readHash(offset1, false, false));
            Assert.assertNull(nrsDevice.readHash(offset2, false, false));
            Assert.assertNull(nrsDevice.readHash(offset3, false, false));

            // Read hash - recursive
            ByteArrays.assertEqualsByteArrays(key14, (byte[]) nrsDevice.readHash(offset1, true, false));
            Assert.assertNull(nrsDevice.readHash(offset2, true, false));
            Assert.assertNull(nrsDevice.readHash(offset3, true, false));

            // Read hash - not recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, false, true);
                ByteArrays.assertEqualsByteArrays(key14, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, false, true);
                Assert.assertNull(lookup2);
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, false, true);
                Assert.assertNull(lookup3);
            }

            // Read hash - recursive, extended
            { // offset1
                final BlockKeyLookupEx lookup1 = (BlockKeyLookupEx) nrsDevice.readHash(offset1, true, true);
                ByteArrays.assertEqualsByteArrays(key14, lookup1.getKey());
                Assert.assertTrue(lookup1.isSourceCurrent());
            }
            { // offset2
                final BlockKeyLookupEx lookup2 = (BlockKeyLookupEx) nrsDevice.readHash(offset2, true, true);
                Assert.assertNull(lookup2);
            }
            { // offset3
                final BlockKeyLookupEx lookup3 = (BlockKeyLookupEx) nrsDevice.readHash(offset3, true, true);
                Assert.assertNull(lookup3);
            }
        }

    }
}
