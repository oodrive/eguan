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
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.nrs.NrsRemote.NrsVersion;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.vvr.configuration.AbstractVvrCommonFixture;
import io.eguan.vvr.persistence.repository.NrsRepository;
import io.eguan.vvr.persistence.repository.NrsSnapshot;
import io.eguan.vvr.remote.VvrRemoteUtils;
import io.eguan.vvr.repository.core.api.Device;
import io.eguan.vvr.repository.core.api.FutureSnapshot;
import io.eguan.vvr.repository.core.api.FutureVoid;
import io.eguan.vvr.repository.core.api.Snapshot;
import io.eguan.vvr.repository.core.api.VersionedVolumeRepository;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the history of a repository.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author pwehrle
 * @author jmcaba
 */
public class TestRepositoryHistory extends AbstractVvrCommonFixture {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TestRepositoryHistory.class);

    private VersionedVolumeRepository repository = null;
    private Snapshot rootSnapshot = null;
    private UUID nodeId = null;

    public TestRepositoryHistory() {
        super(true);
    }

    /**
     * Create the repository to test.
     */
    @Before
    public void createRepository() {
        boolean done = false;

        // Nrs
        final NrsRepository.Builder vvrBuilder = new NrsRepository.Builder();
        vvrBuilder.configuration(getConfiguration());
        vvrBuilder.uuid(UUID.randomUUID());
        vvrBuilder.ownerId(UUID.randomUUID());
        vvrBuilder.nodeId(nodeId = UUID.randomUUID());
        vvrBuilder.rootUuid(UUID.randomUUID());
        repository = vvrBuilder.create();
        Assert.assertNotNull(repository);

        try {
            repository.init();
            try {
                repository.start(true);
                try {
                    rootSnapshot = repository.getRootSnapshot();
                    done = true;
                }
                finally {
                    if (!done) {
                        repository.stop(false);
                    }
                }
            }
            finally {
                if (!done) {
                    repository.fini();
                }
            }
        }
        finally {
            if (!done) {
                repository = null;
                rootSnapshot = null;
            }
        }
    }

    /**
     * Release resources.
     */
    @After
    public void finiRepository() {
        if (repository != null) {
            try {
                repository.stop(false);
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to stop repository " + repository, t);
            }
            try {
                repository.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to fini repository " + repository, t);
            }
            repository = null;
        }
    }

    @Test
    public void testHistory() throws Exception {
        final long size1 = getDefaultBlockSize() * 512;
        final long size2 = getDefaultBlockSize() * 1071;
        final long size3 = getDefaultBlockSize() * 12;
        final long size4 = getDefaultBlockSize() * 333;

        final UUID device1Uuid;
        final UUID device2Uuid;
        final UUID device3Uuid;
        final UUID cloneDevice1Uuid;

        final UUID snapshot1Uuid;
        final UUID snapshot2Uuid;
        final UUID snapshot3Uuid;
        {
            // New device
            final Device device1 = rootSnapshot.createDevice("D1", size1).get();
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
            { // rootSnapshot has 0 snapshot and 1 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New snapshot
            final Snapshot snapshot1 = device1.createSnapshot().get();
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(device1.getParent(), snapshot1.getUuid());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New device
            final Device device2 = snapshot1.createDevice("D2", size2).get();
            Assert.assertEquals(size2, device2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), device2.getParent());
            { // snapshot1 has 0 snapshot and 2 devices
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(2, devices.size());
                Assert.assertTrue(devices.contains(device1.getUuid()));
                Assert.assertTrue(devices.contains(device2.getUuid()));
            }

            // New snapshot
            final Snapshot snapshot2 = device2.createSnapshot("snap2").get();
            Assert.assertEquals(size2, snapshot2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), device2.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }
            { // snapshot2 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device2.getUuid());
            }

            // clone device1
            final Device cloneDevice1 = device1.clone("clonedev1", "description").get();
            Assert.assertEquals(device1.getParent(), cloneDevice1.getParent());
            Assert.assertEquals(device1.getSize(), cloneDevice1.getSize());
            Assert.assertEquals(device1.getBlockSize(), cloneDevice1.getBlockSize());
            Assert.assertEquals(device1.getDataSize(), cloneDevice1.getDataSize());

            // Resize device2
            device2.setSize(size3).get();
            Assert.assertEquals(size3, device2.getSize());
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size2, snapshot2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), device2.getParent());
            Assert.assertEquals(snapshot1.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(2, devices.size());
                int count = 0;
                for (final UUID deviceUuid : devices) {
                    if (deviceUuid.equals(device1.getUuid()) || deviceUuid.equals(cloneDevice1.getUuid())) {
                        count++;
                    }
                }
                Assert.assertEquals(2, count);
            }
            { // snapshot2 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device2.getUuid());
            }

            // New snapshot of device2
            final Snapshot snapshot3 = device2.createSnapshot("snap3").get();
            Assert.assertEquals(size3, snapshot3.getSize());
            Assert.assertEquals(size3, device2.getSize());
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size2, snapshot2.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), snapshot3.getParent());
            Assert.assertEquals(snapshot3.getUuid(), device2.getParent());
            Assert.assertEquals(snapshot1.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(2, devices.size());
                int count = 0;
                for (final UUID deviceUuid : devices) {
                    if (deviceUuid.equals(device1.getUuid()) || deviceUuid.equals(cloneDevice1.getUuid())) {
                        count++;
                    }
                }
                Assert.assertEquals(2, count);
            }
            { // snapshot2 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot3.getUuid());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot3 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot3.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot3.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device2.getUuid());
            }

            // Test resize under root
            // New device under root
            final Device device3 = rootSnapshot.createDevice("D3", size3).get();
            Assert.assertEquals(size3, device3.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device3.getParent());
            { // rootSnapshot has 1 snapshot and 1 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device3.getUuid());
            }
            // Resize device4
            device3.setSize(size4).get();
            Assert.assertEquals(size4, device3.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device3.getParent());
            { // rootSnapshot has 1 snapshot and 1 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device3.getUuid());
            }

            device1Uuid = device1.getUuid();
            device2Uuid = device2.getUuid();
            device3Uuid = device3.getUuid();
            cloneDevice1Uuid = cloneDevice1.getUuid();

            snapshot1Uuid = snapshot1.getUuid();
            snapshot2Uuid = snapshot2.getUuid();
            snapshot3Uuid = snapshot3.getUuid();
        }

        //
        // Restart repository and check state
        //

        repository.stop(true);
        repository.fini();
        Thread.sleep(100);
        repository.init();
        repository.start(true);

        // Load new objects
        rootSnapshot = repository.getRootSnapshot();
        final Device device1 = repository.getDevice(device1Uuid);
        final Device device2 = repository.getDevice(device2Uuid);
        final Device device3 = repository.getDevice(device3Uuid);
        final Device cloneDevice1 = repository.getDevice(cloneDevice1Uuid);
        final Snapshot snapshot1 = repository.getSnapshot(snapshot1Uuid);
        final Snapshot snapshot2 = repository.getSnapshot(snapshot2Uuid);
        final Snapshot snapshot3 = repository.getSnapshot(snapshot3Uuid);

        Assert.assertEquals(size3, snapshot3.getSize());
        Assert.assertEquals(size3, device2.getSize());
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(size1, snapshot1.getSize());
        Assert.assertEquals(size2, snapshot2.getSize());
        Assert.assertEquals(size4, device3.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device3.getParent());
        Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
        Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
        Assert.assertEquals(snapshot2.getUuid(), snapshot3.getParent());
        Assert.assertEquals(snapshot3.getUuid(), device2.getParent());
        Assert.assertEquals(snapshot1.getUuid(), device1.getParent());
        { // rootSnapshot has 1 snapshot and 1 device
            final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
            final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device3.getUuid());
        }
        { // snapshot1 has 1 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
            final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
            Assert.assertEquals(2, devices.size());
            int count = 0;
            for (final UUID deviceUuid : devices) {
                if (deviceUuid.equals(device1.getUuid()) || deviceUuid.equals(cloneDevice1.getUuid())) {
                    count++;
                }
            }
            Assert.assertEquals(2, count);
        }
        { // snapshot2 has 1 snapshot and 0 device
            final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot3.getUuid());
            final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
            Assert.assertTrue(devices.isEmpty());
        }
        { // snapshot3 has 0 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot3.getChildrenSnapshotsUuid();
            Assert.assertTrue(snapshots.isEmpty());
            final Collection<UUID> devices = snapshot3.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device2.getUuid());
        }

        // Take a snapshot of cloneDevice1
        final Snapshot snapshot4 = cloneDevice1.createSnapshot("snap4").get();
        Assert.assertEquals(size3, snapshot3.getSize());
        Assert.assertEquals(size3, device2.getSize());
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(size1, cloneDevice1.getSize());
        Assert.assertEquals(size1, snapshot4.getSize());
        Assert.assertEquals(size1, snapshot1.getSize());
        Assert.assertEquals(size2, snapshot2.getSize());
        Assert.assertEquals(size4, device3.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device3.getParent());
        Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
        Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
        Assert.assertEquals(snapshot2.getUuid(), snapshot3.getParent());
        Assert.assertEquals(snapshot3.getUuid(), device2.getParent());
        Assert.assertEquals(snapshot1.getUuid(), device1.getParent());
        { // rootSnapshot has 1 snapshot and 1 device
            final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
            final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device3.getUuid());
        }
        { // snapshot1 has 2 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
            Assert.assertEquals(2, snapshots.size());
            int count = 0;
            for (final UUID snapshotUuid : snapshots) {
                if (snapshotUuid.equals(snapshot2.getUuid()) || snapshotUuid.equals(snapshot4.getUuid())) {
                    count++;
                }
            }
            Assert.assertEquals(2, count);

            final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device1.getUuid());
        }
        { // snapshot2 has 1 snapshot and 0 device
            final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot3.getUuid());
            final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
            Assert.assertTrue(devices.isEmpty());
        }
        { // snapshot3 has 0 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot3.getChildrenSnapshotsUuid();
            Assert.assertTrue(snapshots.isEmpty());
            final Collection<UUID> devices = snapshot3.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device2.getUuid());
        }
        { // snapshot4 has 0 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot4.getChildrenSnapshotsUuid();
            Assert.assertTrue(snapshots.isEmpty());
            final Collection<UUID> devices = snapshot4.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), cloneDevice1.getUuid());
        }
    }

    @Test
    public void testHistoryDeleteSnapshot() throws Exception {
        final long size1 = getDefaultBlockSize() * 512;
        final long size2 = getDefaultBlockSize() * 12;

        final UUID device1Uuid;
        final UUID snapshot2Uuid;
        {
            // New device
            final Device device1 = rootSnapshot.createDevice("D1", size1).get();
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
            { // rootSnapshot has 0 snapshot and 1 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New snapshot
            final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(device1.getParent(), snapshot1.getUuid());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New snapshot
            final Snapshot snapshot2 = device1.createSnapshot("snap2").get();
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), device1.getParent());
            Assert.assertEquals(size1, snapshot2.getSize());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot2 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // Resize device1
            device1.setSize(size2).get();
            Assert.assertEquals(size2, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size1, snapshot2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot2 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New snapshot of device1
            final Snapshot snapshot3 = device1.createSnapshot("snap3").get();
            Assert.assertEquals(size2, snapshot3.getSize());
            Assert.assertEquals(size2, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size1, snapshot2.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            Assert.assertEquals(snapshot1.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), snapshot3.getParent());
            Assert.assertEquals(snapshot3.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot2 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot3.getUuid());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot3 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot3.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot3.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // Delete snapshot1
            snapshot1.delete().get();
            Assert.assertEquals(size2, snapshot3.getSize());
            Assert.assertEquals(size2, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size1, snapshot2.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), snapshot3.getParent());
            Assert.assertEquals(snapshot3.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot2 has 1 snapshot and 0 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot3.getUuid());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot3 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot3.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot3.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // Delete snapshot3
            snapshot3.delete();
            Assert.assertEquals(size2, snapshot3.getSize());
            Assert.assertEquals(size2, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(size1, snapshot2.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot2.getParent());
            Assert.assertEquals(snapshot2.getUuid(), device1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot2 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            device1Uuid = device1.getUuid();
            snapshot2Uuid = snapshot2.getUuid();
        }

        //
        // Restart repository and check state
        //

        repository.stop(true);
        repository.fini();
        Thread.sleep(1000);
        repository.init();
        repository.start(true);

        // Load new objects
        rootSnapshot = repository.getRootSnapshot();
        final Device device1 = repository.getDevice(device1Uuid);
        final Snapshot snapshot2 = repository.getSnapshot(snapshot2Uuid);

        Assert.assertEquals(size2, device1.getSize());
        Assert.assertEquals(size1, snapshot2.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), snapshot2.getParent());
        Assert.assertEquals(snapshot2.getUuid(), device1.getParent());
        { // rootSnapshot has 1 snapshot and 0 device
            final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
            Assert.assertEquals(1, snapshots.size());
            Assert.assertEquals(snapshots.iterator().next(), snapshot2.getUuid());
            final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
            Assert.assertTrue(devices.isEmpty());
        }
        { // snapshot2 has 0 snapshot and 1 device
            final Collection<UUID> snapshots = snapshot2.getChildrenSnapshotsUuid();
            Assert.assertTrue(snapshots.isEmpty());
            final Collection<UUID> devices = snapshot2.getSnapshotDevicesUuid();
            Assert.assertEquals(1, devices.size());
            Assert.assertEquals(devices.iterator().next(), device1.getUuid());
        }
    }

    /**
     * Check the {@link NrsFile} created with a simplest history.
     * 
     * @throws Exception
     */
    @Test
    public void testNrsFiles() throws Exception {
        final long size1 = getDefaultBlockSize() * 512;
        final long size2 = getDefaultBlockSize() * 12;

        final Uuid snapshot1FileUuid;
        {
            // New device
            final Device device1 = rootSnapshot.createDevice("D1", size1).get();
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
            { // rootSnapshot has 0 snapshot and 1 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // New snapshot
            final NrsSnapshot snapshot1 = (NrsSnapshot) device1.createSnapshot("snap1").get();
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(device1.getParent(), snapshot1.getUuid());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            // Resize device1
            device1.setSize(size2).get();
            Assert.assertEquals(size2, device1.getSize());
            Assert.assertEquals(size1, snapshot1.getSize());
            Assert.assertEquals(device1.getParent(), snapshot1.getUuid());
            Assert.assertEquals(rootSnapshot.getUuid(), snapshot1.getParent());
            { // rootSnapshot has 1 snapshot and 0 device
                final Collection<UUID> snapshots = rootSnapshot.getChildrenSnapshotsUuid();
                Assert.assertEquals(1, snapshots.size());
                Assert.assertEquals(snapshots.iterator().next(), snapshot1.getUuid());
                final Collection<UUID> devices = rootSnapshot.getSnapshotDevicesUuid();
                Assert.assertTrue(devices.isEmpty());
            }
            { // snapshot1 has 0 snapshot and 1 device
                final Collection<UUID> snapshots = snapshot1.getChildrenSnapshotsUuid();
                Assert.assertTrue(snapshots.isEmpty());
                final Collection<UUID> devices = snapshot1.getSnapshotDevicesUuid();
                Assert.assertEquals(1, devices.size());
                Assert.assertEquals(devices.iterator().next(), device1.getUuid());
            }

            snapshot1FileUuid = VvrRemoteUtils.newTUuid(snapshot1.getNrsFileId());
        }

        // Get the NrsFile list
        final List<NrsVersion> versions;
        final Uuid nodeUuid = VvrRemoteUtils.newUuid(nodeId);
        {
            final RemoteOperation.Builder builder = RemoteOperation.newBuilder();
            builder.setVersion(ProtocolVersion.VERSION_1);
            builder.setType(Type.NRS);
            builder.setOp(OpCode.LIST);
            final RemoteOperation reply = (RemoteOperation) repository.handleMsg(builder.build());
            Assert.assertSame(Type.NRS, reply.getType());
            Assert.assertSame(OpCode.LIST, reply.getOp());
            Assert.assertTrue(VvrRemoteUtils.equalsUuid(nodeUuid, reply.getSource()));
            Assert.assertEquals(4, reply.getNrsVersionsCount());
            versions = reply.getNrsVersionsList();
        }

        final Uuid rootSnapshotUuid = VvrRemoteUtils.newUuid(rootSnapshot.getUuid());
        {
            boolean rootFound = false;
            boolean snapFound = false;
            boolean unknownWritableFound = false;
            int unknownCount = 0;
            {
                for (final NrsVersion version : versions) {
                    Assert.assertTrue(VvrRemoteUtils.equalsUuid(nodeUuid, version.getSource()));
                    Assert.assertEquals(0L, version.getVersion());

                    if (VvrRemoteUtils.equalsUuid(rootSnapshotUuid, version.getUuid())) {
                        Assert.assertFalse(rootFound);
                        rootFound = true;
                        Assert.assertFalse(version.getWritable());
                    }
                    else if (VvrRemoteUtils.equalsUuid(snapshot1FileUuid, version.getUuid())) {
                        Assert.assertFalse(snapFound);
                        snapFound = true;
                        Assert.assertFalse(version.getWritable());
                    }
                    else {
                        unknownCount++;
                        if (version.getWritable()) {
                            // Only the NrsFile after resize should be writable
                            Assert.assertFalse(unknownWritableFound);
                            unknownWritableFound = true;
                        }
                    }
                }
            }
            // Found everything?
            Assert.assertTrue(rootFound);
            Assert.assertTrue(snapFound);
            Assert.assertTrue(unknownWritableFound);
            Assert.assertEquals(2, unknownCount);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRootSnapshotCreateDeviceNullName() throws Throwable {
        rootSnapshot.createDevice(null, 12345678);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRootSnapshotCreateDeviceZeroSize() throws Throwable {
        try {
            rootSnapshot.createDevice("D1", 0);
        }
        catch (final Exception e) {
            if (e.getCause() != null)
                throw e.getCause();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testRootSnapshotCreateDeviceBadSize() throws Exception {
        rootSnapshot.createDevice("D1", 256);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRootSnapshotCreateDeviceNoSize() throws Throwable {
        try {
            rootSnapshot.createDevice("D1");
        }
        catch (final Exception e) {
            if (e.getCause() != null)
                throw e.getCause();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testRootSnapshotCreateDeviceNullUuid1() throws Throwable {
        rootSnapshot.createDevice("D1", (UUID) null);
    }

    @Test(expected = NullPointerException.class)
    public void testRootSnapshotCreateDeviceNullUuid2() throws Throwable {
        rootSnapshot.createDevice("D1", (UUID) null, 123456789);
    }

    @Test(expected = NullPointerException.class)
    public void testRootSnapshotCreateDeviceNullUuid3() throws Throwable {
        rootSnapshot.createDevice("D1", "description", (UUID) null);
    }

    @Test(expected = NullPointerException.class)
    public void testRootSnapshotCreateDeviceNullUuid4() throws Throwable {
        rootSnapshot.createDevice("D1", "description", (UUID) null, 123456789);
    }

    @Test(expected = IllegalStateException.class)
    public void testRootSnapshotCreateDeviceDuplicateUuid() throws Throwable {
        final UUID uuid1 = UUID.randomUUID();
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", uuid1, size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
        Assert.assertEquals(uuid1, device1.getUuid());

        // Other device, same uuid
        rootSnapshot.createDevice("D2", uuid1, size1).get();
    }

    @Test(expected = IllegalStateException.class)
    public void testRootSnapshotCreateDeviceDuplicateUuidSnapshot() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        rootSnapshot.createDevice("D1", rootSnapshot.getUuid(), size1).get();
    }

    @Test
    public void testCreateSnapshotUuid1() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final UUID uuid = UUID.randomUUID();
        final Snapshot snapshot1 = device1.createSnapshot(uuid).get();
        Assert.assertEquals(uuid, snapshot1.getUuid());
    }

    @Test
    public void testCreateSnapshotUuid2() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final UUID uuid = UUID.randomUUID();
        final Snapshot snapshot1 = device1.createSnapshot("snap1", uuid).get();
        Assert.assertEquals("snap1", snapshot1.getName());
        Assert.assertEquals(uuid, snapshot1.getUuid());
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSnapshotDuplicateUuid() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();

        // Other snapshot, same uuid
        device1.createSnapshot("snap2", snapshot1.getUuid()).get();
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSnapshotDuplicateUuidDevice() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        final Device device2 = rootSnapshot.createDevice("D2", size1).get();

        // Other snapshot, same uuid as device
        device1.createSnapshot(device2.getUuid()).get();
    }

    @Test
    public void testCreateSnapshotsNoWait() throws Throwable {
        // New device
        final long size1 = getDefaultBlockSize() * 512;
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        final FutureSnapshot futureSnapshot1 = device1.createSnapshot();
        final FutureSnapshot futureSnapshot2 = device1.createSnapshot();

        // Wait for the end of the second one, the first should be done too
        final Snapshot snapshot2 = futureSnapshot2.get();
        Assert.assertTrue(futureSnapshot1.isDone());
        final Snapshot snapshot1 = futureSnapshot1.get();

        // Check history
        Assert.assertEquals(snapshot1.getParent(), rootSnapshot.getUuid());
        Assert.assertEquals(snapshot2.getParent(), snapshot1.getUuid());
        Assert.assertEquals(device1.getParent(), snapshot2.getUuid());
    }

    @Test
    public void testResizeDeviceNoWait() throws Throwable {
        // New device
        final long size1 = getDefaultBlockSize() * 512;
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        final FutureVoid futureVoid1 = device1.setSize(size1 * 2);
        final FutureVoid futureVoid2 = device1.setSize(size1 * 3);

        // Wait for the end of the second one, the first should be done too
        futureVoid2.get();
        Assert.assertTrue(futureVoid1.isDone());

        // Check history and size
        Assert.assertEquals(device1.getParent(), rootSnapshot.getUuid());
        Assert.assertEquals(size1 * 3, device1.getSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSnapshotCreateDeviceZeroSize() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        try {
            snapshot1.createDevice("D1", 0);
        }
        catch (final IllegalStateException e) {
            if (e.getCause() != null)
                throw e.getCause();
        }
    }

    @Test
    public void testSnapshotCreateDeviceNoSize() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1");
    }

    @Test(expected = IllegalStateException.class)
    public void testSnapshotCreateDeviceBadSize() throws Exception {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1", 256);
    }

    @Test(expected = NullPointerException.class)
    public void testSnapshotCreateDeviceNullUuid1() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1", (UUID) null);
    }

    @Test(expected = NullPointerException.class)
    public void testSnapshotCreateDeviceNullUuid2() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1", (UUID) null, 123456789);
    }

    @Test(expected = NullPointerException.class)
    public void testSnapshotCreateDeviceNullUuid3() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1", "description", (UUID) null);
    }

    @Test(expected = NullPointerException.class)
    public void testSnapshotCreateDeviceNullUuid4() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        snapshot1.createDevice("D1", "description", (UUID) null, 123456789);
    }

    @Test(expected = IllegalStateException.class)
    public void testSnapshotCreateDeviceDuplicateUuid() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshot
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();

        // Other device, same uuid
        snapshot1.createDevice("D2", device1.getUuid()).get();
    }

    @Test(expected = IllegalStateException.class)
    public void testSnapshotCreateDeviceDuplicateUuidSnapshot() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;
        // New device
        final Device device1 = rootSnapshot.createDevice("D1", size1).get();
        Assert.assertEquals(size1, device1.getSize());
        Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());

        // New snapshots
        final Snapshot snapshot1 = device1.createSnapshot("snap1").get();
        final Snapshot snapshot2 = device1.createSnapshot("snap2").get();

        // Other device, same uuid
        snapshot1.createDevice("D2", "description", snapshot2.getUuid(), 123456789);
    }

    /**
     * Create a device, giving its UUID.
     * 
     * @throws Throwable
     */
    @Test
    public void testCreateDeviceUuid() throws Throwable {
        final long size1 = getDefaultBlockSize() * 512;

        {// New device
            final UUID uuid1 = UUID.randomUUID();
            final Device device1 = rootSnapshot.createDevice("D1", uuid1, size1).get();
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
            Assert.assertEquals(uuid1, device1.getUuid());
            Assert.assertEquals("D1", device1.getName());
            Assert.assertNull(device1.getDescription());
        }

        final Snapshot snapshot1;
        {
            final UUID uuid1 = UUID.randomUUID();
            final Device device1 = rootSnapshot.createDevice("D1", "Descr", uuid1, size1).get();
            Assert.assertEquals(size1, device1.getSize());
            Assert.assertEquals(rootSnapshot.getUuid(), device1.getParent());
            Assert.assertEquals(uuid1, device1.getUuid());
            Assert.assertEquals("D1", device1.getName());
            Assert.assertEquals("Descr", device1.getDescription());

            // New snapshot
            snapshot1 = device1.createSnapshot("snap1").get();
        }

        // Create device, not changing the size
        {
            final UUID uuid2 = UUID.randomUUID();
            final Device device2 = snapshot1.createDevice("D1", uuid2).get();
            Assert.assertEquals(size1, device2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), device2.getParent());
            Assert.assertEquals(uuid2, device2.getUuid());
            Assert.assertEquals("D1", device2.getName());
            Assert.assertNull(device2.getDescription());
        }
        {
            final UUID uuid2 = UUID.randomUUID();
            final Device device2 = snapshot1.createDevice("D1", "description", uuid2).get();
            Assert.assertEquals(size1, device2.getSize());
            Assert.assertEquals(snapshot1.getUuid(), device2.getParent());
            Assert.assertEquals(uuid2, device2.getUuid());
            Assert.assertEquals("D1", device2.getName());
            Assert.assertEquals("description", device2.getDescription());
        }
    }

    @Test
    public void testUserProperties() {
        final String PROP1 = "the first property";
        final String PROP2 = "the second property";
        final String PROP3 = "the third property";
        final String PROP4 = "not set property";

        final String VAL11 = "the first value of property 1";
        final String VAL12 = "the second value of property 1";
        final String VAL2 = "value of property 2";
        final String VAL3 = "value of property 3";

        Assert.assertTrue(rootSnapshot.getUserProperties().isEmpty());
        Assert.assertNull(rootSnapshot.getUserProperty(PROP1));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP2));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP3));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP4));

        rootSnapshot.setUserProperties(PROP1, VAL11, PROP2, VAL2);

        Assert.assertEquals(VAL11, rootSnapshot.getUserProperty(PROP1));
        Assert.assertEquals(VAL2, rootSnapshot.getUserProperty(PROP2));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP3));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP4));

        rootSnapshot.setUserProperties(PROP1, VAL12);

        Assert.assertEquals(VAL12, rootSnapshot.getUserProperty(PROP1));
        Assert.assertEquals(VAL2, rootSnapshot.getUserProperty(PROP2));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP3));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP4));

        // Unsetting VAL12, which is not a user property should not fail
        rootSnapshot.unsetUserProperties(PROP1, VAL12);

        Assert.assertNull(rootSnapshot.getUserProperty(PROP1));
        Assert.assertEquals(VAL2, rootSnapshot.getUserProperty(PROP2));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP3));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP4));

        rootSnapshot.setUserProperties(PROP3, VAL3);
        Assert.assertNull(rootSnapshot.getUserProperty(PROP1));
        Assert.assertEquals(VAL2, rootSnapshot.getUserProperty(PROP2));
        Assert.assertEquals(VAL3, rootSnapshot.getUserProperty(PROP3));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP4));

        final Map<String, String> keyValues = rootSnapshot.getUserProperties();
        Assert.assertEquals(VAL2, keyValues.remove(PROP2));
        Assert.assertEquals(VAL3, keyValues.remove(PROP3));
        Assert.assertEquals(0, keyValues.size());
    }

    @Test(expected = NullPointerException.class)
    public void testUserPropertiesNull() {
        rootSnapshot.setUserProperties((String[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserPropertiesEmpty() {
        rootSnapshot.setUserProperties(new String[0]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserPropertiesOdd1() {
        final String PROP1 = "the first property";

        Assert.assertNull(rootSnapshot.getUserProperty(PROP1));

        // Odd number of key/value items
        rootSnapshot.setUserProperties(PROP1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserPropertiesOdd3() {
        final String PROP1 = "the first property";
        final String PROP2 = "the second property";

        final String VAL1 = "the first value of property 1";

        Assert.assertNull(rootSnapshot.getUserProperty(PROP1));
        Assert.assertNull(rootSnapshot.getUserProperty(PROP2));

        // Odd number of key/value items
        rootSnapshot.setUserProperties(PROP1, VAL1, PROP2);
    }

}
