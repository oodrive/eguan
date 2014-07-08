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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.LongMath;
import com.google.protobuf.GeneratedMessageLite;
import com.google.protobuf.MessageLite;
import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.net.MsgClientStartpoint;
import com.oodrive.nuage.net.MsgNode;
import com.oodrive.nuage.net.MsgServerEndpoint;
import com.oodrive.nuage.net.MsgServerHandler;
import com.oodrive.nuage.net.TestMessagingService;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.Common.Uuid;
import com.oodrive.nuage.proto.nrs.NrsRemote.NrsFileMapping;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.utils.ByteArrays;
import com.oodrive.nuage.utils.SimpleIdentifierProvider;
import com.oodrive.nuage.utils.UuidT;

/**
 * Test {@link NrsFile} write and update on several pseudo nodes.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * @author ebredzinski
 * 
 */
@RunWith(value = Parameterized.class)
public class TestNrsFileRemoteL {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestNrsFileRemoteL.class);

    // Sometimes, the restore of the file is longer than the background close timeout, so it may not be restored unless
    // if the file is locked
    private static final int RETRY_COUNT_LOCK = 5;

    final class NrsInstance implements MsgServerHandler {

        private final MsgNode node;
        private final NrsFileJanitor janitor;
        private final NrsInstallationHelper helper;
        private MsgServerEndpoint serverEndpoint;
        private boolean serverEndpointStarted;
        private final AtomicLong timeLastUpdate = new AtomicLong();

        NrsInstance(final MsgNode node) throws InitializationError {
            super();
            this.node = node;
            this.helper = new NrsInstallationHelper();
            helper.setUpNrs();

            final MetaConfiguration config = helper.getConfiguration();
            final NrsFileJanitor janitor = new NrsFileJanitor(config);
            janitor.init();

            this.janitor = janitor;
        }

        final int getClusterSize() {
            return janitor.newNrsFileHeaderBuilder().clusterSize();
        }

        final long getTimeLastUpdate() {
            return timeLastUpdate.get();
        }

        final void setServerEndpoint(final MsgServerEndpoint serverEndpoint) {
            this.serverEndpoint = serverEndpoint;
            this.serverEndpointStarted = true;
        }

        final void startServerEndpoint() {
            assert !serverEndpointStarted;
            serverEndpoint.start();
            serverEndpointStarted = true;
        }

        final void stopServerEndpoint() {
            assert serverEndpointStarted;
            serverEndpoint.stop();
            serverEndpointStarted = false;
        }

        final void setMsgClientStartpoint(final MsgClientStartpoint clientStartpoint) {
            this.janitor.setClientStartpoint(clientStartpoint, new NrsMsgEnhancer() {
                @Override
                public final void enhance(final GeneratedMessageLite.Builder<?, ?> genericBuilder) {
                    final RemoteOperation.Builder builder = (RemoteOperation.Builder) genericBuilder;
                    builder.setSource(NrsMsgPostOffice.newUuid(node.getNodeId()));
                }
            });
        }

        final void fini() throws InitializationError {
            if (serverEndpoint != null) {
                try {
                    serverEndpoint.stop();
                }
                catch (final Throwable t) {
                    errors.add(t);
                }
                serverEndpoint = null;
            }

            try {
                janitor.fini();
            }
            catch (final Throwable t) {
                errors.add(t);
            }

            try {
                helper.tearDownNrs();
            }
            catch (final Throwable t) {
                errors.add(t);
            }
        }

        final void createTestNrsFile(final UuidT<NrsFile> parent, final UUID device, final UuidT<NrsFile> file,
                final UUID nodeCreator, final long timestamp) throws NrsException {
            TestNrsFileRemoteL.createTestNrsFile(janitor, helper.getConfiguration(), parent, device, file, nodeCreator,
                    blockCount.longValue(), hashSize, timestamp);
        }

        final NrsFile getNrsFile(final UuidT<NrsFile> file) throws NrsException {
            return janitor.loadNrsFile(file);
        }

        final NrsFile openNrsFile(final UuidT<NrsFile> file, final boolean readOnly) throws IOException {
            return janitor.openNrsFile(file, readOnly);
        }

        final void unlockNrsFile(final NrsFile file) throws IOException {
            janitor.unlockNrsFile(file);
        }

        final void closeNrsFile(final NrsFile file) throws IOException {
            janitor.closeNrsFile(file, false);
        }

        @Override
        public final MessageLite handleMessage(final MessageLite message) {
            timeLastUpdate.set(System.currentTimeMillis());
            try {
                if (!(message instanceof RemoteOperation)) {
                    final AssertionError ae = new AssertionError("Not an operation: " + message.getClass());
                    errors.add(ae);
                    return null;
                }

                final RemoteOperation op = (RemoteOperation) message;
                if (op.getType() != Type.NRS) {
                    final AssertionError ae = new AssertionError("type=" + op.getType());
                    errors.add(ae);
                    return null;
                }
                if (op.getOp() != OpCode.SET) {
                    final AssertionError ae = new AssertionError("type=" + op.getOp());
                    errors.add(ae);
                    return null;
                }
                if (!op.hasNrsFileUpdate()) {
                    final AssertionError ae = new AssertionError("No file update");
                    errors.add(ae);
                    return null;
                }
                final UuidT<NrsFile> fileUuid = fromUuidT(op.getUuid());
                try {
                    // Have received an update message (for testNrsFileHotRepare1Xxx())
                    final boolean broadcast = op.getNrsFileUpdate().getBroadcast();
                    if (!broadcast)
                        messageUpdateReceived.set(true);

                    final NrsFile nrsFile = janitor.openNrsFile(fileUuid, false);
                    try {
                        // final String log = node.getNodeId() + ": received " + op.getNrsFileUpdate().getUpdatesCount()
                        // + " updates, broadcast=" + broadcast + ", version=" + nrsFile.getVersion() + ", size="
                        // + message.getSerializedSize();

                        // LOGGER.info(log);
                        nrsFile.handleNrsFileUpdate(op.getNrsFileUpdate());
                        // LOGGER.info(log + " DONE version=" + nrsFile.getVersion());
                    }
                    finally {
                        janitor.unlockNrsFile(nrsFile);
                    }
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("File update failed " + node.getNodeId(), t);
                    errors.add(ae);
                    return null;
                }

                // No reply
                return null;
            }
            finally {
                // Update time once again
                timeLastUpdate.set(System.currentTimeMillis());
            }
        }
    }

    private final int hashSize;

    private final MsgNode SERVER_1 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 35205));
    private final MsgNode SERVER_2 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 35206));
    private final MsgNode SERVER_3 = new MsgNode(UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 35207));

    private NrsInstance nrsInstance1;
    private NrsInstance nrsInstance2;
    private NrsInstance nrsInstance3;

    private MsgClientStartpoint clientStartpoint1;
    private MsgClientStartpoint clientStartpoint2;
    private MsgClientStartpoint clientStartpoint3;

    // Test file
    private UuidT<NrsFile> fileUuid;

    // Comparison read size: set to the cluster size
    private int readSize;

    // Stores errors happening in background threads
    private final Queue<Throwable> errors = new ConcurrentLinkedQueue<>();

    // Set when a NrsInstance receives non-broadcast messages
    private final AtomicBoolean messageUpdateReceived = new AtomicBoolean();

    private final Long blockCount;

    /**
     * Select the block count so that the H1 table of the {@link NrsFile} have one block or more than one block.
     * 
     * @return initial parameters.
     */
    @Parameters
    public static Collection<Object[]> getBlockCountConfig() {
        final Object[][] blockCounts = new Object[][] {
                { /* _________ L1 table one cluster */Long.valueOf(50L), Integer.valueOf(24) },
                { /* L1 table more than one cluster */Long.valueOf(563700L), Integer.valueOf(24) },
                // FIXME { /* ______________ Very large file */Long.valueOf(268435456L), Integer.valueOf(24) },
                { /* _________ L1 table one cluster */Long.valueOf(50L), Integer.valueOf(123) },
                { /* L1 table more than one cluster */Long.valueOf(563700L), Integer.valueOf(123) },
        // FIXME { /* ______________ Very large file */Long.valueOf(268435456L), Integer.valueOf(123) },
        };
        return Arrays.asList(blockCounts);
    }

    public TestNrsFileRemoteL(final Long blockCount, final Integer hashSize) {
        super();
        this.blockCount = blockCount;
        this.hashSize = hashSize.intValue();
        LOGGER.info("Test " + TestNrsFileRemoteL.class.getSimpleName() + ": " + blockCount + " blocks, hashSize="
                + this.hashSize);
    }

    @Before
    public void setupTest() throws InitializationError, InterruptedException, NrsException {

        {
            nrsInstance1 = new NrsInstance(SERVER_1);
            final MsgServerEndpoint serverEndpoint1 = new MsgServerEndpoint(SERVER_1, nrsInstance1,
                    RemoteOperation.getDefaultInstance());
            serverEndpoint1.start();
            nrsInstance1.setServerEndpoint(serverEndpoint1);
        }

        {
            nrsInstance2 = new NrsInstance(SERVER_2);
            final MsgServerEndpoint serverEndpoint2 = new MsgServerEndpoint(SERVER_2, nrsInstance2,
                    RemoteOperation.getDefaultInstance());
            serverEndpoint2.start();
            nrsInstance2.setServerEndpoint(serverEndpoint2);
        }
        {
            nrsInstance3 = new NrsInstance(SERVER_3);
            final MsgServerEndpoint serverEndpoint3 = new MsgServerEndpoint(SERVER_3, nrsInstance3,
                    RemoteOperation.getDefaultInstance());
            serverEndpoint3.start();
            nrsInstance3.setServerEndpoint(serverEndpoint3);
        }

        {
            clientStartpoint1 = new MsgClientStartpoint(SERVER_1.getNodeId(), null);
            clientStartpoint1.addPeer(SERVER_2);
            clientStartpoint1.addPeer(SERVER_3);
            clientStartpoint1.setTimeout(30 * 1000);
            clientStartpoint1.start();
            nrsInstance1.setMsgClientStartpoint(clientStartpoint1);
            TestMessagingService.waitConnected(2, clientStartpoint1);
        }

        {
            clientStartpoint2 = new MsgClientStartpoint(SERVER_2.getNodeId(), null);
            clientStartpoint2.addPeer(SERVER_1);
            clientStartpoint2.addPeer(SERVER_3);
            clientStartpoint2.start();
            nrsInstance2.setMsgClientStartpoint(clientStartpoint2);
            TestMessagingService.waitConnected(2, clientStartpoint2);
        }

        {
            clientStartpoint3 = new MsgClientStartpoint(SERVER_3.getNodeId(), null);
            clientStartpoint3.addPeer(SERVER_1);
            clientStartpoint3.addPeer(SERVER_2);
            clientStartpoint3.start();
            nrsInstance3.setMsgClientStartpoint(clientStartpoint3);
            TestMessagingService.waitConnected(2, clientStartpoint3);
        }

        // Create a file on each node
        final UuidT<NrsFile> parent = SimpleIdentifierProvider.newId();
        final UUID device = UUID.randomUUID();
        fileUuid = SimpleIdentifierProvider.newId();
        final UUID creatorNode = SERVER_1.getNodeId();
        final long timestamp = System.currentTimeMillis();
        nrsInstance1.createTestNrsFile(parent, device, fileUuid, creatorNode, timestamp);
        nrsInstance2.createTestNrsFile(parent, device, fileUuid, creatorNode, timestamp);
        nrsInstance3.createTestNrsFile(parent, device, fileUuid, creatorNode, timestamp);

        // File comparison: read cluster per cluster
        readSize = nrsInstance1.getClusterSize();

        messageUpdateReceived.set(false);
    }

    @After
    public void tearDownTest() throws Throwable {

        if (clientStartpoint1 != null) {
            try {
                clientStartpoint1.stop();
            }
            catch (final Throwable t) {
                errors.add(t);
            }
            clientStartpoint1 = null;
        }
        if (clientStartpoint2 != null) {
            try {
                clientStartpoint2.stop();
            }
            catch (final Throwable t) {
                errors.add(t);
            }
            clientStartpoint2 = null;
        }
        if (clientStartpoint3 != null) {
            try {
                clientStartpoint3.stop();
            }
            catch (final Throwable t) {
                errors.add(t);
            }
            clientStartpoint3 = null;
        }

        if (nrsInstance1 != null) {
            nrsInstance1.fini();
            nrsInstance1 = null;
        }
        if (nrsInstance2 != null) {
            nrsInstance2.fini();
            nrsInstance2 = null;
        }
        if (nrsInstance3 != null) {
            nrsInstance3.fini();
            nrsInstance3 = null;
        }

        // Check Errors
        if (!errors.isEmpty()) {
            // Throw first one
            final Throwable t = errors.peek();
            while (!errors.isEmpty()) {
                LOGGER.warn("Test error", errors.poll());
            }
            throw t;
        }
    }

    /**
     * Test live update.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileLiveUpdate() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        // Write on the three nodes for 12 seconds
        randomWrite(nrsInstance1, 12 * 1000, new AtomicBoolean());

        // Restore file: should have nothing to do (and wait for the end of the live update of the files)
        Assert.assertFalse(restoreFile(nrsInstance1, nrsInstance2, SERVER_2, false));
        Assert.assertFalse(restoreFile(nrsInstance2, nrsInstance3, SERVER_3, false));

        // Check final contents
        compareFiles(false);
    }

    /**
     * Interrupt a server while writing in an NrsFile than repair the file after the end of writes (no write in
     * progress).
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileColdRepare() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        final Thread writer = new Thread(new Runnable() {

            @Override
            public final void run() {
                // Write on the nodes for 10 seconds
                try {
                    randomWrite(nrsInstance1, 10 * 1000, new AtomicBoolean());
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("Write failed", t);
                    errors.add(ae);
                }
            }
        }, "Writer");

        writer.setDaemon(true);
        writer.start();
        try {
            // Wait a little before stopping SERVER_3
            Thread.sleep(6 * 1000);
            nrsInstance3.stopServerEndpoint();
        }
        finally {
            writer.join();
        }

        // Restart server and restore file
        nrsInstance3.startServerEndpoint();
        TestMessagingService.waitConnected(2, clientStartpoint1);
        doRestoreFile(nrsInstance3, SERVER_3);

        // Check final contents
        compareFiles(false);
    }

    /**
     * Update a file while some writes are in progress at the beginning of the update.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileHotRepare1OneNode() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        final Thread writer = new Thread(new Runnable() {

            @Override
            public final void run() {
                // Write on the nodes until the first update message is received
                try {
                    randomWrite(nrsInstance1, 40 * 1000, messageUpdateReceived);
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("Write failed", t);
                    errors.add(ae);
                }
            }
        }, "Writer");

        writer.setDaemon(true);
        writer.start();
        try {
            suspendServerRestoreFile(nrsInstance3, SERVER_3);
        }
        finally {
            // The writer should have been stopped
            writer.join();
        }

        // Check final contents
        compareFiles(true);
    }

    /**
     * Update a file while some writes are in progress during the update.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileHotRepare2OneNode() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        // Stop write at the end
        final AtomicBoolean conditionStop = new AtomicBoolean();

        final Thread writer = new Thread(new Runnable() {

            @Override
            public final void run() {
                // Write on the nodes until the first update message is received
                try {
                    randomWrite(nrsInstance1, 40 * 1000, conditionStop);
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("Write failed", t);
                    errors.add(ae);
                }
            }
        }, "Writer");

        writer.setDaemon(true);
        writer.start();
        try {
            suspendServerRestoreFile(nrsInstance3, SERVER_3);
        }
        finally {
            // Stops the writer
            conditionStop.set(true);
            writer.join();
        }

        // Check final contents
        compareFiles(true);
    }

    /**
     * Update a file in two nodes while some writes are in progress at the beginning of the update.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileHotRepare1TwoNodes() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        final Thread writer = new Thread(new Runnable() {

            @Override
            public final void run() {
                // Write on the nodes until the first update message is received
                try {
                    randomWrite(nrsInstance1, 40 * 1000, messageUpdateReceived);
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("Write failed", t);
                    errors.add(ae);
                }
            }
        }, "Writer");

        writer.setDaemon(true);
        writer.start();

        try {

            final Thread restore2 = new Thread(new Runnable() {

                @Override
                public final void run() {
                    // Suspend and restore file in instance2
                    try {
                        suspendServerRestoreFile(nrsInstance2, SERVER_2);
                    }
                    catch (final Throwable t) {
                        final AssertionError ae = new AssertionError("Restore failed", t);
                        errors.add(ae);
                    }
                }
            }, "Restore2");

            restore2.setDaemon(true);
            restore2.start();
            try {
                suspendServerRestoreFile(nrsInstance3, SERVER_3);
            }
            finally {
                restore2.join();
            }
        }
        finally {
            // The writer should have been stopped
            writer.join();
        }

        // Check final contents
        compareFiles(true);
    }

    /**
     * Update a file in two nodes while some writes are in progress during the update.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNrsFileHotRepare2TwoNodes() throws IOException, InterruptedException {
        // Check initial contents
        compareFiles(false);

        // Stop write at the end
        final AtomicBoolean conditionStop = new AtomicBoolean();

        final Thread writer = new Thread(new Runnable() {

            @Override
            public final void run() {
                // Write on the nodes until the first update message is received
                try {
                    randomWrite(nrsInstance1, 40 * 1000, conditionStop);
                }
                catch (final Throwable t) {
                    final AssertionError ae = new AssertionError("Write failed", t);
                    errors.add(ae);
                }
            }
        }, "Writer");

        writer.setDaemon(true);
        writer.start();

        try {

            final Thread restore2 = new Thread(new Runnable() {

                @Override
                public final void run() {
                    // Suspend and restore file in instance2
                    try {
                        suspendServerRestoreFile(nrsInstance2, SERVER_2);
                    }
                    catch (final Throwable t) {
                        final AssertionError ae = new AssertionError("Restore failed", t);
                        errors.add(ae);
                    }
                }
            }, "Restore2");

            restore2.setDaemon(true);
            restore2.start();
            try {
                suspendServerRestoreFile(nrsInstance3, SERVER_3);
            }
            finally {
                restore2.join();
            }
        }
        finally {
            // Stops the writer
            conditionStop.set(true);
            writer.join();
        }

        // Check final contents
        compareFiles(true);
    }

    private final void randomWrite(final NrsInstance nrsInstance, final long duration, final AtomicBoolean conditionStop)
            throws IOException, InterruptedException {
        final NrsFile nrsFile = nrsInstance.openNrsFile(fileUuid, false);
        try {
            LOGGER.info("Start writing in '" + nrsFile + "'");

            final NrsFileHeader<NrsFile> header = nrsFile.getDescriptor();
            final long blockCount = header.getSize() / header.getBlockSize();

            // Write some hashes
            final Random random = new Random();
            final byte[] hash = new byte[hashSize];
            long prevBlockIndex = 0L;
            final long end = System.currentTimeMillis() + duration;

            while (System.currentTimeMillis() < end && !conditionStop.get()) {
                random.nextBytes(hash);
                final long blockIndex = LongMath.mod(random.nextLong(), blockCount);
                nrsFile.write(blockIndex, hash);

                // Reset some blocks and wait a little to avoid unrealistic saturation
                final long version = nrsFile.getVersion();
                if ((version % 10) == 0) {
                    nrsFile.reset(prevBlockIndex);
                    Thread.sleep(1);
                }
                if ((version % 12) == 0) {
                    nrsFile.trim(prevBlockIndex);
                }
                prevBlockIndex = blockIndex;
            }
        }
        finally {
            nrsInstance.closeNrsFile(nrsFile);
            LOGGER.info("End   writing in '" + nrsFile + "'");
        }
    }

    private final void suspendServerRestoreFile(final NrsInstance nrsInstanceDst, final MsgNode nodeDst)
            throws InterruptedException, IllegalStateException, IOException {
        // Wait a little before stopping the server
        Thread.sleep(4 * 1000);
        nrsInstanceDst.stopServerEndpoint();

        // Wait a little before performing update
        Thread.sleep(7 * 1000);
        // Restart server and restore file
        nrsInstanceDst.startServerEndpoint();
        TestMessagingService.waitConnected(2, clientStartpoint1);

        doRestoreFile(nrsInstanceDst, nodeDst);
    }

    private final void doRestoreFile(final NrsInstance nrsInstanceDst, final MsgNode nodeDst)
            throws IllegalStateException, IOException {
        final NrsFile nrsFile = nrsInstanceDst.getNrsFile(fileUuid);
        LOGGER.info("Start restore '" + nrsFile + "'");
        try {
            boolean lockDst = false;
            int retryCount = 0;
            while (restoreFile(nrsInstance1, nrsInstanceDst, nodeDst, lockDst)) {
                LOGGER.info("Retry file update");
                retryCount++;
                if (retryCount > RETRY_COUNT_LOCK)
                    lockDst = true;
            }
        }
        finally {
            LOGGER.info("End   restore '" + nrsFile + "'");
        }
    }

    private final boolean restoreFile(final NrsInstance nrsInstanceSrc, final NrsInstance nrsInstanceDst,
            final MsgNode nodeDst, final boolean lockDst) throws IllegalStateException, IOException {
        boolean dstOpened = true;
        final NrsFile nrsFileDst = nrsInstanceDst.openNrsFile(fileUuid, false);
        try {
            if (!lockDst) {
                nrsInstanceDst.unlockNrsFile(nrsFileDst);
                dstOpened = false;
            }

            final RemoteOperation.Builder builder = nrsFileDst.getFileMapping(HashAlgorithm.TIGER);
            // Complete builder
            builder.setVersion(ProtocolVersion.VERSION_1);
            builder.setType(Type.NRS);
            builder.setOp(OpCode.LIST);
            final NrsFileMapping mapping = builder.build().getNrsFileMapping();
            final NrsFile nrsFileSrc = nrsInstanceSrc.openNrsFile(fileUuid, true);
            nrsInstanceSrc.unlockNrsFile(nrsFileSrc);
            nrsFileSrc.processNrsFileSync(mapping, nodeDst.getNodeId());

            // Wait for the update end on the destination file
            final boolean inProgress = nrsFileDst.waitUpdateEnd(60, TimeUnit.SECONDS);
            if (inProgress) {
                nrsFileDst.resetUpdate();
            }
            return nrsFileDst.isLastUpdateAborted();
        }
        finally {
            if (dstOpened) {
                nrsInstanceDst.unlockNrsFile(nrsFileDst);
            }
        }
    }

    /**
     * Check that the {@link NrsFile} is identical on the three 'nodes'.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    private final void compareFiles(final boolean checkDone) throws IOException, InterruptedException {
        if (checkDone) {
            checkMessagesDone();
        }

        LOGGER.info("Start compare files");

        final long v1, v2, v3;
        final File file1, file2, file3;
        {
            final NrsFile nrsFile1 = nrsInstance1.getNrsFile(fileUuid);
            file1 = nrsFile1.getFile().toFile();
            v1 = nrsFile1.getVersion();
        }
        {
            final NrsFile nrsFile2 = nrsInstance2.getNrsFile(fileUuid);
            file2 = nrsFile2.getFile().toFile();
            v2 = nrsFile2.getVersion();
        }
        {
            final NrsFile nrsFile3 = nrsInstance3.getNrsFile(fileUuid);
            file3 = nrsFile3.getFile().toFile();
            v3 = nrsFile3.getVersion();
        }

        Assert.assertEquals(v1, v2);
        Assert.assertEquals(v1, v3);
        compareFiles(file1, file2);
        compareFiles(file1, file3);

        LOGGER.info("End   compare files");
    }

    private final long MSG_TIMEOUT = 200; // 200 ms
    private final int MSG_COUNT = 20;

    /**
     * Wait for the end of the processing of update messages. Check if some messages have been received for the
     * {@link NrsInstance}.
     * 
     * @throws InterruptedException
     */
    private final void checkMessagesDone() throws InterruptedException {
        final NrsInstance[] nrsInstances = new NrsInstance[] { nrsInstance2, nrsInstance3 };
        final long[] prevTimeLastUpdates = new long[] { nrsInstance2.getTimeLastUpdate(),
                nrsInstance3.getTimeLastUpdate() };
        final int[] counts = new int[] { 0, 0 };
        int done;
        do {
            Thread.sleep(MSG_TIMEOUT);

            done = 0;
            for (int i = 0; i < nrsInstances.length; i++) {
                final NrsInstance nrsInstance = nrsInstances[i];
                final long newTimeLastUpdate = nrsInstance.getTimeLastUpdate();
                if (prevTimeLastUpdates[i] == newTimeLastUpdate) {
                    counts[i]++;
                    done += counts[i] >= MSG_COUNT ? 1 : 0;
                }
                else {
                    // New message(s) received: reset counter
                    counts[i] = 0;
                    prevTimeLastUpdates[i] = newTimeLastUpdate;
                }
            }
        } while (done != nrsInstances.length);
    }

    private final void compareFiles(final File f1, final File f2) throws IOException {
        final byte[] bufFile1 = new byte[readSize];
        final byte[] bufFile2 = new byte[readSize];

        // Check file size
        Assert.assertEquals("size", f1.length(), f2.length());
        Assert.assertEquals(0, f1.length() % readSize);

        // Read contents
        long offset = 0;
        try (FileInputStream fis1 = new FileInputStream(f1)) {
            try (FileInputStream fis2 = new FileInputStream(f2)) {
                int readLen;
                while ((readLen = fis1.read(bufFile1)) != -1) {
                    // Should fill the buffer at every read
                    Assert.assertEquals(readSize, readLen);
                    Assert.assertEquals(readSize, fis2.read(bufFile2));
                    // Compare read bytes
                    ByteArrays.assertEqualsByteArrays("offset=" + offset + " ", bufFile1, bufFile2);
                    offset += readLen;
                }
            }
        }
    }

    private static final UuidT<NrsFile> fromUuidT(@Nonnull final Uuid uuid) {
        return new UuidT<>(uuid.getMsb(), uuid.getLsb());
    }

    private static final void createTestNrsFile(final NrsFileJanitor janitor, final MetaConfiguration config,
            final UuidT<NrsFile> parent, final UUID device, final UuidT<NrsFile> file, final UUID node,
            final long blockCount, final int hashSize, final long timestamp) throws NrsException {
        // Check cluster size
        final int clusterSize = janitor.newNrsFileHeaderBuilder().clusterSize();
        Assert.assertEquals(NrsClusterSizeConfigKey.getInstance().getTypedValue(config), Integer.valueOf(clusterSize));

        // Create NrsFile
        final int blockSize = 4096;
        final long size = blockSize * blockCount;

        final NrsFileHeader.Builder<NrsFile> headerBuilder = janitor.newNrsFileHeaderBuilder();
        headerBuilder.parent(parent);
        headerBuilder.device(device);
        headerBuilder.node(node);
        headerBuilder.file(file);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.timestamp(timestamp);
        headerBuilder.addFlags(NrsFileFlag.PARTIAL);
        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        janitor.createNrsFile(header);
    }

}
