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
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.oodrive.nuage.utils.unix.UnixTarget;

public abstract class TestAbstractServerIOL<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig, M extends TargetMgr<S, T, K>>
        extends TestAbstractTargetServer<S, T, K, M> {

    protected TestAbstractServerIOL(final M mgr) {
        super(mgr);
    }

    protected static final int BLOCKSIZE = 4096;
    protected static final int NUMBLOCKS = 4 * 64;
    protected static final int LENGTH = 4;

    protected static final long size = 8192 * 1024L * 1024L;

    protected final FsOpsTestHelper fsOpsHelper = new FsOpsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);
    protected final BasicIopsTestHelper basicIopsHelper = new BasicIopsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);

    private static final String JENKINS_TMPDIR_NAME = "/media/backup/tmp";
    private static final String SERVER_TMPDIR_PREFIX = "tmpServer";

    @Test
    public void testTwoDevicesFsOps() throws IOException, InterruptedException, ExecutionException, TimeoutException {

        // Add two devices
        final File deviceFile1 = File.createTempFile("testDevice", null);
        targets.put(deviceFile1, Long.valueOf(size));

        final File deviceFile2 = File.createTempFile("testDevice", null);
        targets.put(deviceFile2, Long.valueOf(size));
        mgr.addTarget(server, targets);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final UnixTarget unixTarget1 = mgr.createTarget(deviceFile1, 0);
            final Future<File> future1 = fsOpsHelper.multiThreadRW(executor, unixTarget1);

            final UnixTarget unixTarget2 = mgr.createTarget(deviceFile2, 1);
            final Future<File> future2 = fsOpsHelper.multiThreadRW(executor, unixTarget2);

            final File file1 = future1.get(2, TimeUnit.MINUTES);
            try {
                final File file2 = future2.get(2, TimeUnit.MINUTES);
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

    @Test
    public void testTwoDevicesBasicIopsL() throws IOException, InterruptedException, ExecutionException,
            TimeoutException {

        final BasicIopsTestHelper helper = new BasicIopsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH * 64);

        final File tmpServerBaseDir;

        // Ugly workaround for small tempdir on Jenkins host
        final File tmpJenkinsDir = new File(JENKINS_TMPDIR_NAME);
        if (tmpJenkinsDir.exists()) {
            tmpServerBaseDir = Files.createTempDirectory(tmpJenkinsDir.toPath(), SERVER_TMPDIR_PREFIX).toFile();
        }
        else {
            tmpServerBaseDir = Files.createTempDirectory(SERVER_TMPDIR_PREFIX).toFile();
        }
        try {
            // Add two devices
            final File deviceFile1 = File.createTempFile("testDevice", null, tmpServerBaseDir);
            targets.put(deviceFile1, Long.valueOf(size));

            final File deviceFile2 = File.createTempFile("testDevice", null, tmpServerBaseDir);
            targets.put(deviceFile2, Long.valueOf(size));
            mgr.addTarget(server, targets);

            final ExecutorService executor = Executors.newFixedThreadPool(2);
            try {

                final ClientBasicIops iopClient1 = mgr.initClient();
                final Future<File> future1 = helper.multiThreadRW(executor, mgr.getTargetName(deviceFile1), iopClient1,
                        size, File.createTempFile("tmpDataDump", null, tmpServerBaseDir));

                final ClientBasicIops iopClient2 = mgr.initClient();
                final Future<File> future2 = helper.multiThreadRW(executor, mgr.getTargetName(deviceFile2), iopClient2,
                        size, File.createTempFile("tmpDataDump", null, tmpServerBaseDir));

                final File dataDump1 = future1.get(10, TimeUnit.MINUTES);
                try {
                    final File dataDump2 = future2.get(10, TimeUnit.MINUTES);
                    dataDump2.delete();
                }
                finally {
                    dataDump1.delete();
                }
            }
            finally {
                executor.shutdownNow();
            }
        }
        finally {
            com.oodrive.nuage.utils.Files.deleteRecursive(tmpServerBaseDir.toPath());
        }

    }
}
