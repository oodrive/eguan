package io.eguan.srv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.srv.AbstractServerConfig;
import io.eguan.srv.DeviceTarget;
import io.eguan.utils.unix.UnixTarget;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

public abstract class TestAbstractServerIO<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig, M extends TargetMgr<S, T, K>>
        extends TestAbstractTargetServer<S, T, K, M> {

    protected TestAbstractServerIO(final M mgr) {
        super(mgr);
    }

    @Test
    public void testBasicIops() throws Exception {

        final File deviceFile = File.createTempFile("testDevice", null);
        targets.put(deviceFile, Long.valueOf(size));
        mgr.addTarget(server, targets);

        final ClientBasicIops iopClient = mgr.initClient();
        final File dataDump = basicIopsHelper.initiatorReadWriteData(iopClient, mgr.getTargetName(deviceFile), size);
        dataDump.delete();
    }

    @Test
    public void testFsOps() throws Throwable {
        final File deviceFile = File.createTempFile("testDevice", null);
        targets.put(deviceFile, Long.valueOf(size));
        mgr.addTarget(server, targets);

        final UnixTarget unixTarget = mgr.createTarget(deviceFile, 0);
        final File file = fsOpsHelper.testReadWriteFile(unixTarget);
        file.delete();
    }

    @Test
    public void testTwoDevices() throws IOException, InterruptedException, ExecutionException, TimeoutException {

        // Add two devices
        final File deviceFile1 = File.createTempFile("testDevice", null);
        targets.put(deviceFile1, Long.valueOf(size));

        final File deviceFile2 = File.createTempFile("testDevice", null);
        targets.put(deviceFile2, Long.valueOf(size));
        mgr.addTarget(server, targets);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {

            final ClientBasicIops iopClient1 = mgr.initClient();
            final Future<File> future1 = basicIopsHelper.multiThreadRW(executor, mgr.getTargetName(deviceFile1),
                    iopClient1, size);

            final ClientBasicIops iopClient2 = mgr.initClient();
            final Future<File> future2 = basicIopsHelper.multiThreadRW(executor, mgr.getTargetName(deviceFile2),
                    iopClient2, size);

            final File dataDump1 = future1.get(1, TimeUnit.MINUTES);
            try {
                final File dataDump2 = future2.get(1, TimeUnit.MINUTES);
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

}
