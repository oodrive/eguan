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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestAbstractTargetServer<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig, M extends TargetMgr<S, T, K>> {

    protected static final int BLOCKSIZE = 4096;
    protected static final int NUMBLOCKS = 4 * 64;
    protected static final int LENGTH = 4;

    protected static final long size = 8192 * 1024L * 1024L;

    protected final FsOpsTestHelper fsOpsHelper = new FsOpsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);
    protected final BasicIopsTestHelper basicIopsHelper = new BasicIopsTestHelper(BLOCKSIZE, NUMBLOCKS, LENGTH);

    static final Logger LOGGER = LoggerFactory.getLogger(TestAbstractTargetServer.class);

    protected AbstractServer<S, T, K> server;
    protected final Map<File, Long> targets = new HashMap<>();

    protected final M mgr;

    TestAbstractTargetServer(final M mgr) {
        this.mgr = mgr;
    }

    @Before
    public void init() throws IOException {
        server = mgr.createServer();
        Assert.assertFalse(server.isStarted());

        server.start();
        Assert.assertTrue(server.isStarted());

    }

    @After
    public void fini() throws IOException {
        try {
            server.stop();
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to stop server ", t);
        }
        for (final File deviceFile : targets.keySet()) {
            try {
                if (deviceFile.exists()) {
                    Files.delete(deviceFile.toPath());
                }
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to delete file: " + deviceFile.getAbsolutePath(), t);
            }
        }
        mgr.removeFiles();
        targets.clear();
    }
}
