package io.eguan.vold.model;

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

import io.eguan.hash.HashAlgorithm;
import io.eguan.srv.CheckSrvCommand;
import io.eguan.utils.unix.UnixNbdTarget;
import io.eguan.utils.unix.UnixTarget;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.VoldTestHelper.CompressionType;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Test;

public class TestVoldFsOpsOnTargetNbdL extends TestVoldFsOpsOnTargetAbstract implements CheckSrvCommand {

    @AfterClass
    public static final void killNbdClient() throws IOException {
        UnixNbdTarget.killAll();
    }

    public TestVoldFsOpsOnTargetNbdL(final CompressionType compressionType, final HashAlgorithm hash,
            final Integer blockSize, final Integer numBlocks) throws Exception {
        super(compressionType, hash, blockSize, numBlocks);
    }

    @Override
    public final UnixNbdTarget createTarget(final DeviceMXBean device, final int number) throws IOException {
        return new UnixNbdTarget("127.0.0.1", device.getName(), number);
    }

    @Override
    protected void updateTarget(final UnixTarget unixTarget) throws Throwable {
        fsOpsHelper.logoutTarget(unixTarget);
        fsOpsHelper.loginTarget(unixTarget);
    }

    @Test
    public void testVoldFsOpsWithTrim() throws Throwable {
        // Create the target
        final UnixNbdTarget unixNbdTarget = createTarget(device, 0);
        fsOpsHelper.testNbdTrim(unixNbdTarget, this);
    }

    @Override
    public final void checkTrim(final UnixNbdTarget unixNbdTarget, final long expected, final boolean compareExactly)
            throws IOException {
        // No check can be done
    }
}
