package com.oodrive.nuage.vold.model;

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
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Test;

import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.srv.FsOpsTestHelper;
import com.oodrive.nuage.utils.unix.UnixIScsiTarget;
import com.oodrive.nuage.utils.unix.UnixMount;
import com.oodrive.nuage.utils.unix.UnixTarget;
import com.oodrive.nuage.vold.model.VoldTestHelper.CompressionType;

public class TestVoldFsOpsOnTargetIscsiL extends TestVoldFsOpsOnTargetAbstract {

    public TestVoldFsOpsOnTargetIscsiL(final CompressionType compressionType, final HashAlgorithm hash,
            final Integer blockSize, final Integer numBlocks) throws Exception {
        super(compressionType, hash, blockSize, numBlocks);
    }

    @Override
    protected UnixIScsiTarget createTarget(final DeviceMXBean device, final int number) throws IOException {
        UnixIScsiTarget.sendTarget("127.0.0.1");
        return new UnixIScsiTarget("127.0.0.1", device.getIqn());
    }

    @Override
    protected void updateTarget(final UnixTarget unixTarget) throws IOException {
        ((UnixIScsiTarget) unixTarget).rescan();
    }

    @Test
    public void testIncreaseDeviceSizeFsNoPartition() throws Throwable {
        final UnixIScsiTarget unixIscsiTarget = createTarget(device, 0);

        final File mountPoint = Files.createTempDirectory("mount").toFile();
        try {
            fsOpsHelper.loginTarget(unixIscsiTarget);

            try {
                final String targetPartStr = unixIscsiTarget.getDeviceFilePath();
                FsOpsTestHelper.createAndWaitTargetDevice(unixIscsiTarget);
                FsOpsTestHelper.formatFileSystem(unixIscsiTarget, targetPartStr);
                FsOpsTestHelper.sync(unixIscsiTarget);
                final UnixMount unixMount = FsOpsTestHelper.mountDiscOnTarget(unixIscsiTarget, targetPartStr,
                        mountPoint, null);
                final boolean isMount = true;
                try {
                    // Create temp file, cp file, read/compare file
                    final File tmpFile = fsOpsHelper.writeTmpFile();
                    try {
                        FsOpsTestHelper.writeFileOnTarget(unixIscsiTarget, tmpFile, mountPoint);

                        // Increase size in the device
                        final long size = device.getSize();
                        device.setSize(size + 4096 * 256 * 512);

                        // Update Target
                        updateTarget(unixIscsiTarget);

                        // Resize the file system
                        FsOpsTestHelper.fsResize(unixIscsiTarget, targetPartStr);

                        // Read/compare file again
                        FsOpsTestHelper.compareFileOnTarget(unixIscsiTarget, tmpFile, mountPoint);

                    }
                    finally {
                        tmpFile.delete();
                    }
                }
                finally {
                    if (isMount) {
                        unixMount.umount();
                    }
                }
            }
            finally {
                fsOpsHelper.logoutTarget(unixIscsiTarget);
            }
        }
        finally {
            mountPoint.delete();
        }
    }
}
