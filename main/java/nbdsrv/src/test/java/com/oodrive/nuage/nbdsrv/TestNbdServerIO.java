package com.oodrive.nuage.nbdsrv;

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
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.srv.CheckSrvCommand;
import com.oodrive.nuage.srv.FsOpsTestHelper;
import com.oodrive.nuage.srv.TestAbstractServerIO;
import com.oodrive.nuage.utils.unix.UnixNbdTarget;

public class TestNbdServerIO extends TestAbstractServerIO<ExportServer, NbdExport, NbdServerConfig, NbdTargetImpl>
        implements CheckSrvCommand {

    // Version from which the trim command is supported
    private static final String KERNEL_VERSION_REFERENCE = "3.7";

    private final Map<UnixNbdTarget, NbdDeviceFile> devices = new HashMap<>();

    @AfterClass
    public static final void killAll() throws IOException {
        UnixNbdTarget.killAll();
    }

    public TestNbdServerIO() {
        super(new NbdTargetImpl());
    }

    @Test
    public void testVoldFsOpsWithTrim() throws Throwable {

        // Create file
        final File deviceFile = File.createTempFile("testDevice", null);
        targets.put(deviceFile, size);

        // Add target to the server
        final String deviceFileName = deviceFile.getAbsolutePath();
        final NbdDeviceFile device = Main.createNbdDeviceFile(deviceFileName, size);
        final NbdExport export = new NbdExport(deviceFileName, device);
        server.addTarget(export);

        // Create the target and test
        final UnixNbdTarget unixNbdTarget = new UnixNbdTarget("127.0.0.1", deviceFileName, 0);
        devices.put(unixNbdTarget, device);
        try {
            fsOpsHelper.testNbdTrim(unixNbdTarget, this);
        }
        finally {
            devices.clear();
        }
    }

    @Test
    public void testCheckVersion() throws IOException {
        Assert.assertTrue(checkVersion("3.8.5"));
        Assert.assertFalse(checkVersion("2.6"));
        Assert.assertTrue(checkVersion("3.7"));
        Assert.assertFalse(checkVersion("3"));
        Assert.assertFalse(checkVersion("3.2.35"));
        Assert.assertTrue(checkVersion("3.7.0"));
        Assert.assertTrue(checkVersion("3.7.1"));
    }

    private final boolean checkVersion(final String version) throws IOException {
        final String[] versionSplit = version.split("\\.");
        final String[] refVersionSplit = KERNEL_VERSION_REFERENCE.split("\\.");
        int i = 0;
        while (i < versionSplit.length && i < refVersionSplit.length && versionSplit[i].equals(refVersionSplit[i])) {
            i++;
        }
        if (i < versionSplit.length && i < refVersionSplit.length) {
            return Integer.parseInt(versionSplit[i]) >= Integer.parseInt(refVersionSplit[i]);
        }
        else {
            return versionSplit.length >= refVersionSplit.length;
        }
    }

    @Override
    public final void checkTrim(final UnixNbdTarget unixNbdTarget, final long waiting, final boolean compareExactly)
            throws IOException {

        final String version = FsOpsTestHelper.getKernelVersion(unixNbdTarget).toString().trim();
        final String[] versionSplit = version.split("\\-");

        if (checkVersion(versionSplit[0])) {
            long length = 0;
            final NbdDeviceFile device = devices.get(unixNbdTarget);
            final int size = device.getTrimListSize();
            for (int i = 0; i < size; i++) {
                length += device.peekTrim().getLength();
            }
            if (compareExactly) {
                Assert.assertEquals(waiting, length);
            }
            else {
                Assert.assertTrue(length > 0);
            }
        }
    }

}
