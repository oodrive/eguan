package com.oodrive.nuage.nbdsrv;

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
import java.net.InetAddress;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;

public abstract class TestNbdAbstract {

    private static File deviceFile;
    protected static NbdDeviceFile device;
    protected static NbdServer server;
    protected static final long size = 8192 * 1024L * 1024L;
    protected static String deviceFileName;

    @BeforeClass
    public static void initServer() throws InitializationError {
        server = new NbdServer(InetAddress.getLoopbackAddress());
        server.start();
        try {
            deviceFile = File.createTempFile("testDevice", null);
            try {
                deviceFileName = deviceFile.getAbsolutePath();
                device = Main.createNbdDeviceFile(deviceFileName, size);
                final NbdExport export = new NbdExport(deviceFileName, device);
                server.addTarget(export);
            }
            catch (final Throwable t) {
                if (deviceFile != null) {
                    deviceFile.delete();
                }
                throw t;
            }
        }
        catch (final Throwable t) {
            server.stop();
            throw new InitializationError(t);
        }
    }

    @AfterClass
    public static void finiServer() {
        try {
            server.stop();
        }
        finally {
            if (deviceFile != null) {
                deviceFile.delete();
            }
        }
    }

    protected final File addNewExport() throws IOException {
        final File newdeviceFile = File.createTempFile("testDevice", null);
        final String name = newdeviceFile.getAbsolutePath();
        final NbdExport export = new NbdExport(name, Main.createNbdDeviceFile(name, size));
        server.addTarget(export);
        return newdeviceFile;
    }

    protected final void removeExport(final File export) {
        try {
            server.removeTarget(export.getAbsolutePath());
        }
        finally {
            export.delete();
        }
    }

}
