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
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class Main {

    static final long size = 8192 * 1024L * 1024L;

    public static void main(final String[] args) {
        File deviceFile = null;
        try {
            final NbdServer server = new NbdServer(InetAddress.getByName("0.0.0.0"));
            server.start();

            deviceFile = File.createTempFile("testDevice", null);
            final String deviceFileName = deviceFile.getAbsolutePath();

            final NbdExport export = new NbdExport(deviceFileName, createNbdDeviceFile(deviceFileName, size));

            server.addTarget(export);

            // Keep the server running
            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (final InterruptedException e) {
                System.out.println("Interrupted");
                System.exit(0);
            }

        }
        catch (final IOException e) {
            e.printStackTrace();
        }
        finally {
            if (deviceFile != null) {
                deviceFile.delete();
            }
        }
    }

    private static class ReadOnlyDeviceFile extends NbdDeviceFile {

        ReadOnlyDeviceFile(final FileChannel fileChannel, final String path) {
            super(fileChannel, path);
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }

    public final static NbdDeviceFile createReadOnlyDeviceFile(final String path, final long size) throws IOException {

        final File file = new File(path);
        // Set file size
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(size);
        }

        // Create and add target
        final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                StandardOpenOption.CREATE);

        return new ReadOnlyDeviceFile(fileChannel, path);
    }

    public final static NbdDeviceFile createNbdDeviceFile(final String path, final long size) throws IOException {

        final File file = new File(path);
        // Set file size
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(size);
        }

        // Create and add target
        final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        return new NbdDeviceFile(fileChannel, path);
    }
}
