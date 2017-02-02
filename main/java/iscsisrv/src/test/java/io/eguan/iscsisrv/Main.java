package io.eguan.iscsisrv;

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

import io.eguan.iscsisrv.IscsiDevice;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiTarget;
import io.eguan.srv.AbstractDeviceFile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple main to test JMX support of the iSCSI server with jconsole, jvisualvm or other tools.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 */
public class Main {

    static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        // iSCSI server
        final IscsiServer iscsiServer = new IscsiServer(InetAddress.getLoopbackAddress()/* , 56270 */);
        {
            final ObjectName mxbeanName = new ObjectName("io.eguan.iscsisrv:type=iSCSIsrv");
            mbs.registerMBean(iscsiServer, mxbeanName);
        }

        // target controller
        {
            final ScsiTargetController controller = new ScsiTargetController(iscsiServer);
            final ObjectName mxbeanName = new ObjectName("io.eguan.iscsisrv:type=iSCSItgt");
            mbs.registerMBean(controller, mxbeanName);
        }

        LOGGER.info("Waiting...");
        Thread.sleep(Long.MAX_VALUE);
    }

}

final class ScsiDeviceFile extends AbstractDeviceFile implements IscsiDevice {

    ScsiDeviceFile(final FileChannel fileChannel, final String path) {
        super(fileChannel, path);
    }

    @Override
    public final int getBlockSize() {
        return 4096;
    }

    @Override
    public final boolean isReadOnly() {
        return false;
    }
}

/**
 * Add and remove targets
 * 
 */
final class ScsiTargetController implements ScsiTargetControllerMXBean {

    private static String TARGET_DEVICE_HEADER = "iqn.2000-06.com.oodrive:";

    private final IscsiServer iscsiServer;

    private static final String pathToTargetName(final String path) {
        return path.replace('/', '-');
    }

    ScsiTargetController(final IscsiServer iscsiServer) {
        super();
        this.iscsiServer = iscsiServer;
    }

    @Override
    public final void addTarget(final String path, final long size) throws IOException {
        // Check size
        if (size <= 0) {
            throw new IllegalArgumentException("size=" + size);
        }

        final File file = new File(path);
        // Set file size
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(size);
        }

        // Create and add target
        final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        final ScsiDeviceFile device = new ScsiDeviceFile(fileChannel, path);
        final IscsiTarget target = IscsiTarget.newIscsiTarget(TARGET_DEVICE_HEADER + pathToTargetName(path), "File "
                + path, device);
        final IscsiTarget targetPrev = iscsiServer.addTarget(target);
        if (targetPrev != null) {
            targetPrev.close();
        }
        Main.LOGGER.info("Target added: '" + path + "' size=" + size);
    }

    @Override
    public final void removeTarget(final String path) throws IOException {
        final IscsiTarget target = iscsiServer.removeTarget(TARGET_DEVICE_HEADER + pathToTargetName(path));
        if (target != null) {
            target.close();
            Main.LOGGER.info("Target removed: '" + path + "'");
        }
    }
}
