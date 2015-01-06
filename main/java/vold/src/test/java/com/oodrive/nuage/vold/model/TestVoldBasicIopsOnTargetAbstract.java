package com.oodrive.nuage.vold.model;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.junit.Test;

import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.srv.BasicIopsTestHelper;
import com.oodrive.nuage.srv.ClientBasicIops;
import com.oodrive.nuage.vold.model.VoldTestHelper.CompressionType;

/**
 * Read/write iSCSI target via the jSCSI initiator.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 */
public abstract class TestVoldBasicIopsOnTargetAbstract extends AbstractVoldIopsOnTarget {

    private final ClientBasicIops client;
    private final BasicIopsTestHelper basicIoHelper;

    protected abstract ClientBasicIops initClient();

    @Override
    public int getBlockSize() {
        return basicIoHelper.getBlockSize();
    }

    public TestVoldBasicIopsOnTargetAbstract(final CompressionType compressionType, final HashAlgorithm hash,
            final Integer blockSize, final Integer numBlocks) throws Exception {
        super(compressionType, hash);
        basicIoHelper = new BasicIopsTestHelper(blockSize.intValue(), numBlocks.intValue(), LENGTH);
        client = initClient();
    }

    @Test
    public void testCreateStopRestore1() throws Exception {

        final File dataDump = basicIoHelper.initiatorReadWriteData(client, target, size1);
        try {
            final ObjectName deviceObjectName = helper.getDeviceObjectName(vvrUuid, devUuid);

            // Restart VOLD
            restartVold();

            assertTrue(helper.waitMXBeanRegistration(deviceObjectName));

            basicIoHelper.initiatorReadData(dataDump, client, target, size1);
        }
        finally {
            dataDump.delete();
        }

    }

    @Test
    public void testCloneDevice() throws Exception {
        final File dataDump = basicIoHelper.initiatorReadWriteData(client, target, size1);
        try {

            final String task = device.clone("clonedev0", "clone desc");
            final DeviceMXBean clone = helper.getDevice(vvrUuid, task);

            assertEquals(size1, clone.getSize());
            clone.setIscsiBlockSize(getBlockSize());
            helper.waitTaskEnd(vvrUuid, clone.activateRW(), ManagementFactory.getPlatformMBeanServer());
            basicIoHelper.initiatorReadData(dataDump, client, "clonedev0", size1);
        }
        finally {
            dataDump.delete();
        }
    }

    @Test
    public void testReadTwoDevices() throws Exception {

        createAnotherDevice();

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<File> future1 = basicIoHelper.multiThreadRW(executor, target, client, size1);

            final ClientBasicIops client2 = initClient();
            final Future<File> future2 = basicIoHelper.multiThreadRW(executor, target2, client2, size1);

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
