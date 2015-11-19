package io.eguan.vold.model;

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

import io.eguan.hash.HashAlgorithm;
import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VoldTestHelper.CompressionType;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * VOLD unit test parent class to read/write from/to a device.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
@RunWith(value = Parameterized.class)
public abstract class AbstractVoldIopsOnTarget extends AbstractVoldTest {

    protected final long size1 = 8192 * 1024L * 1024L;
    protected final String target = "dev0";
    protected final String target2 = "dev1";
    protected UUID devUuid;

    protected DeviceMXBean device;

    private static final int BLOCKSIZE1 = 4096;// 4096 == 64KB,
    private static final int BLOCKSIZE2 = 512;
    private static final int NUMBLOCKS1 = 4;
    private static final int NUMBLOCKS2 = 2;
    protected static final int LENGTH = 64 * 4; // 64KB == 4MB

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] {
                { CompressionType.front, HashAlgorithm.MD5, Integer.valueOf(BLOCKSIZE1), Integer.valueOf(NUMBLOCKS1) },
                { CompressionType.no, HashAlgorithm.TIGER, Integer.valueOf(BLOCKSIZE2), Integer.valueOf(NUMBLOCKS2) } };
        return Arrays.asList(data);
    }

    protected AbstractVoldIopsOnTarget(final CompressionType compression, final HashAlgorithm hash) throws Exception {
        super(compression, hash, null);
    }

    /**
     * Create and activate a device.
     * 
     * @throws Exception
     */
    @Before
    public final void createDevice() throws Exception {
        // Get root snapshot
        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        // Create device and activate device
        final String uuidTaskDevice = rootSnapshot.createDevice(target, size1);
        device = helper.getDevice(vvrUuid, uuidTaskDevice);
        Assert.assertEquals(size1, device.getSize());
        Assert.assertEquals(target, device.getName());
        device.setIscsiBlockSize(getBlockSize());
        helper.waitTaskEnd(vvrUuid, device.activateRW(), ManagementFactory.getPlatformMBeanServer());
        devUuid = UUID.fromString(device.getUuid());
    }

    /**
     * Gets block size.
     * 
     * @return the block size
     */
    protected abstract int getBlockSize();

    protected final DeviceMXBean createAnotherDevice() throws Exception {

        // Get root snapshot
        final SnapshotMXBean rootSnapshot = helper.getSnapshot(vvrUuid, rootUuid);
        // Create device and activate device
        final String uuidTaskDevice = rootSnapshot.createDevice(target2, size1);
        final DeviceMXBean device = helper.getDevice(vvrUuid, uuidTaskDevice);
        Assert.assertEquals(size1, device.getSize());
        Assert.assertEquals(target2, device.getName());
        device.setIscsiBlockSize(getBlockSize());
        helper.waitTaskEnd(vvrUuid, device.activateRW(), ManagementFactory.getPlatformMBeanServer());
        return device;
    }

}
