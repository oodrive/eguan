package com.oodrive.nuage.vvr.repository.core.api;

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

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.vvr.configuration.AbstractVvrCommonFixture;
import com.oodrive.nuage.vvr.persistence.repository.NrsRepository;

public abstract class TestDeviceAbstract extends AbstractVvrCommonFixture {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TestDeviceAbstract.class);

    /** Name of the device. */
    private static final String DEVICE_NAME = "test device";

    private VersionedVolumeRepository repository = null;
    protected Device device = null;
    protected int deviceBlockSize;

    protected TestDeviceAbstract() {
        super();
    }

    protected TestDeviceAbstract(final boolean helpersErr) {
        super(helpersErr);
    }

    /**
     * Create the device to test.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Before
    public void createDevice() throws InterruptedException, ExecutionException {

        boolean done = false;

        // Nrs repository
        final NrsRepository.Builder vvrBuilder = new NrsRepository.Builder();
        vvrBuilder.configuration(getConfiguration());
        vvrBuilder.uuid(UUID.randomUUID());
        vvrBuilder.ownerId(UUID.randomUUID());
        vvrBuilder.nodeId(UUID.randomUUID());
        vvrBuilder.rootUuid(UUID.randomUUID());
        repository = vvrBuilder.create();

        Assert.assertNotNull(repository);

        try {
            repository.init();
            try {
                repository.start(true);
                try {
                    // Use the default block size TODO get VVR block size?
                    final long deviceSize = DEFAULT_TOTAL_BLOCK_COUNT * getDefaultBlockSize();
                    final Snapshot parentSnapshot = repository.getRootSnapshot();
                    device = parentSnapshot.createDevice(DEVICE_NAME, deviceSize).get();

                    Assert.assertNotNull(device);
                    Assert.assertEquals(DEVICE_NAME, device.getName());
                    Assert.assertEquals(null, device.getDescription());
                    Assert.assertEquals(repository, device.getVvr());
                    Assert.assertEquals(parentSnapshot.getUuid(), device.getParent());
                    Assert.assertEquals(deviceSize, device.getSize());

                    device.activate().get();
                    deviceBlockSize = device.getBlockSize();

                    done = true;
                }
                finally {
                    if (!done) {
                        repository.stop(true);
                    }
                }
            }
            finally {
                if (!done) {
                    repository.fini();
                }
            }
        }
        finally {
            if (!done) {
                repository = null;
                device = null;
            }
        }
    }

    /**
     * Release resources.
     */
    @After
    public void deleteDevice() {
        if (device != null) {
            try {
                device.deactivate();
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to deactivate device " + device, t);
            }
            try {
                device.delete().get();
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to delete device " + device, t);
            }
            device = null;
        }
        if (repository != null) {
            try {
                repository.stop(false);
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to stop repository " + repository, t);
            }
            try {
                repository.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to fini repository " + repository, t);
            }
            repository = null;
        }
    }
}
