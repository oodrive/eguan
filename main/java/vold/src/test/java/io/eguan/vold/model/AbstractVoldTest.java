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

import io.eguan.dtx.DtxTaskApiAbstract.TaskKeeperParameters;
import io.eguan.hash.HashAlgorithm;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VoldTestHelper.CompressionType;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import javax.management.JMException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch a VOLD and create a VVR.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public abstract class AbstractVoldTest {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractVoldTest.class);

    protected final VoldTestHelper helper;
    private boolean started = false;

    protected UUID vvrUuid;
    protected UUID rootUuid;

    private final TaskKeeperParameters parameters;

    protected AbstractVoldTest() throws Exception {
        this(CompressionType.no, HashAlgorithm.MD5, null);
    }

    protected AbstractVoldTest(final TaskKeeperParameters parameters) throws Exception {
        this(CompressionType.no, HashAlgorithm.MD5, parameters);
    }

    protected AbstractVoldTest(final CompressionType compression, final HashAlgorithm hash,
            final TaskKeeperParameters parameters) throws Exception {
        super();
        this.helper = new VoldTestHelper(compression, hash, Boolean.TRUE);
        this.parameters = parameters;

        LOGGER.info("New test compressionType=" + compression + ", hash=" + hash);
    }

    /**
     * Create and activate a device.
     * 
     * @throws Exception
     */
    @Before
    public final void createVvr() throws Exception {
        helper.createTemporary(parameters);
        helper.start();
        started = true;

        Assert.assertTrue(helper.waitMXBeanRegistration(helper.newVvrManagerObjectName()));

        // Create VVR
        final VvrMXBean vvr = helper.createVvr("TestVOLD", "Test VOLD start/stop, read/write");
        vvrUuid = UUID.fromString(vvr.getUuid());
        // Get root snapshot
        final Set<SnapshotMXBean> snapshots = helper.getSnapshots(vvrUuid);
        Assert.assertEquals(1, snapshots.size());
        final SnapshotMXBean rootSnapshot = snapshots.iterator().next();
        rootUuid = UUID.fromString(rootSnapshot.getUuid());
    }

    protected final void restartVold() throws JMException, IOException, InterruptedException {
        helper.stop();
        started = false;
        helper.start();
        started = true;

        Assert.assertTrue(helper.waitMXBeanRegistration(helper.newVvrManagerObjectName()));
    }

    @After
    public final void deleteVvr() throws Exception {
        try {
            if (started) {
                helper.stop();
            }
        }
        finally {
            helper.destroy();
        }
    }

}
