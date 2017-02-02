package io.eguan.dtx;

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

import static io.eguan.dtx.DtxTestHelper.newDtxManagerConfig;
import static org.junit.Assert.assertEquals;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxManagerConfig;
import io.eguan.dtx.DtxNode;
import io.eguan.dtx.journal.JournalRotationManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;

/**
 * Superclass for all test classes using one common cluster for all tests.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public abstract class AbstractCommonClusterTest {

    /**
     * The number of nodes to use.
     */
    protected static final int NB_OF_NODES = 3;

    /**
     * A {@link Map} of {@link DtxManager}s mapped to the temporary directories containing their data.
     */
    protected static final Map<DtxManager, Path> DTX_MGR_JOURNAL_MAP = new ConcurrentHashMap<DtxManager, Path>();

    /**
     * The {@link JournalRotationManager} to use for setting up journals.
     */
    protected static final JournalRotationManager SETUP_ROT_MGR = new JournalRotationManager(0);

    /**
     * Sets up common fixture.
     * 
     * @throws InitializationError
     *             if creating temporary directories fails
     */
    @BeforeClass
    public static final void setUp() throws InitializationError {
        final Set<DtxNode> peerList = DtxTestHelper.newRandomCluster(NB_OF_NODES);

        // constructs DtxManager instances
        for (final DtxNode currPeer : peerList) {
            final Path journalDir;
            try {
                journalDir = Files.createTempDirectory(TestDtxManagerSynchronizationL.class.getSimpleName());
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
            final ArrayList<DtxNode> otherPeers = new ArrayList<DtxNode>(peerList);
            otherPeers.remove(currPeer);
            final DtxManagerConfig dtxConfig = newDtxManagerConfig(currPeer, journalDir,
                    otherPeers.toArray(new DtxNode[otherPeers.size()]));
            final DtxManager newDtxMgr = new DtxManager(dtxConfig);
            newDtxMgr.init();
            newDtxMgr.start();
            DTX_MGR_JOURNAL_MAP.put(newDtxMgr, dtxConfig.getJournalDirectory());
        }

        SETUP_ROT_MGR.start();

    }

    /**
     * Tears down common fixture.
     * 
     * @throws InitializationError
     *             if removing temporary files fails
     */
    @AfterClass
    public static final void tearDown() throws InitializationError {
        final ArrayList<Throwable> exceptionList = new ArrayList<Throwable>();

        SETUP_ROT_MGR.stop();

        for (final DtxManager currMgr : DTX_MGR_JOURNAL_MAP.keySet()) {
            assertEquals(0, currMgr.getNbOfPendingRequests());
            currMgr.stop();
            currMgr.fini();

            try {
                io.eguan.utils.Files.deleteRecursive(DTX_MGR_JOURNAL_MAP.remove(currMgr).getParent());
            }
            catch (final IOException e) {
                exceptionList.add(e);
            }
        }

        if (!exceptionList.isEmpty()) {
            throw new InitializationError(exceptionList);
        }
    }

}
