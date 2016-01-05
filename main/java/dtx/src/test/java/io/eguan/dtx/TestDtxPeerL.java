package io.eguan.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import static io.eguan.dtx.DtxNodeState.INITIALIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxNodeState;
import io.eguan.dtx.DtxPeerAdm;
import io.eguan.dtx.DtxManager.DtxLocalNode;
import io.eguan.dtx.DtxPeerAdm.DtxPeerStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class TestDtxPeerL extends AbstractCommonClusterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDtxPeerL.class);

    /**
     * Tests dtx manager DtxLocalNode inner class API when all the nodes are started.
     * 
     */
    @Test
    public final void testCheckPeers() {

        LOGGER.info("Executing");
        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        for (final DtxManager dtxManager : dtxMgrs) {
            final DtxLocalNode localNode = dtxManager.new DtxLocalNode();
            assertEquals(dtxManager.getNodeId().toString(), localNode.getUuid());
            assertEquals("127.0.0.1", localNode.getIpAddress());
            assertFalse(0 == localNode.getPort());
            assertEquals(DtxNodeState.STARTED, localNode.getStatus());
            assertEquals(0, localNode.getNextAtomicLong());
            assertEquals(0, localNode.getCurrentAtomicLong());

            final DtxPeerAdm[] peers = localNode.getPeers();
            boolean found = false;

            final ArrayList<DtxManager> otherMgrs = new ArrayList<DtxManager>(dtxMgrs);
            otherMgrs.remove(dtxManager);

            for (final DtxManager otherMgr : otherMgrs) {
                for (final DtxPeerAdm peer : peers) {
                    if (otherMgr.getNodeId().toString().equals(peer.getUuid())) {
                        assertEquals("127.0.0.1", peer.getIpAddress());
                        assertFalse(0 == peer.getPort());
                        assertEquals(DtxPeerStatus.ONLINE, peer.getStatus());
                        found = true;
                    }
                }
            }
            assertTrue(found);
        }

    }

    /**
     * Tests dtx manager DtxLocalNode inner class API when a node is stopped.
     * 
     * @throws InterruptedException
     *             if interrupted while waiting for results
     */
    @Test
    public final void testGetPeersStatusStopOneNode() throws InterruptedException {

        LOGGER.info("Executing");
        final Set<DtxManager> dtxMgrs = DTX_MGR_JOURNAL_MAP.keySet();

        final ArrayList<DtxManager> dtxMgrKillList = new ArrayList<DtxManager>(dtxMgrs);
        Collections.shuffle(dtxMgrKillList);

        // stops one node out of order, executes some transactions and then lets it catch up
        for (final DtxManager currVictimDtxMgr : dtxMgrKillList) {

            currVictimDtxMgr.stop();
            assertEquals(INITIALIZED, currVictimDtxMgr.getStatus());

            // All peers are online except the victim
            for (final DtxManager dtxMgr : dtxMgrs) {
                if (dtxMgr == currVictimDtxMgr) {
                    continue;
                }
                assertTrue(waitClusterMembers(dtxMgr, dtxMgrs.size() - 1));

                for (final DtxPeerAdm peer : dtxMgr.new DtxLocalNode().getPeers()) {
                    if (peer.getUuid().equals(currVictimDtxMgr.getNodeId().toString())) {
                        assertEquals(DtxPeerStatus.OFFLINE, peer.getStatus());
                    }
                    else {
                        assertEquals(DtxPeerStatus.ONLINE, peer.getStatus());
                    }
                }
            }

            currVictimDtxMgr.start();
            assertEquals(DtxNodeState.STARTED, currVictimDtxMgr.getStatus());

            // Now all peers are online
            for (final DtxManager dtxMgr : dtxMgrs) {
                assertTrue(waitClusterMembers(dtxMgr, dtxMgrs.size()));

                for (final DtxPeerAdm peer : dtxMgr.new DtxLocalNode().getPeers()) {
                    assertEquals(DtxPeerStatus.ONLINE, peer.getStatus());
                }
            }
        }
    }

    private boolean waitClusterMembers(final DtxManager dtxManager, final int size) throws InterruptedException {
        final HazelcastInstance hzInstance = Hazelcast.getHazelcastInstanceByName(dtxManager.getNodeId().toString());

        assertNotNull(hzInstance);
        int nodesCount = 0;
        int retry = 0;

        while ((nodesCount != size) && (retry++ < 10)) {
            nodesCount = hzInstance.getCluster().getMembers().size();
            Thread.sleep(200);
        }
        if (nodesCount != size) {
            return false;
        }
        else {
            return true;
        }
    }
}
