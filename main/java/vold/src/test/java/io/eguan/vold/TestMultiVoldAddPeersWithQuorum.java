package io.eguan.vold;

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

import static org.junit.Assert.assertEquals;
import io.eguan.vold.VoldConfigurationContext;
import io.eguan.vold.VoldLocation;
import io.eguan.vold.model.VvrManagerMXBean;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

public class TestMultiVoldAddPeersWithQuorum extends TestMultiVoldAbstractPeers {

    @Test
    public void testConnectTreePeers() throws Exception {
        vold1 = initVold(helper1, server1, voldMxBeanObjectName1);
        vold2 = initVold(helper2, server2, voldMxBeanObjectName2);
        vold3 = initVold(helper3, server3, voldMxBeanObjectName3);

        // change peers list to have a quorum with node1 and node2
        // node3 is configured with correct peers list to be add it to this quorum
        final VoldLocation node1 = vold1.getVoldLocation();
        final VoldLocation node2 = vold2.getVoldLocation();
        final VoldLocation node3 = vold3.getVoldLocation();

        final ArrayList<VoldLocation> peers1 = new ArrayList<>();
        final ArrayList<VoldLocation> peers2 = new ArrayList<>();
        final ArrayList<VoldLocation> peers3 = new ArrayList<>();
        peers1.add(node2);
        peers2.add(node1);
        peers3.add(node1);
        peers3.add(node2);

        changePeersList(vold1, peers1);
        changePeersList(vold2, peers2);
        changePeersList(vold3, peers3);

        // start quorum (node1 and node2)
        vold1.start();
        vold2.start();

        Assert.assertTrue((VvrManagerMXBean) server1.waitMXBean(helper1.newVvrManagerObjectName()) != null);
        Assert.assertTrue((VvrManagerMXBean) server2.waitMXBean(helper2.newVvrManagerObjectName()) != null);

        // add node3 to quorum (node1 and node2)
        // randomly choose between node1 and node2
        // for adding node3 as it shall be the same
        if (Math.random() < 0.5) {
            addPeer(vold1, node3);
        }
        else {
            addPeer(vold2, node3);
        }

        final String expectedPeerList1 = node2.toString() + "," + node3.toString();
        final String expectedPeerList2 = node1.toString() + "," + node3.toString();
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        // check node3 was added in list of peers from quorum (node1 and node2)
        assertEquals(expectedPeerList1, getPeersList(vold1, voldContext));
        assertEquals(expectedPeerList2, getPeersList(vold2, voldContext));

        vold3.start();

        Assert.assertTrue((VvrManagerMXBean) server3.waitMXBean(helper3.newVvrManagerObjectName()) != null);

        vold1.stop();
        vold2.stop();
        vold3.stop();

        for (int i = 0; i < nodeStarted.length; i++) {
            nodeStarted[i] = false;
        }
    }
}
