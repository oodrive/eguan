package io.eguan.vold;

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
import io.eguan.vold.VoldConfigurationContext;
import io.eguan.vold.model.VvrManagerMXBean;

import org.junit.Assert;
import org.junit.Test;

public class TestMultiVoldAddPeersNoQuorum extends TestMultiVoldAbstractPeers {

    @Test(expected = IllegalArgumentException.class)
    public void testAddPeerVoldNotStarted() throws Exception {
        vold1 = initVold(helper1, server1, voldMxBeanObjectName1);

        vold1.addPeer("34485ff6-d068-11e3-9fac-ab1f27d490d3", "127.0.0.1", 1234);
    }

    @Test
    public void testAddCommonPeer() throws Exception {
        vold1 = initVold(helper1, server1, voldMxBeanObjectName1);
        vold2 = initVold(helper2, server2, voldMxBeanObjectName2);
        vold3 = initVold(helper3, server3, voldMxBeanObjectName3);

        // change peers list to have no quorum
        changePeersList(vold1, null);
        changePeersList(vold2, null);
        changePeersList(vold3, null);

        vold1.start();
        vold2.start();

        Assert.assertTrue((VvrManagerMXBean) server1.waitMXBean(helper1.newVvrManagerObjectName()) != null);
        Assert.assertTrue((VvrManagerMXBean) server2.waitMXBean(helper2.newVvrManagerObjectName()) != null);

        vold1.addPeer("34485ff6-d068-11e3-9fac-ab1f27d490d3", "127.0.0.1", 1234);
        vold2.addPeer("34485ff6-d068-11e3-9fac-ab1f27d490d3", "127.0.0.1", 1234);

        final String expectedPeerList = "34485ff6-d068-11e3-9fac-ab1f27d490d3@127.0.0.1:1234";
        final VoldConfigurationContext voldContext = VoldConfigurationContext.getInstance();

        // check vold3 was added in list of peers of vold1 and vold2
        assertEquals(expectedPeerList, getPeersList(vold1, voldContext));
        assertEquals(expectedPeerList, getPeersList(vold2, voldContext));

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
