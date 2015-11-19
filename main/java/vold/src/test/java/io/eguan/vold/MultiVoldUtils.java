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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgNode;
import io.eguan.vold.PeersConfigKey;
import io.eguan.vold.ServerEndpointInetAddressConfigKey;
import io.eguan.vold.ServerEndpointPortConfigKey;
import io.eguan.vold.VoldLocation;
import io.eguan.vold.VoldSyncServer;
import io.eguan.vold.model.VvrManager;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;

public final class MultiVoldUtils {

    static final class VoldClientServer {
        private final VoldSyncServer voldSyncServer;
        private final MsgClientStartpoint msgClientStartpoint;

        VoldClientServer(final VoldSyncServer voldSyncServer, final MsgClientStartpoint msgClientStartpoint) {
            super();
            this.voldSyncServer = voldSyncServer;
            this.msgClientStartpoint = msgClientStartpoint;
        }

        public VoldSyncServer getVoldSyncServer() {
            return voldSyncServer;
        }

        public MsgClientStartpoint getMsgClientStartpoint() {
            return msgClientStartpoint;
        }
    }

    static final VoldClientServer initAndGetRemote(final UUID nodeId, final VvrManager vvrManager,
            final MetaConfiguration metaConfiguration) {

        VoldSyncServer syncServer;
        MsgClientStartpoint syncClient;
        final ArrayList<VoldLocation> peers = PeersConfigKey.getInstance().getTypedValue(metaConfiguration);

        Assert.assertNotNull(peers);

        boolean done = false;
        final InetAddress serverEndpoint = ServerEndpointInetAddressConfigKey.getInstance().getTypedValue(
                metaConfiguration);
        final int serverPort = ServerEndpointPortConfigKey.getInstance().getTypedValue(metaConfiguration).intValue();
        syncServer = new VoldSyncServer(nodeId, vvrManager, serverEndpoint, serverPort);

        try {
            final List<MsgNode> peerNodes = new ArrayList<>(peers.size());
            for (final VoldLocation voldLocation : peers) {
                peerNodes.add(new MsgNode(voldLocation.getNode(), voldLocation.getSockAddr()));
            }
            syncClient = new MsgClientStartpoint(nodeId, peerNodes);

            try {
                syncServer.start();
                syncClient.start();

                done = true;
            }
            finally {
                if (!done) {
                    // Can stop even if the client is not started
                    syncClient.stop();
                    syncClient = null;
                }
            }
        }
        finally {
            if (!done) {
                // Can stop even if the server is not started
                syncServer.stop();
                syncServer = null;
            }
        }

        Assert.assertTrue(done);

        return new VoldClientServer(syncServer, syncClient);
    }
}
