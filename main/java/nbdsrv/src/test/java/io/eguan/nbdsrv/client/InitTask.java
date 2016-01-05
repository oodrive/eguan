package io.eguan.nbdsrv.client;

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

import io.eguan.nbdsrv.packet.GlobalFlagsPacket;
import io.eguan.nbdsrv.packet.InitPacket;
import io.eguan.nbdsrv.packet.NbdException;
import io.eguan.nbdsrv.packet.OptionPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InitTask implements Callable<Void> {

    /** NBD Client */
    private final NbdClient nbdClient;
    private static final Logger LOGGER = LoggerFactory.getLogger(InitTask.class);

    InitTask(final NbdClient nbdClient) {
        this.nbdClient = nbdClient;
    }

    @Override
    public final Void call() throws IOException, NbdException {
        LOGGER.debug("enter INITIALIZATION phase");
        // Read init packet and save global remote flags.

        final InitPacket initpacket = readInitPacket(nbdClient);
        nbdClient.setGlobalRemoteFlags(initpacket.getGlobalFlags());
        // Send its own global flags
        sendGlobalFlags(nbdClient);
        return null;
    }

    /**
     * Read the init packet.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the write on the socket failed
     * @throws NbdException
     * 
     */
    private final InitPacket readInitPacket(final NbdClient nbdClient) throws IOException, NbdException {
        LOGGER.debug("Read Init Packet");

        final ByteBuffer dst = InitPacket.allocateHeader();
        try {
            nbdClient.readSocket(dst);
            return InitPacket.deserialize(dst);
        }
        finally {
            GlobalFlagsPacket.release(dst);
        }
    }

    /**
     * Send Global flags for the client.
     * 
     * @param nbdClient
     *            the NBD client
     * @throws IOException
     *             if an I/O error occurs
     */
    private final void sendGlobalFlags(final NbdClient nbdClient) throws IOException {
        LOGGER.debug("Send global flags");

        final ByteBuffer src = GlobalFlagsPacket.serialize(GlobalFlagsPacket.NBD_FLAG_FIXED_NEWSTYLE);
        try {
            nbdClient.writeSocket(src);
        }
        finally {
            OptionPacket.release(src);
        }

    }

}
