package io.eguan.nbdsrv.client;

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

import io.eguan.srv.ClientBasicIops;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class NbdClientBasicIops implements ClientBasicIops {

    private final Client nbdClient;

    public NbdClientBasicIops(final int port) {
        super();
        final InetSocketAddress address = new InetSocketAddress("127.0.0.1", port);
        this.nbdClient = new Client(address);
    }

    @Override
    public void write(final String targetName, final ByteBuffer src, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws Exception {
        // transferLength is useless for nbd
        nbdClient.write(src, logicalBlockAddress * blockSize);
    }

    @Override
    public void read(final String targetName, final ByteBuffer dst, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws Exception {
        // transferLength is useless for nbd
        nbdClient.read(dst, logicalBlockAddress * blockSize);
    }

    @Override
    public void createSession(final String targetName) throws Exception {
        nbdClient.handshake();
        nbdClient.setExportName(targetName);
    }

    @Override
    public void closeSession(final String targetName) throws Exception {
        nbdClient.disconnect();
    }

    @Override
    public void checkCapacity(final String target, final long size) throws Exception {
    }

}
