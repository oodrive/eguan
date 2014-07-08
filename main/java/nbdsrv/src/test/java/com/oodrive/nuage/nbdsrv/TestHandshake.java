package com.oodrive.nuage.nbdsrv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import org.junit.Test;

import com.oodrive.nuage.nbdsrv.client.Client;
import com.oodrive.nuage.nbdsrv.packet.ExportFlagsPacket;
import com.oodrive.nuage.nbdsrv.packet.NbdException;

public class TestHandshake extends TestNbdAbstract {

    @Test
    public void testStartAndAbort() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // Abort
        client.abortHandshake();
    }

    @Test
    public void testListAndAbort() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // get list
        final String[] list = client.getList();
        assertEquals(deviceFileName, list[0]);
        // Abort
        client.abortHandshake();
    }

    @Test
    public void testSeveralListAndAbort() throws IOException, InterruptedException, NbdException {

        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();

        for (int i = 0; i < 5; i++) {
            // get list
            final String[] list = client.getList();
            assertEquals(deviceFileName, list[0]);
        }
        // Abort
        client.abortHandshake();
    }

    @Test
    public void testListAndConnect() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // get list
        final String[] list = client.getList();
        assertEquals(deviceFileName, list[0]);
        // Connect
        client.setExportName(list[0]);
        client.disconnect();
    }

    @Test
    public void testMultipleTarget() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
        final File newExport = addNewExport();
        try {
            // Start handshake
            client.handshake();
            // get list
            final String[] list = client.getList();

            // Check if the 2 export are here
            boolean found1 = false;
            boolean found2 = false;
            for (final String name : list) {
                if (deviceFileName.equals(name)) {
                    found1 = true;
                }
                else if (newExport.getAbsolutePath().equals(name)) {
                    found2 = true;
                }
            }
            assertTrue(found1);
            assertTrue(found2);

            // Connect
            client.setExportName(list[1]);
            client.disconnect();
        }
        finally {
            if (newExport != null) {
                removeExport(newExport);
            }
        }
    }

    @Test
    public void testConnect() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // Connect
        client.setExportName(deviceFileName);

        assertEquals(deviceFileName, client.getExportName());
        assertEquals(size, client.getExportSize());
        assertEquals(ExportFlagsPacket.NBD_FLAG_HAS_FLAGS | ExportFlagsPacket.NBD_FLAG_SEND_TRIM,
                client.getExportFlags());

        client.disconnect();
    }

    @Test
    public void testConnectReconnect() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // Connect
        client.setExportName(deviceFileName);
        assertEquals(deviceFileName, client.getExportName());

        client.disconnect();
        assertEquals("", client.getExportName());

        // Start handshake
        client.handshake();
        // Connect
        client.setExportName(deviceFileName);
        assertEquals(deviceFileName, client.getExportName());
        client.disconnect();

    }

    @Test(expected = ClosedChannelException.class)
    public void testConnectBadName() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // Connect
        client.setExportName("Bad Name");
    }

    @Test
    public void testConnectNameUpperCAse() throws IOException, InterruptedException, NbdException {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        // Start handshake
        client.handshake();
        // Connect
        client.setExportName(deviceFileName.toUpperCase());
        client.disconnect();

    }

    @Test
    public void testConnectTwoClients() throws IOException, InterruptedException, NbdException {
        final Client client1 = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
        final Client client2 = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));

        client1.handshake();
        client1.setExportName(deviceFileName);

        client2.handshake();
        client2.setExportName(deviceFileName);

        client1.disconnect();
        client2.disconnect();
    }
}
