package io.eguan.nbdsrv;

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

import io.eguan.nbdsrv.ExportServer;
import io.eguan.nbdsrv.NbdExport;
import io.eguan.nbdsrv.NbdExportAttributes;
import io.eguan.nbdsrv.NbdServerConfig;
import io.eguan.nbdsrv.client.Client;
import io.eguan.nbdsrv.packet.NbdException;
import io.eguan.srv.TestAbstractServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import org.junit.Assert;

public abstract class TestNbdServerAbstract extends
        TestAbstractServer<ExportServer, NbdExport, NbdServerConfig, Client, NbdExportAttributes> {

    private static String TARGET_NAME = "test-eguan";
    private static String TARGET_NAME_UPPERCASE = "TEST-EGUAN";
    private static String BAD_TARGET_NAME = "bad-test-eguan";

    @Override
    protected NbdExport createTarget() {
        return new NbdExport(TARGET_NAME, TestNbdExportCreate.DUMMY_DEVICE);
    }

    @Override
    protected NbdExport createSecondTarget() {
        final String name2 = TARGET_NAME + "bis";
        return new NbdExport(name2, TestNbdExportCreate.DUMMY_DEVICE);
    }

    @Override
    protected NbdExport createTargetUppercase() {
        return new NbdExport(TARGET_NAME_UPPERCASE, TestNbdExportCreate.DUMMY_DEVICE);
    }

    @Override
    protected Client createClient() {
        final Client client = new Client(new InetSocketAddress(InetAddress.getLoopbackAddress(), 10809));
        return client;
    }

    @Override
    protected void connectClient(final Client client) {
        try {
            client.handshake();
        }
        catch (NbdException | IOException | InterruptedException e) {
            throw new IllegalStateException("Client can not handshake");
        }
        try {
            client.setExportName(TARGET_NAME);
        }
        catch (final ClosedChannelException e) {
            throw new IllegalArgumentException("Client can not connect to this target");
        }
        catch (NbdException | IOException | InterruptedException e) {
            // / ignored, do not test the client here
        }
    }

    @Override
    protected void disconnectClient(final Client client) {
        try {
            client.disconnect();
        }
        catch (final ClosedChannelException e) {
            throw new IllegalArgumentException("Client can not disconnect to this target");
        }
        catch (IOException | InterruptedException | NbdException e) {
            // ignored, do not test the client here
        }
    }

    @Override
    protected void removeTargetUppercaseAndCheck(final NbdExport target) {
        // try to remove the target with a lower case name
        Assert.assertSame(target, serverOrig.removeTarget(TARGET_NAME));
        Assert.assertNull(serverOrig.removeTarget(TARGET_NAME_UPPERCASE));

    }

    @Override
    protected void connectClientBadTarget(final Client client) throws IllegalArgumentException {
        try {
            client.handshake();
        }
        catch (NbdException | IOException | InterruptedException e) {
            throw new IllegalStateException("Client can not handshake");
        }
        try {
            client.setExportName(BAD_TARGET_NAME);
        }
        catch (final ClosedChannelException e) {
            throw new IllegalArgumentException("Client can not connect to this target");
        }
        catch (NbdException | IOException | InterruptedException e) {
            // ignored, do not test the client here
        }

    }

    @Override
    protected void disconnectClientBadTarget(final Client client) throws IllegalArgumentException {
        try {
            client.abortHandshake();
        }
        catch (final ClosedChannelException e) {
            throw new IllegalArgumentException("Client can not disconnect to this target");
        }
        catch (NbdException | IOException | InterruptedException e) {
            // ignored, do not test the client here
        }

    }

    @Override
    protected void checkTargetAttributes(final int targetCount, final NbdExport target, final int connectionCount) {
        final NbdExportAttributes[] attributesList = getServerTargetAttributes();
        Assert.assertEquals(targetCount, attributesList.length);
        boolean found = false;
        for (int i = 0; i < targetCount; i++) {
            final NbdExportAttributes attributes = attributesList[i];
            if (attributes.getName().equalsIgnoreCase(target.getTargetName())) {
                // Target must be found once
                Assert.assertFalse(found);
                found = true;

                Assert.assertEquals(connectionCount, attributes.getConnectionCount());
                Assert.assertEquals(target.getDevice().getSize(), attributes.getSize());
            }
        }
        Assert.assertTrue(found);

    }

}
