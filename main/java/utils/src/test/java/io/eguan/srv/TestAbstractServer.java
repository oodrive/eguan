package io.eguan.srv;

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

import io.eguan.srv.AbstractServer;
import io.eguan.srv.AbstractServerConfig;
import io.eguan.srv.AbstractServerMXBean;
import io.eguan.srv.DeviceTarget;

import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

public abstract class TestAbstractServer<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig, C, A> {

    /** Server to test. A MXBean proxy or a server */
    protected AbstractServerMXBean server;
    /** Real server (target management) */
    protected AbstractServer<S, T, K> serverOrig;

    @Test(expected = NullPointerException.class)
    public void testAddNullTarget() {
        Assert.assertFalse(server.isStarted());
        serverOrig.addTarget(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNullTarget() {
        Assert.assertFalse(server.isStarted());
        serverOrig.removeTarget(null);
    }

    @Test
    public void testAddRemoveTargets() {
        Assert.assertFalse(server.isStarted());
        final T target1 = createTarget();
        final T target2 = createTarget();
        Assert.assertNull(serverOrig.addTarget(target1));
        Assert.assertSame(target1, serverOrig.addTarget(target2));

        Assert.assertSame(target2, serverOrig.removeTarget(target1.getTargetName()));
        Assert.assertNull(serverOrig.removeTarget(target1.getTargetName()));
    }

    @Test
    public void testChangeAddressServerStarted() throws UnknownHostException {

        // Server Started
        Assert.assertFalse(server.isStarted());
        server.start();
        try {
            final String oldAddress = server.getAddress();
            if (oldAddress.equals("127.0.0.1")) {
                server.setAddress("0.0.0.0");
            }
            else {
                server.setAddress("127.0.0.1");
            }
            Assert.assertTrue(server.isRestartRequired());
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testChangeAddressServerNotStarted() throws UnknownHostException {
        // Server not started
        Assert.assertFalse(server.isStarted());
        final String oldAddress = server.getAddress();
        if (oldAddress.equals("127.0.0.1")) {
            server.setAddress("0.0.0.0");
        }
        else {
            server.setAddress("127.0.0.1");
        }
        Assert.assertFalse(server.isRestartRequired());
    }

    @Test
    public void testSameAddressServerStarted() throws UnknownHostException {
        // Same ip address
        server.start();
        try {
            final String oldAddress = server.getAddress();
            server.setAddress(oldAddress);
            Assert.assertFalse(server.isRestartRequired());

            if (oldAddress.equals("127.0.0.1")) {
                server.setAddress("0.0.0.0");
            }
            else {
                server.setAddress("127.0.0.1");
            }
            Assert.assertTrue(server.isRestartRequired());

            server.setAddress(oldAddress);
            Assert.assertFalse(server.isRestartRequired());

        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testChangePortServerStarted() throws UnknownHostException {

        // Server Started
        Assert.assertFalse(server.isStarted());
        server.start();
        try {
            final int oldPort = server.getPort();
            server.setPort(oldPort + 2);
            Assert.assertTrue(server.isRestartRequired());
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testChangePortServerNotStarted() throws UnknownHostException {
        // Server not started
        Assert.assertFalse(server.isStarted());
        final int oldPort = server.getPort();
        server.setPort(oldPort + 2);
        Assert.assertFalse(server.isRestartRequired());
    }

    @Test
    public void testSamePortServerStarted() throws UnknownHostException {
        server.start();
        try {
            final int oldPort = server.getPort();
            server.setPort(oldPort);
            Assert.assertFalse(server.isRestartRequired());

            server.setPort(oldPort + 2);
            Assert.assertTrue(server.isRestartRequired());

            server.setPort(oldPort);
            Assert.assertFalse(server.isRestartRequired());

        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testStartStop() throws Exception {

        Assert.assertFalse(server.isStarted());
        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            // Let the server run a little
            Thread.yield();
            Thread.sleep(500);
            Thread.yield();

            Assert.assertTrue(server.isStarted());
        }
        finally {
            server.stop();
        }
        Assert.assertFalse(server.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public void testReStart() {
        Assert.assertFalse(server.isStarted());
        server.start();
        try {
            Assert.assertTrue(server.isStarted());
            server.start();
        }
        finally {
            server.stop();
        }
        // Should not get here
        throw new AssertionError("Not reached");
    }

    @Test
    public void testStartStopRestart() throws Exception {

        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();
        Assert.assertNull(serverOrig.addTarget(target));

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            // Let the server run a little
            Thread.yield();
            Thread.sleep(500);
            Thread.yield();

            Assert.assertTrue(server.isStarted());

            server.stop();

            Assert.assertFalse(server.isStarted());

            server.start();

            Assert.assertTrue(server.isStarted());

            // check if connection is possible
            final C client = createClient();
            connectClient(client);
            disconnectClient(client);

            Assert.assertSame(target, serverOrig.removeTarget(target.getTargetName()));
            Assert.assertNull(serverOrig.removeTarget(target.getTargetName()));

        }
        finally {
            server.stop();
        }
        Assert.assertFalse(server.isStarted());
    }

    @Test
    public void testTargetDevice() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();
        Assert.assertNull(serverOrig.addTarget(target));

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            final C client = createClient();
            connectClient(client);
            disconnectClient(client);

            Assert.assertSame(target, serverOrig.removeTarget(target.getTargetName()));
            Assert.assertNull(serverOrig.removeTarget(target.getTargetName()));
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testAddTargetIgnoreCase() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device with an uppercase name
        final T target = createTargetUppercase();

        server.start();
        try {
            Assert.assertTrue(server.isStarted());
            Assert.assertNull(serverOrig.addTarget(target));
            removeTargetUppercaseAndCheck(target);

        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testConnectTargetIgnoreCase() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTargetUppercase();

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            Assert.assertNull(serverOrig.addTarget(target));

            final C client = createClient();
            connectClient(client);
            disconnectClient(client);

            removeTargetUppercaseAndCheck(target);

        }
        finally {
            server.stop();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTargetBadDevice() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();
        Assert.assertNull(serverOrig.addTarget(target));

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            final C client = createClient();
            try {
                connectClientBadTarget(client);
                disconnectClientBadTarget(client);
                // Should not get here
                throw new AssertionError("Not reached");
            }
            catch (final IllegalArgumentException e) {
                // Expected exception
                throw e;
            }
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testTargetDeviceAddedRemovedOnline() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();

        server.start();
        try {
            Assert.assertTrue(server.isStarted());
            Assert.assertNull(serverOrig.addTarget(target));

            final C client = createClient();
            connectClient(client);

            // Remove target although a session is opened
            Assert.assertSame(target, serverOrig.removeTarget(target.getTargetName()));
            Assert.assertNull(serverOrig.removeTarget(target.getTargetName()));

            try {
                disconnectClient(client);
            }
            catch (final IllegalArgumentException e) {
                // Get the expected exceptions or not: ignored, do not test the initiator here
            }
        }
        finally {
            server.stop();
        }
    }

    /**
     * Stops the server while a session is opened.
     * 
     * @throws Exception
     */
    @Test
    public void testTargetDeviceStopSession() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();
        Assert.assertNull(serverOrig.addTarget(target));

        boolean stopped = false;

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            final C client = createClient();
            connectClient(client);
            try {
                server.stop();
                stopped = true;
            }
            finally {
                try {
                    disconnectClient(client);
                }
                catch (final IllegalArgumentException e) {
                    // Get the expected exceptions or not: ignored, do not test the initiator here
                }
            }

            Assert.assertSame(target, serverOrig.removeTarget(target.getTargetName()));
            Assert.assertNull(serverOrig.removeTarget(target.getTargetName()));
        }
        finally {
            if (!stopped)
                server.stop();
        }
    }

    @Test
    public void testOpenTwoSessions() throws Exception {
        Assert.assertFalse(server.isStarted());

        // Add target device
        final T target = createTarget();
        Assert.assertNull(serverOrig.addTarget(target));

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            final C client1 = createClient();
            connectClient(client1);
            final C client2 = createClient();
            connectClient(client2);

            disconnectClient(client1);
            disconnectClient(client2);

            Assert.assertSame(target, serverOrig.removeTarget(target.getTargetName()));
            Assert.assertNull(serverOrig.removeTarget(target.getTargetName()));
        }
        finally {
            server.stop();
        }
    }

    /**
     * Test the attributes of the targets.
     * 
     * @throws Exception
     */
    @Test
    public void testTargetAttributes() throws Exception {
        Assert.assertFalse(server.isStarted());
        Assert.assertEquals(0, getServerTargetAttributes().length);

        // Add two target devices
        final T target1 = createTarget();
        Assert.assertNull(serverOrig.addTarget(target1));

        checkTargetAttributes(1, target1, 0);

        final T target2 = createSecondTarget();
        Assert.assertNull(serverOrig.addTarget(target2));

        checkTargetAttributes(2, target1, 0);
        checkTargetAttributes(2, target2, 0);

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            // Check targets after the start of the server
            checkTargetAttributes(2, target1, 0);
            checkTargetAttributes(2, target2, 0);

            final C client = createClient();
            connectClient(client);

            // Check targets after the creation of the session
            checkTargetAttributes(2, target1, 1);
            checkTargetAttributes(2, target2, 0);

            disconnectClient(client);

            // Give some time to the server to update its configuration
            // Note: the initiator does not close the connection (see usage
            // of SimpleTaskBalancer/Session in the initiator)
            Thread.sleep(200);

            // Check targets after the close of the session
            checkTargetAttributes(2, target1, 0);
            checkTargetAttributes(2, target2, 0);

            Assert.assertSame(target1, serverOrig.removeTarget(target1.getTargetName()));
            checkTargetAttributes(1, target2, 0);
            Assert.assertSame(target2, serverOrig.removeTarget(target2.getTargetName()));
        }
        finally {
            server.stop();
        }

        Assert.assertEquals(0, getServerTargetAttributes().length);
    }

    /**
     * Create a new target instance.
     * 
     * @return the target
     */
    protected abstract T createTarget();

    /**
     * Create a second target instance with a different name.
     * 
     * @return the target
     */
    protected abstract T createSecondTarget();

    /**
     * Create a target with a name in uppercase.
     * 
     * @return the target
     */
    protected abstract T createTargetUppercase();

    /**
     * Create a client on the target.
     * 
     * @return the new client
     */
    protected abstract C createClient();

    /**
     * Connect a client.
     * 
     * @param client
     *            the client to connect
     */
    protected abstract void connectClient(C client);

    /**
     * Disconnect a client.
     * 
     * @param client
     *            the client to disconnect
     */
    protected abstract void disconnectClient(C client);

    /**
     * Remove a target in uppercase. First remove with its name in lower case, then check that the target has been
     * removed by removing it with its name in uppercase.
     * 
     * @param target
     *            the target to remove
     */
    protected abstract void removeTargetUppercaseAndCheck(T target);

    /**
     * Connect a target with a bad name
     * 
     * @param client
     *            the client to connect
     * @throws IllegalArgumentException
     */
    protected abstract void connectClientBadTarget(C client) throws IllegalArgumentException;

    /**
     * disconnect a target with a bad name
     * 
     * @param client
     *            the client to disconnect
     * 
     * @throws IllegalArgumentException
     */
    protected abstract void disconnectClientBadTarget(C client) throws IllegalArgumentException;

    /**
     * Get the target attributes.
     * 
     * @return an array of targets attributes
     */
    protected abstract A[] getServerTargetAttributes();

    /**
     * Check the target attributes.
     * 
     * @param targetCount
     *            the target number
     * @param target
     *            the target
     * @param connectionCount
     *            the connection number
     */
    protected abstract void checkTargetAttributes(int targetCount, T target, int connectionCount);

}
