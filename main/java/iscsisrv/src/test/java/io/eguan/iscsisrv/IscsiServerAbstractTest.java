package io.eguan.iscsisrv;

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

import io.eguan.iscsisrv.IscsiServerConfig;
import io.eguan.iscsisrv.IscsiTarget;
import io.eguan.iscsisrv.IscsiTargetAttributes;
import io.eguan.srv.TestAbstractServer;

import java.nio.channels.SocketChannel;

import org.jscsi.exception.ConfigurationException;
import org.jscsi.exception.NoSuchSessionException;
import org.jscsi.exception.TaskExecutionException;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;
import org.jscsi.target.TargetServer;
import org.junit.Assert;
import org.junit.Test;

public abstract class IscsiServerAbstractTest extends
        TestAbstractServer<TargetServer, IscsiTarget, IscsiServerConfig, Initiator, IscsiTargetAttributes> {

    /** TargetName (server) for the initiator (see unit test initiator config file) */
    private static String TARGET_NAME = "test-eguan";
    /** TargetName (device) for the initiator (see unit test initiator config file) */
    private static String TARGET_DEVICE_IQN = "iqn.2000-06.com.oodrive:test-target";
    /** TargetName (device) for the initiator (see unit test initiator config file) */
    private static String TARGET_DEVICE_IQN_UPPERCASE = "iqn.2000-06.com.oodrive:TEST-TARGET";

    /** TargetName (device) for the initiator which does not exist */
    private static String BAD_TARGET_NAME = "bad-test-eguan";

    @Override
    protected IscsiTarget createTarget() {
        return IscsiTarget.newIscsiTarget(TARGET_DEVICE_IQN, "target alias1", IscsiTargetCreateTest.DUMMY_DEVICE);
    }

    @Override
    protected IscsiTarget createSecondTarget() {
        final String name2 = TARGET_DEVICE_IQN + "bis";
        final String alias2 = "my second target";
        return IscsiTarget.newIscsiTarget(name2, alias2, IscsiTargetCreateTest.DUMMY_DEVICE);
    }

    @Override
    protected IscsiTarget createTargetUppercase() {
        return IscsiTarget.newIscsiTarget(TARGET_DEVICE_IQN_UPPERCASE, null, IscsiTargetCreateTest.DUMMY_DEVICE);
    }

    @Override
    protected Initiator createClient() {
        try {
            final Configuration configuration = Configuration.create(Initiator.class.getResource("/jscsi.xsd"),
                    getClass().getResource("/iscsisrv-tst-config.xml"));
            final Initiator initiator = new Initiator(configuration);
            return initiator;
        }
        catch (final ConfigurationException e) {
            throw new IllegalArgumentException("Bad configuration");
        }

    }

    @Override
    protected void connectClient(final Initiator initiator) {
        try {
            initiator.createSession(TARGET_NAME);
        }
        catch (final NoSuchSessionException e) {
            throw new IllegalArgumentException("Connect client with this target failed");
        } // if throw null pointer exception, may be because open socket failed
    }

    @Override
    protected void disconnectClient(final Initiator initiator) {
        try {
            initiator.closeSession(TARGET_NAME);
        }
        catch (final NoSuchSessionException e) {
            throw new IllegalArgumentException("Can not disconnect client from this target");
        }
        catch (final TaskExecutionException e) {
            // ignored, do not test the initiator here
        }

    }

    @Override
    protected void connectClientBadTarget(final Initiator initiator) throws IllegalArgumentException {
        try {
            initiator.createSession(BAD_TARGET_NAME);
        }
        catch (final NoSuchSessionException e) {
            // Expected exception
            throw new IllegalArgumentException("Can connect client to this target");
        }
    }

    @Override
    protected void disconnectClientBadTarget(final Initiator initiator) throws IllegalArgumentException {
        try {
            initiator.closeSession(BAD_TARGET_NAME);
        }
        catch (final NoSuchSessionException e) {
            throw new IllegalArgumentException("Can not disconnect client from this target");
        }
        catch (final TaskExecutionException e) {
            // ignored, do not test the initiator here
        }
    }

    @Override
    protected void removeTargetUppercaseAndCheck(final IscsiTarget target) {
        // try to remove the target with a lower case name
        Assert.assertSame(target, serverOrig.removeTarget(TARGET_DEVICE_IQN));
        Assert.assertNull(serverOrig.removeTarget(TARGET_DEVICE_IQN_UPPERCASE));
    }

    @Override
    protected final void checkTargetAttributes(final int targetCount, final IscsiTarget target,
            final int connectionCount) {
        final IscsiTargetAttributes[] attributesList = getServerTargetAttributes();
        Assert.assertEquals(targetCount, attributesList.length);
        boolean found = false;
        for (int i = 0; i < targetCount; i++) {
            final IscsiTargetAttributes attributes = attributesList[i];
            if (attributes.getName().equalsIgnoreCase(target.getTargetName())) {
                // Target must be found once
                Assert.assertFalse(found);
                found = true;
                if (target.getTargetAlias() == null) {
                    Assert.assertEquals(attributes.getAlias(), "");
                }
                else {
                    Assert.assertEquals(attributes.getAlias(), target.getTargetAlias());
                }
                Assert.assertEquals(connectionCount, attributes.getConnectionCount());
                Assert.assertEquals(target.getSize(), attributes.getSize());
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testAddRemoveTargetsWithDifferentAlias() {
        Assert.assertFalse(server.isStarted());
        final IscsiTarget target1 = IscsiTarget.newIscsiTarget(TARGET_DEVICE_IQN, "target alias1",
                IscsiTargetCreateTest.DUMMY_DEVICE);
        Assert.assertNull(serverOrig.addTarget(target1));
        final IscsiTarget target2 = IscsiTarget.newIscsiTarget(TARGET_DEVICE_IQN, "target alias2",
                IscsiTargetCreateTest.DUMMY_DEVICE);
        Assert.assertSame(target1, serverOrig.addTarget(target2));

        // null if nothing removed: take alias name
        Assert.assertNull(serverOrig.removeTarget(target1.getTargetAlias()));
        Assert.assertNull(serverOrig.removeTarget(target2.getTargetAlias()));

        Assert.assertSame(target2, serverOrig.removeTarget(target1.getTargetName()));
        Assert.assertNull(serverOrig.removeTarget(target1.getTargetName()));

    }

    @Test
    public void testConnectDisconnect() throws Exception {
        Assert.assertFalse(server.isStarted());

        server.start();
        try {
            Assert.assertTrue(server.isStarted());
            final Configuration configuration = Configuration.create(Initiator.class.getResource("/jscsi.xsd"),
                    getClass().getResource("/iscsisrv-tst-config.xml"));

            final SocketChannel socketChannel = SocketChannel.open();
            try {
                socketChannel.connect(configuration.getTargetAddress(TARGET_NAME));
                // close before sending/reading anything on the socket
            }
            finally {
                socketChannel.close();
                // Wait client socket really closed
                Thread.sleep(500);
            }
        }
        finally {
            server.stop();
        }
    }

}
