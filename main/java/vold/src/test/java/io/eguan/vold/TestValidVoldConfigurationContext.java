package io.eguan.vold;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.vold.EnableIscsiConfigKey;
import io.eguan.vold.EnableNbdConfigKey;
import io.eguan.vold.NodeConfigKey;
import io.eguan.vold.OwnerConfigKey;
import io.eguan.vold.PeersConfigKey;
import io.eguan.vold.ServerEndpointInetAddressConfigKey;
import io.eguan.vold.ServerEndpointPortConfigKey;
import io.eguan.vold.VoldConfigurationContext;
import io.eguan.vold.VoldLocation;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;

public final class TestValidVoldConfigurationContext extends ValidConfigurationContext {

    private static final ContextTestHelper<VoldConfigurationContext> TEST_HELPER = new ContextTestHelper<VoldConfigurationContext>(
            VoldConfigurationContext.getInstance()) {

        @Override
        public final void tearDown() throws InitializationError {
            // nothing
        }

        @Override
        public final void setUp() throws InitializationError {
            // nothing
        }

        @Override
        public final Properties getConfig() {
            final Properties result = new Properties();
            result.setProperty(TEST_HELPER.getPropertyKey(EnableIscsiConfigKey.getInstance()), Boolean.TRUE.toString());
            result.setProperty(TEST_HELPER.getPropertyKey(EnableNbdConfigKey.getInstance()), Boolean.TRUE.toString());
            result.setProperty(TEST_HELPER.getPropertyKey(NodeConfigKey.getInstance()), UUID.randomUUID().toString());
            result.setProperty(TEST_HELPER.getPropertyKey(OwnerConfigKey.getInstance()), UUID.randomUUID().toString());

            final InetAddress localhost = InetAddress.getLoopbackAddress();
            final ServerEndpointPortConfigKey serverPortKey = ServerEndpointPortConfigKey.getInstance();
            final int serverPort = serverPortKey.getDefaultValue().intValue();

            String peerString = Arrays.asList(
                    new VoldLocation[] {
                            new VoldLocation(UUID.randomUUID(), new InetSocketAddress(localhost, serverPort + 20)),
                            new VoldLocation(UUID.randomUUID(), new InetSocketAddress(localhost, serverPort + 30)) })
                    .toString();
            peerString = peerString.replace('[', ' ').replace(']', ' ');
            result.setProperty(TEST_HELPER.getPropertyKey(PeersConfigKey.getInstance()), peerString);

            result.setProperty(TEST_HELPER.getPropertyKey(ServerEndpointInetAddressConfigKey.getInstance()),
                    InetAddress.getLoopbackAddress().getHostAddress());

            result.setProperty(TEST_HELPER.getPropertyKey(serverPortKey),
                    Integer.toString(serverPortKey.getDefaultValue().intValue() + 10));
            return result;
        }
    };

    /**
     * Sets up class fixture.
     * 
     * @throws InitializationError
     *             if initialization fails
     */
    @BeforeClass
    public static final void setUpClass() throws InitializationError {
        TEST_HELPER.setUp();
    }

    /**
     * Tears down class fixture.
     * 
     * @throws InitializationError
     *             if shutdown fails even partially
     */
    @AfterClass
    public static final void tearDownClass() throws InitializationError {
        TEST_HELPER.tearDown();
    }

    @Override
    public final ContextTestHelper<?> getTestHelper() {
        return TEST_HELPER;
    }

}
