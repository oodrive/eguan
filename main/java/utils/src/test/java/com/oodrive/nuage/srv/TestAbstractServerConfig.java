package com.oodrive.nuage.srv;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.configuration.AbstractConfigurationContext;
import com.oodrive.nuage.configuration.ConfigValidationException;
import com.oodrive.nuage.configuration.MetaConfiguration;

public abstract class TestAbstractServerConfig<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig, C extends AbstractConfigurationContext> {

    protected abstract AbstractServer<S, T, K> newServer(final InetAddress address);

    protected abstract AbstractServer<S, T, K> newServer(final InetAddress address, final int port);

    protected abstract AbstractServer<S, T, K> newServer(final MetaConfiguration configuration);

    protected abstract C getConfigurationContext();

    protected abstract int getDefaultPort();

    protected abstract String getPropertiesAsString();

    @Test(expected = NullPointerException.class)
    public void testNullAddress1() {
        newServer((InetAddress) null);
    }

    @Test(expected = NullPointerException.class)
    public void testNullAddress2() {
        newServer(null, 123);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidAddress1() throws UnknownHostException {
        newServer(InetAddress.getByName("8.8.8.8"));
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidAddress2() throws UnknownHostException {
        newServer(InetAddress.getByName("8.8.8.8"), 123);
    }

    @Test(expected = NullPointerException.class)
    public void testSetNullAddressString() throws UnknownHostException {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetNullAddressAddr() {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress((InetAddress) null);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testSetInvalidAddressString() throws UnknownHostException {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress("8.8.8.8");
    }

    @Test(expected = ServerConfigurationException.class)
    public void testSetInvalidAddressAddr() throws ServerConfigurationException, UnknownHostException {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress(InetAddress.getByName("8.8.8.8"));
    }

    @Test
    public void testSetValidAddressString() throws UnknownHostException {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress("localhost");
        Assert.assertEquals(InetAddress.getLoopbackAddress(), server.getInetAddress());
    }

    @Test
    public void testSetValidAddressAddr() throws ServerConfigurationException, UnknownHostException {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 123);
        server.setAddress(InetAddress.getByName("localhost"));
        Assert.assertEquals(InetAddress.getLoopbackAddress(), server.getInetAddress());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidPort1() {
        newServer(InetAddress.getLoopbackAddress(), -1);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testInvalidPort2() {
        newServer(InetAddress.getLoopbackAddress(), 65600);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testSetInvalidPort1() {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress());
        server.setPort(-1);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testSetInvalidPort2() {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress());
        server.setPort(65600);
    }

    @Test
    public void testCreateOk1() {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress());
        Assert.assertEquals(getDefaultPort(), server.getPort());
    }

    @Test
    public void testCreateOk2() {
        final AbstractServer<S, T, K> server = newServer(InetAddress.getLoopbackAddress(), 13510);
        Assert.assertEquals(13510, server.getPort());
    }

    @Test
    public void testEqualsHashcodeToString() throws UnknownHostException {
        final AbstractServer<S, T, K> server1 = newServer(InetAddress.getLoopbackAddress());
        final AbstractServer<S, T, K> server2 = newServer(InetAddress.getLoopbackAddress(), getDefaultPort());
        final AbstractServer<S, T, K> server3 = newServer(InetAddress.getLoopbackAddress(), 13510);
        Assert.assertEquals(server1.hashCode(), server2.hashCode());
        Assert.assertTrue(server1.equals(server1));
        Assert.assertTrue(server1.equals(server2));
        Assert.assertFalse(server1.hashCode() == server3.hashCode());
        Assert.assertFalse(server1.equals(server3));
        Assert.assertFalse(server1.equals("server1"));

        // Check address
        Assert.assertTrue(server1.getAddress().equals(server2.getAddress()));

        // Update server2 and server3 port
        server2.setPort(13510);
        Assert.assertTrue(server2.equals(server3));
        server3.setPort(getDefaultPort());
        Assert.assertTrue(server1.equals(server3));

        // Update server3 address
        // IPv6 localhost/loopback: should be defined locally and not be equals to InetAddress.getLoopbackAddress()
        server3.setAddress(InetAddress.getByName("::1"));
        Assert.assertFalse(server1.equals(server3));
        Assert.assertFalse(server1.getAddress().equals(server3.getAddress()));

        // Check toString
        Assert.assertTrue(server1.toString().startsWith(server1.getClass().getSimpleName() + "["));
        Assert.assertTrue(server1.toString().endsWith(":" + Integer.toString(getDefaultPort()) + "]"));
    }

    @Test
    public void testMetaConfigurationEmpty() throws IOException, ConfigValidationException {
        final ByteArrayInputStream is = new ByteArrayInputStream(new byte[0]);
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(is, getConfigurationContext());
        final AbstractServer<S, T, K> server = newServer(configuration);
        Assert.assertEquals(getDefaultPort(), server.getPort());
        Assert.assertTrue(server.getInetAddress().isAnyLocalAddress());
    }

    @Test
    public void testMetaConfigurationValues() throws IOException, ConfigValidationException {
        final ByteArrayInputStream is = new ByteArrayInputStream(getPropertiesAsString().getBytes());
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(is, getConfigurationContext());
        final AbstractServer<S, T, K> server = newServer(configuration);
        Assert.assertEquals(3333, server.getPort());
        Assert.assertTrue(server.getInetAddress().isLoopbackAddress());
    }

    @Test
    public void testConfigEquals() throws UnknownHostException {
        final AbstractServerConfig config = new AbstractServerConfig(InetAddress.getLoopbackAddress(), 9999) {
        };
        final AbstractServerConfig configSame = new AbstractServerConfig(InetAddress.getLoopbackAddress(), 9999) {
        };

        Assert.assertEquals(config, configSame);

        config.setAddress(InetAddress.getByName("0.0.0.0"));
        Assert.assertFalse(config.equals(configSame));

        config.setAddress(InetAddress.getLoopbackAddress());
        Assert.assertEquals(config, configSame);

        config.setPort(1234);
        Assert.assertFalse(config.equals(configSame));
    }

    @Test
    public void testConfigClone() throws UnknownHostException {
        final AbstractServerConfig config = new AbstractServerConfig(InetAddress.getLoopbackAddress(), 9999) {
        };
        final AbstractServerConfig clone = config.clone();
        Assert.assertEquals(config.getPort(), clone.getPort());
        Assert.assertEquals(config.getAddress(), clone.getAddress());

        config.setAddress(InetAddress.getByName("0.0.0.0"));
        Assert.assertFalse(config.getAddress().equals(clone.getAddress()));

        config.setPort(1234);
        Assert.assertFalse(config.getPort() == clone.getPort());
    }
}
