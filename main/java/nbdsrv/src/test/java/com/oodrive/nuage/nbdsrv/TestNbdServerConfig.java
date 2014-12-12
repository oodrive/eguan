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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.srv.AbstractServer;
import com.oodrive.nuage.srv.TestAbstractServerConfig;

public class TestNbdServerConfig extends
        TestAbstractServerConfig<ExportServer, NbdExport, NbdServerConfig, NbdServerConfigurationContext> {

    @Override
    protected AbstractServer<ExportServer, NbdExport, NbdServerConfig> newServer(final InetAddress address) {
        return new NbdServer(address);
    }

    @Override
    protected AbstractServer<ExportServer, NbdExport, NbdServerConfig> newServer(final InetAddress address,
            final int port) {
        return new NbdServer(address, port);
    }

    @Override
    protected AbstractServer<ExportServer, NbdExport, NbdServerConfig> newServer(final MetaConfiguration configuration) {
        return new NbdServer(configuration);
    }

    @Override
    protected NbdServerConfigurationContext getConfigurationContext() {
        return NbdServerConfigurationContext.getInstance();
    }

    @Override
    protected int getDefaultPort() {
        return NbdServer.DEFAULT_NBD_PORT;
    }

    @Override
    protected String getPropertiesAsString() {
        return "com.oodrive.nuage.nbdsrv.port=3333\ncom.oodrive.nuage.nbdsrv.address=127.0.0.1";
    }

    @Test
    public void testNbdConfigEquals() throws UnknownHostException {
        final NbdServerConfig config = new NbdServerConfig(InetAddress.getLoopbackAddress(), 9999, true);
        final NbdServerConfig configSame = new NbdServerConfig(InetAddress.getLoopbackAddress(), 9999, true);

        Assert.assertEquals(config, configSame);

        config.setAddress(InetAddress.getByName("0.0.0.0"));
        Assert.assertFalse(config.equals(configSame));

        config.setAddress(InetAddress.getLoopbackAddress());
        Assert.assertEquals(config, configSame);

        config.setPort(1234);
        Assert.assertFalse(config.equals(configSame));

        config.setPort(9999);
        Assert.assertEquals(config, configSame);

        config.setTrimEnabled(false);
        Assert.assertFalse(config.equals(configSame));
    }

    @Test
    public void testNbdConfigClone() throws UnknownHostException {
        final NbdServerConfig config = new NbdServerConfig(InetAddress.getLoopbackAddress(), 9999, true);
        final NbdServerConfig clone = config.clone();

        Assert.assertEquals(config.getPort(), clone.getPort());
        Assert.assertEquals(config.getAddress(), clone.getAddress());
        Assert.assertEquals(config.isTrimEnabled(), clone.isTrimEnabled());

        config.setAddress(InetAddress.getByName("0.0.0.0"));
        Assert.assertFalse(config.getAddress().equals(clone.getAddress()));

        config.setPort(1234);
        Assert.assertFalse(config.getPort() == clone.getPort());

        config.setTrimEnabled(false);
        Assert.assertFalse(config.isTrimEnabled() == clone.isTrimEnabled());
    }

}
