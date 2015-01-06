package com.oodrive.nuage.iscsisrv;

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

import java.net.InetAddress;

import org.jscsi.target.TargetServer;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.srv.AbstractServer;
import com.oodrive.nuage.srv.TestAbstractServerConfig;

/**
 * Unit tests for the configuration of the iSCSI server.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class IscsiServerConfigTest extends
        TestAbstractServerConfig<TargetServer, IscsiTarget, IscsiServerConfig, IscsiServerConfigurationContext> {

    @Override
    protected AbstractServer<TargetServer, IscsiTarget, IscsiServerConfig> newServer(final InetAddress address) {
        return new IscsiServer(address);
    }

    @Override
    protected AbstractServer<TargetServer, IscsiTarget, IscsiServerConfig> newServer(final InetAddress address,
            final int port) {
        return new IscsiServer(address, port);
    }

    @Override
    protected AbstractServer<TargetServer, IscsiTarget, IscsiServerConfig> newServer(
            final MetaConfiguration configuration) {
        return new IscsiServer(configuration);
    }

    @Override
    protected IscsiServerConfigurationContext getConfigurationContext() {
        return IscsiServerConfigurationContext.getInstance();
    }

    @Override
    protected int getDefaultPort() {
        return IscsiServer.DEFAULT_ISCSI_PORT;
    }

    @Override
    protected String getPropertiesAsString() {
        return "com.oodrive.nuage.iscsisrv.port=3333\ncom.oodrive.nuage.iscsisrv.address=127.0.0.1";
    }
}
