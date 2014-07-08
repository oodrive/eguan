package com.oodrive.nuage.iscsisrv;

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

import com.oodrive.nuage.srv.AbstractServerConfig;

/**
 * Configuration of the iSCSI server.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class IscsiServerConfig extends AbstractServerConfig {

    IscsiServerConfig(final InetAddress address, final int port) {
        super(address, port);
    }

}
