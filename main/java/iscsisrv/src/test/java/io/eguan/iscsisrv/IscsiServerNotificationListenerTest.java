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

import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiServerConfig;
import io.eguan.iscsisrv.IscsiTarget;
import io.eguan.srv.TestAbstractServerNotificationListener;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jscsi.target.TargetServer;
import org.junit.Before;

/**
 * Test {@link IscsiServer} notifications handling.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 */
public class IscsiServerNotificationListenerTest extends
        TestAbstractServerNotificationListener<TargetServer, IscsiTarget, IscsiServerConfig> {
    @Before
    public void initServer() throws UnknownHostException {
        server = new IscsiServer(InetAddress.getLoopbackAddress());
    }

}
