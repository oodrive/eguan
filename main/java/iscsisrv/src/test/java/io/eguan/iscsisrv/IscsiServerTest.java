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
import io.eguan.iscsisrv.IscsiTargetAttributes;

import java.net.InetAddress;

import org.junit.Before;

/**
 * Test management of the server.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class IscsiServerTest extends IscsiServerAbstractTest {

    /**
     * Initialize iSCSI server MBean.
     * 
     * @throws Exception
     */
    @Before
    public void initServerMBean() throws Exception {
        server = serverOrig = new IscsiServer(InetAddress.getLoopbackAddress());
    }

    @Override
    protected final IscsiTargetAttributes[] getServerTargetAttributes() {
        return ((IscsiServer) server).getTargets();
    }

}
