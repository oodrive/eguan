package io.eguan.nbdsrv;

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

import io.eguan.nbdsrv.NbdExportAttributes;
import io.eguan.nbdsrv.NbdServer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNbdServer extends TestNbdServerAbstract {
    /**
     * Initialize NBD server MBean.
     * 
     * @throws Exception
     */
    @Before
    public void initServerMBean() throws Exception {
        server = serverOrig = new NbdServer(InetAddress.getLoopbackAddress());
    }

    @Override
    protected final NbdExportAttributes[] getServerTargetAttributes() {
        return ((NbdServer) server).getExports();
    }

    @Test
    public void testChangeTrimServerStarted() throws UnknownHostException {

        // Server Started
        Assert.assertFalse(server.isStarted());
        server.start();
        try {
            final boolean oldvalue = ((NbdServer) server).isTrimEnabled();
            ((NbdServer) server).setTrimEnabled(!oldvalue);
            Assert.assertTrue(server.isRestartRequired());
        }
        finally {
            server.stop();
        }
    }

    @Test
    public void testChangeTrimServerNotStarted() throws UnknownHostException {
        // Server not started
        Assert.assertFalse(server.isStarted());
        final boolean oldvalue = ((NbdServer) server).isTrimEnabled();
        ((NbdServer) server).setTrimEnabled(!oldvalue);
        Assert.assertFalse(server.isRestartRequired());
    }

    @Test
    public void testSameTrimServerStarted() throws UnknownHostException {
        server.start();
        try {
            final boolean oldvalue = ((NbdServer) server).isTrimEnabled();
            ((NbdServer) server).setTrimEnabled(oldvalue);
            Assert.assertFalse(server.isRestartRequired());

            ((NbdServer) server).setTrimEnabled(!oldvalue);
            Assert.assertTrue(server.isRestartRequired());

            ((NbdServer) server).setTrimEnabled(oldvalue);
            Assert.assertFalse(server.isRestartRequired());

        }
        finally {
            server.stop();
        }
    }

}
