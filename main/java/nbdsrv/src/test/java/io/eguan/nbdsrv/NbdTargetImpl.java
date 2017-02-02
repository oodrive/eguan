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

import io.eguan.nbdsrv.ExportServer;
import io.eguan.nbdsrv.NbdExport;
import io.eguan.nbdsrv.NbdServer;
import io.eguan.nbdsrv.NbdServerConfig;
import io.eguan.nbdsrv.client.NbdClientBasicIops;
import io.eguan.srv.AbstractServer;
import io.eguan.srv.ClientBasicIops;
import io.eguan.srv.TargetMgr;
import io.eguan.utils.unix.UnixNbdTarget;
import io.eguan.utils.unix.UnixTarget;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Map.Entry;

public class NbdTargetImpl extends TargetMgr<ExportServer, NbdExport, NbdServerConfig> {

    @Override
    protected AbstractServer<ExportServer, NbdExport, NbdServerConfig> createServer() {
        return new NbdServer(InetAddress.getLoopbackAddress());
    }

    @Override
    protected void addTarget(final AbstractServer<ExportServer, NbdExport, NbdServerConfig> server,
            final Map<File, Long> targets) throws IOException {

        for (final Entry<File, Long> target : targets.entrySet()) {
            final String deviceFileName = target.getKey().getAbsolutePath();
            final NbdDeviceFile device = Main.createNbdDeviceFile(deviceFileName, target.getValue());
            final NbdExport export = new NbdExport(deviceFileName, device);
            server.addTarget(export);
        }
    }

    @Override
    protected ClientBasicIops initClient() throws IOException {
        return new NbdClientBasicIops(10809);
    }

    @Override
    protected UnixTarget createTarget(final File deviceFile, final int number) throws IOException {
        return new UnixNbdTarget("127.0.0.1", deviceFile.getAbsolutePath(), number);
    }

    @Override
    protected String getTargetName(final File file) {
        return file.getAbsolutePath();
    }

    @Override
    protected void removeFiles() {
        // nothing to do
    }
}
