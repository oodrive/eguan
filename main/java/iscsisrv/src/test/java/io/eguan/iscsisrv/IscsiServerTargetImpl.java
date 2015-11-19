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

import static io.eguan.iscsisrv.IscsiInitiatorConfigDefinition.INITIATOR_CONFIG_FILE_PREFIX;
import static io.eguan.iscsisrv.IscsiInitiatorConfigDefinition.INITIATOR_CONFIG_FILE_SUFFIX;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiServerConfig;
import io.eguan.iscsisrv.IscsiTarget;
import io.eguan.srv.AbstractServer;
import io.eguan.srv.ClientBasicIops;
import io.eguan.srv.TargetMgr;
import io.eguan.utils.unix.UnixIScsiTarget;
import io.eguan.utils.unix.UnixTarget;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.jscsi.target.TargetServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IscsiServerTargetImpl extends TargetMgr<TargetServer, IscsiTarget, IscsiServerConfig> {

    static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerIOTest.class);

    private final static String TARGET_DEVICE_HEADER = "iqn.2000-06.com.oodrive:";

    private File initiatorConfigFile;

    @Override
    protected IscsiServer createServer() {
        return new IscsiServer(InetAddress.getLoopbackAddress());
    }

    @Override
    protected void addTarget(final AbstractServer<TargetServer, IscsiTarget, IscsiServerConfig> server,
            final Map<File, Long> targets) throws IOException {

        // Iscsi target controller
        final ScsiTargetController controller = new ScsiTargetController((IscsiServer) server);
        final ArrayList<File> targetFiles = new ArrayList<>();

        // Add target
        for (final Entry<File, Long> target : targets.entrySet()) {
            final String deviceFileName = target.getKey().getAbsolutePath();
            controller.addTarget(deviceFileName, target.getValue().longValue());
            targetFiles.add(target.getKey());
        }
        // Create config file for initiator, depends on target name
        initInitiatorConfig(targetFiles);
    }

    @Override
    protected ClientBasicIops initClient() throws IOException {
        return new InitiatorClientBasicIops("/jscsi.xsd", initiatorConfigFile.toURI().toURL());
    }

    @Override
    protected UnixTarget createTarget(final File target, final int count) throws IOException {
        UnixIScsiTarget.sendTarget("127.0.0.1");
        final String targetIqn = TARGET_DEVICE_HEADER + target.getAbsolutePath().replace('/', '-');
        return new UnixIScsiTarget("127.0.0.1", targetIqn);
    }

    @Override
    protected String getTargetName(final File deviceFile) {
        return deviceFile.getName();
    }

    @Override
    protected void removeFiles() {
        try {
            Files.delete(initiatorConfigFile.toPath());
        }
        catch (final Throwable t) {
            LOGGER.warn("Failed to delete file: " + initiatorConfigFile.getAbsolutePath(), t);
        }
    }

    private void initInitiatorConfig(final ArrayList<File> targets) throws IOException {
        initiatorConfigFile = File.createTempFile(INITIATOR_CONFIG_FILE_PREFIX, INITIATOR_CONFIG_FILE_SUFFIX);

        // Write config
        try (PrintStream config = new PrintStream(initiatorConfigFile)) {
            IscsiInitiatorConfigDefinition.configWriteBegin(config);
            for (final File target : targets) {
                IscsiInitiatorConfigDefinition.configWriteUpToTargetId(config);

                final String deviceName = target.getName();
                config.print(deviceName);
                IscsiInitiatorConfigDefinition.configWriteUpToTargetName(config);

                final String iqn = TARGET_DEVICE_HEADER + target.getAbsolutePath().replace('/', '-');
                config.print(iqn);
                IscsiInitiatorConfigDefinition.configWriteAfterTargetName(config);
            }
            IscsiInitiatorConfigDefinition.configWriteEnd(config);
        }
    }
}
