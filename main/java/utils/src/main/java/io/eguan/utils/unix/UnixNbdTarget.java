package io.eguan.utils.unix;

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

import io.eguan.utils.RunCmdErrorException;
import io.eguan.utils.RunCmdUtils;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public final class UnixNbdTarget implements UnixTarget {
    private final String host;
    private final String port;
    private final String exportName;
    private final String device;

    static final Logger LOGGER = LoggerFactory.getLogger(UnixNbdTarget.class);

    public UnixNbdTarget(@Nonnull final String address, @Nonnull final String exportName, final int deviceNumber) {
        this(address, 10809, exportName, deviceNumber);
    }

    public UnixNbdTarget(@Nonnull final String address, @Nonnegative final int port, @Nonnull final String exportName,
            final int deviceNumber) {
        this.host = Objects.requireNonNull(address);
        this.port = Integer.toString(port);
        this.device = "/dev/nbd" + deviceNumber;
        this.exportName = exportName;
    }

    /**
     * TODO linux specific
     * 
     * @return The device path after successful login
     */
    @Override
    public final String getDeviceFilePath() {
        return this.device;
    }

    /**
     * Perform NBD handshake.
     * 
     * @throws IOException
     */
    @Override
    public final void login() throws IOException {
        // TODO clean sudo
        final String[] nbdClient = new String[] { "sudo", "nbd-client", this.host, this.port, this.device, "-N",
                exportName };
        RunCmdUtils.runCmd(nbdClient, this);
    }

    /**
     * Perform NBD disconnect.
     * 
     * @throws IOException
     */
    @Override
    public final void logout() throws IOException {
        // TODO clean sudo
        final String[] nbdClient = new String[] { "sudo", "nbd-client", "-d", this.device };
        RunCmdUtils.runCmd(nbdClient, this);
    }

    @Override
    public final String toString() {
        return "UnixNbdTarget[host=" + this.host + ", export=" + this.exportName + "]";
    }

    @Override
    public String getDevicePart1Suffix() {
        return "p1";
    }

    public static final void killAll() throws IOException {
        try {
            final String[] nbdClient = new String[] { "sudo", "killall", "nbd-client" };
            RunCmdUtils.runCmd(nbdClient, UnixTarget.class);
        }
        catch (final RunCmdErrorException e) {
            // Could be a normal error (no nbd-client)
            LOGGER.warn("Failed to kill nbd-client, exitValue=" + e.getExitValue());
        }
    }
}
