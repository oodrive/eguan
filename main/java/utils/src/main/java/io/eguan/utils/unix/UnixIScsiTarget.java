package io.eguan.utils.unix;

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

import io.eguan.utils.RunCmdUtils;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Wrap access to an iSCSI target using open-iscsi.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 */
@Immutable
public final class UnixIScsiTarget implements UnixTarget {

    private final String portal;
    private final String iqn;
    private final String device;

    /**
     * Discover the target for the given portal TODO parse and return discovered target(s)
     * 
     * @param portal
     *            the portal that will be use
     * @throws IOException
     */
    public final static void sendTarget(final String portal) throws IOException {
        // TODO clean sudo
        final String[] iscsiadm = new String[] { "sudo", "iscsiadm", "--mode", "discovery", "--type",
                "sendtargets", "--portal", portal };
        RunCmdUtils.runCmd(iscsiadm, portal);
    }

    public UnixIScsiTarget(@Nonnull final String address, @Nonnull final String iqn) {
        this(address, 3260, iqn);
    }

    public UnixIScsiTarget(@Nonnull final String address, @Nonnegative final int port, @Nonnull final String iqn) {
        this.iqn = iqn;
        this.portal = Objects.requireNonNull(address) + ":" + port;
        this.device = "/dev/disk/by-path/ip-" + portal + "-iscsi-" + Objects.requireNonNull(iqn) + "-lun-0";
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
     * Perform iScsi login.
     * 
     * @throws IOException
     */
    @Override
    public final void login() throws IOException {
        // TODO clean sudo
        final String[] iscsiadm = new String[] { "sudo", "iscsiadm", "--mode", "node", "--portal", this.portal,
                "--targetname", this.iqn, "--login" };
        RunCmdUtils.runCmd(iscsiadm, this);
    }

    /**
     * Perform iScsi logout.
     * 
     * @throws IOException
     */
    @Override
    public final void logout() throws IOException {
        // TODO clean sudo
        final String[] iscsiadm = new String[] { "sudo", "iscsiadm", "--mode", "node", "--portal", this.portal,
                "--targetname", this.iqn, "--logout" };
        RunCmdUtils.runCmd(iscsiadm, this);
    }

    /**
     * Rescan the connection.
     * 
     * @throws IOException
     */
    public final void rescan() throws IOException {
        final String[] iscsiadm = new String[] { "sudo", "iscsiadm", "--mode", "node", "--rescan" };
        RunCmdUtils.runCmd(iscsiadm, this);
    }

    @Override
    public final String toString() {
        return "UnixIScsiTarget[portal=" + this.portal + ", iqn=" + this.iqn + "]";
    }

    @Override
    public String getDevicePart1Suffix() {
        return "-part1";
    }

}
