package com.oodrive.nuage.utils.unix;

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

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

import com.oodrive.nuage.utils.RunCmdUtils;

/**
 * Unix mount information. Must be root to mount or umount the file system.
 * 
 * @author oodrive
 */
@Immutable
public final class UnixMount {

    /** Mount point */
    private final File mountPoint;
    /** Mount device */
    private final String device;
    /** Mount options */
    private final String opts;
    /** fs type */
    private final String type;

    /**
     * Create a new Unix mount.
     * 
     * @param mountPoint
     *            mount point. The directory must exist and should be empty.
     * @param device
     *            a device, an exported file system or a file containing a file system.
     * @param opts
     *            mount options string. The options are separated by a comma. May be <code>null</code>
     * @param type
     *            fs type: ext3, nfs, cifs, ...
     * @throws IllegalArgumentException
     *             if mountPoint does not exist or is not a directory.
     */
    public UnixMount(final File mountPoint, final String device, final String opts, final String type) {
        super();
        if (!mountPoint.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + mountPoint);
        }
        this.mountPoint = mountPoint;
        if ((device == null) || (type == null)) {
            throw new NullPointerException();
        }
        this.device = device;
        this.opts = opts;
        this.type = type;
    }

    /**
     * Mounts the device on the mount point. Must be root, except for some fstab entries.
     * 
     * @throws IOException
     *             if the mount fails: device invalid, permission denied, invalid options, ...
     */
    public final void mount() throws IOException {
        // TODO clean sudo
        final String[] cmdArray;
        if (this.opts == null) {
            // No options
            cmdArray = new String[] { "sudo", "mount", "-t", this.type, this.device, this.mountPoint.getAbsolutePath() };
        }
        else {
            cmdArray = new String[] { "sudo", "mount", "-t", this.type, "-o", this.opts, this.device,
                    this.mountPoint.getAbsolutePath() };
        }
        RunCmdUtils.runCmd(cmdArray, this);
    }

    /**
     * Unmount the file system. Syncing before umounting seems to prevent from losing loop device.
     * 
     * @throws IOException
     *             if the umount fails: file system busy, ...
     */
    public final void umount() throws IOException {
        String[] cmdArray = new String[] { "sync" };
        RunCmdUtils.runCmd(cmdArray, this);
        // TODO clean sudo
        cmdArray = new String[] { "sudo", "umount", "-d", this.mountPoint.getAbsolutePath() };
        RunCmdUtils.runCmd(cmdArray, this);
    }

}
