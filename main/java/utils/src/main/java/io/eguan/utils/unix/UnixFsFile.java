package io.eguan.utils.unix;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.Immutable;

/**
 * File system inside a file.
 * 
 * @author oodrive
 */
@Immutable
public class UnixFsFile {

    /** File containing the file system */
    private final File file;
    /** File / file system size */
    private final long size;
    /** file system type (ext3, bfs, reiserfs, vfat, ...) */
    private final String type;
    /** Mount options */
    private final String mopts;

    /**
     * Define a file containing a file system.
     * 
     * @param file
     *            file containing the file system. The file may not exist: in this case, the method
     *            {@link UnixFsFile#create()} must be call before mounting the file system.
     * @param size
     *            file size. The size must be large enough to contain a file system of the given type.
     * @param type
     *            file system type (ext3, bfs, xfs, ...)
     * @param mopts
     *            mount options string. The options are separated by a comma. May be <code>null</code>
     */
    public UnixFsFile(final File file, final long size, final String type, final String mopts) {
        super();
        if ((file == null) || (type == null)) {
            throw new NullPointerException();
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size=" + size);
        }
        this.file = file;
        this.size = size;
        this.type = type;
        this.mopts = mopts;
    }

    /**
     * Create the file and the file system. If a file already exists, it is deleted.
     * 
     * @throws IOException
     *             on any failure.
     */
    public final void create() throws IOException {
        // Delete any existing element and create a new file
        this.file.delete();
        if (!this.file.createNewFile()) {
            throw new IOException("Failed to create file '" + this.file.getAbsolutePath() + "'");
        }

        // Set the file size to the give FS size
        // try with fallocate. Use standard call on failure
        final String absPath = this.file.getAbsolutePath();
        final String sizeStr = Long.toString(this.size);
        try {
            final String[] fallocate = new String[] { "fallocate", "-l", sizeStr, absPath };
            RunCmdUtils.runCmd(fallocate, this.file);
        }
        catch (final IOException e1) {
            // No fallocate? try truncate
            try {
                final String[] truncate = new String[] { "truncate", "-s", sizeStr, absPath };
                RunCmdUtils.runCmd(truncate, this.file);
            }
            catch (final IOException e2) {
                // No truncate? try dd
                // Need to set the dd block size (bs=1 is REALLY slow)
                final String sizeStrKo = Long.toString(this.size / 1024);
                final String[] dd = new String[] { "dd", "if=/dev/zero", "of=" + absPath, "bs=1024",
                        "count=" + sizeStrKo };
                RunCmdUtils.runCmd(dd, this.file);
            }
        }

        // Create the file system
        // TODO: add mkfs options
        // May have to respond to some questions, for example with ext2/3/4 (Proceed anyway? (y,n))
        final String[] mkfs = new String[] { "/sbin/mkfs", "-t", this.type, absPath };
        RunCmdUtils.runCmd(mkfs, this.file, "y\n", new String[] { "LANG", "C", "LANGUAGE", "C" });
    }

    /**
     * Create a new {@link UnixMount} to mount the {@link UnixFsFile} in the given directory.
     * 
     * @param mountPoint
     * @return {@link UnixMount} to mount the {@link UnixFsFile} in the given directory.
     */
    public final UnixMount newUnixMount(final File mountPoint) {
        // Add the option loop
        String opts = "loop";
        if (this.mopts != null) {
            opts = opts + "," + this.mopts;
        }
        return new UnixMount(mountPoint, this.file.getAbsolutePath(), opts, this.type);
    }
}
