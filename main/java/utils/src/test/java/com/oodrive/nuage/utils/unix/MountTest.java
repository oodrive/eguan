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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test file system and mount classes. The test parameters are the mount options.
 * 
 * @author oodrive
 * @author llambert
 */
@RunWith(value = Parameterized.class)
public class MountTest {

    @Parameters
    public static Collection<Object[]> mountOpts() {
        final Object[][] data = new Object[][] { { null }, { "noatime" } };
        return Arrays.asList(data);
    }

    private final String mopts;

    public MountTest(final String mopts) {
        this.mopts = mopts;
    }

    /**
     * Create a file containing an ext2 FS and mount it in a temporary directory.
     * 
     * @throws IOException
     */
    @Test
    public void mountExt2() throws IOException {
        final File file = File.createTempFile("tst-fs-", ".fs");
        try {
            final UnixFsFile fsFile = new UnixFsFile(file, 20 * 1024 * 1024, "ext2", this.mopts);
            fsFile.create();

            final File mountPoint = Files.createTempDirectory("tst-mnt-").toFile();
            try {
                final UnixMount unixMount = fsFile.newUnixMount(mountPoint);
                unixMount.mount();

                // Must have a lost+found directory
                try {
                    final File lf = new File(mountPoint, "lost+found");
                    assertTrue(lf.isDirectory());
                }
                finally {
                    unixMount.umount();
                }
            }
            finally {
                mountPoint.delete();
            }

        }
        finally {
            file.delete();
        }
    }
}
