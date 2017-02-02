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

import io.eguan.utils.unix.UnixMount;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

/**
 * Simple unit tests for UnixMount.
 * 
 * @author oodrive
 */
public class UnixMountTest {

    /**
     * Directory is null.
     */
    @Test(expected = NullPointerException.class)
    public void dirNull() {
        new UnixMount(null, "dev", "opts", "type");
    }

    /**
     * Directory is a file.
     */
    @Test(expected = IllegalArgumentException.class)
    public void dirNotDir() throws IOException {
        final File file = File.createTempFile("tst", ".tmp");
        try {
            final File dir = new File(file.getAbsolutePath());
            new UnixMount(dir, "dev", "opts", "type");
        }
        finally {
            file.delete();
        }
    }

    /**
     * Directory does not exist.
     */
    @Test(expected = IllegalArgumentException.class)
    public void dirNotExist() {
        final String tempdir = System.getProperty("java.io.tmpdir");
        final File dir = new File(tempdir, "tst-dir-" + System.currentTimeMillis());
        new UnixMount(dir, "dev", "opts", "type");
    }

    /**
     * device is null.
     */
    @Test(expected = NullPointerException.class)
    public void devNull() {
        new UnixMount(new File("/"), null, "opts", "type");
    }

    /**
     * FS type is null.
     */
    @Test(expected = NullPointerException.class)
    public void typeNull() {
        new UnixMount(new File("/"), "dev", "opts", null);
    }

    /**
     * Options are null.
     */
    @Test
    public void optsNull() {
        new UnixMount(new File("/"), "dev", null, "type");
    }
}
