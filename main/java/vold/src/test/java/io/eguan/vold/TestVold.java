package io.eguan.vold;

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

import io.eguan.utils.RunCmdErrorException;
import io.eguan.utils.RunCmdUtils;
import io.eguan.vold.Vold;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ObjectArrays;

/**
 * Test launch of the vold.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public class TestVold {

    @Test
    public void testNoArg() throws IOException {
        try {
            launchVold(null);
            throw new AssertionError("Not reached");
        }
        catch (final RunCmdErrorException e) {
            Assert.assertEquals(1, e.getExitValue());
        }
    }

    @Test
    public void testDirNotExist() throws IOException {
        final File tempDir = Files.createTempDirectory("vold-launch").toFile();
        tempDir.delete();
        try {
            launchVold(new String[] { tempDir.getAbsolutePath() });
            throw new AssertionError("Not reached");
        }
        catch (final RunCmdErrorException e) {
            Assert.assertEquals(1, e.getExitValue());
        }
    }

    @Test
    public void testConfigNotExist() throws IOException {
        final Path tempDirPath = Files.createTempDirectory("vold-launch");
        try {
            final File tempDir = tempDirPath.toFile();
            launchVold(new String[] { tempDir.getAbsolutePath() });
            throw new AssertionError("Not reached");
        }
        catch (final RunCmdErrorException e) {
            Assert.assertEquals(2, e.getExitValue());
        }
        finally {
            io.eguan.utils.Files.deleteRecursive(tempDirPath);
        }
    }

    @Test
    public void testConfigEmpty() throws IOException {
        final Path tempDirPath = Files.createTempDirectory("vold-launch");
        try {
            final File tempDir = tempDirPath.toFile();
            final File config = new File(tempDir, "vold.cfg");
            try {
                config.createNewFile();
                launchVold(new String[] { tempDir.getAbsolutePath() });
                throw new AssertionError("Not reached");
            }
            catch (final RunCmdErrorException e) {
                Assert.assertEquals(2, e.getExitValue());
            }
            finally {
                config.delete();
            }
        }
        finally {
            io.eguan.utils.Files.deleteRecursive(tempDirPath);
        }
    }

    /**
     * Launch VOLD.
     * 
     * @param javaArgs
     *            VOLD arguments, may be <code>null</code>
     * @throws IOException
     */
    private void launchVold(final String[] javaArgs) throws IOException {
        final String classpath = System.getProperty("java.class.path");
        final String main = Vold.class.getName();
        String[] args = new String[] { "java", "-cp", classpath, main };
        if (javaArgs != null) {
            args = ObjectArrays.concat(args, javaArgs, String.class);
        }
        RunCmdUtils.runCmd(args, this, true);
    }

    /**
     * Run the VOLD name for test/debug purpose. Define the environment variable 'voldtest_dir' containing the path of
     * the VOLD directory to launch that VOLD.
     */
    @Test
    public final void main() {
        final String voldDir = System.getenv("voldtest_dir");
        if (voldDir != null) {
            Vold.main(new String[] { voldDir });
        }
    }
}
