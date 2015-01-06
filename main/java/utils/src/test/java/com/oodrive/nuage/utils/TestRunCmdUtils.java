package com.oodrive.nuage.utils;

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

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.utils.RunCmdUtils.RunningCmd;

/**
 * Unit tests for {@link RunCmdUtils}.
 * 
 * @author oodrive
 * @author ebredzinski
 */
public class TestRunCmdUtils {

    @Test
    public void testCmd1Ok() throws IOException {
        final File f = File.createTempFile("runcmd", ".tmp");
        try {
            final String[] stat = new String[] { "stat", f.getAbsolutePath() };
            RunCmdUtils.runCmd(stat, f.getAbsolutePath());
        }
        finally {
            f.delete();
        }
    }

    @Test(expected = IOException.class)
    public void testCmd1Ko() throws IOException {
        final File f = File.createTempFile("runcmd", ".tmp");
        f.delete();
        final String[] stat = new String[] { "stat", f.getAbsolutePath() };
        RunCmdUtils.runCmd(stat, f.getAbsolutePath(), true);
    }

    @Test
    public void testCmd3Ok() throws IOException {
        final File f = File.createTempFile("runcmd", ".tmp");
        try {
            final String[] test = new String[] { "bash", "-c", "test -e $TST_FILE" };
            final String[] addEnv = new String[] { "TST_FILE", f.getAbsolutePath() };
            RunCmdUtils.runCmd(test, f.getAbsolutePath(), "", addEnv);
        }
        finally {
            f.delete();
        }
    }

    @Test(expected = IOException.class)
    public void testCmd3Ko() throws IOException {
        final File f = File.createTempFile("runcmd", ".tmp");
        f.delete();
        final String[] test = new String[] { "bash", "-c", "test -e $TST_FILE" };
        final String[] addEnv = new String[] { "TST_FILE", f.getAbsolutePath() };
        RunCmdUtils.runCmd(test, f.getAbsolutePath(), "", addEnv, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCmd3Env() throws IOException {
        final String[] test = new String[] { "bash", "-c", "test -e $TST_FILE" };
        final String[] addEnv = new String[] { "TST_FILE" };
        RunCmdUtils.runCmd(test, this, "", addEnv, true);
    }

    @Test
    public void testCmdOutput() throws IOException {
        final File f = File.createTempFile("runcmd", ".tmp");
        try {
            final String[] stat = new String[] { "ls", f.getAbsolutePath() };
            final StringBuilder str = RunCmdUtils.runCmd(stat, f.getAbsolutePath(), true);
            Assert.assertEquals(f.getAbsolutePath(), str.toString());
        }
        finally {
            f.delete();
        }
    }

    @Test(expected = RunCmdErrorException.class)
    public void testRunningCmd() throws IOException, InterruptedException {
        final String[] sleep = new String[] { "sleep", "3" };
        final RunningCmd cmd = RunCmdUtils.newRunningCmd(sleep, this);
        final Thread killer = new Thread(new Runnable() {
            @Override
            public final void run() {
                try {
                    Thread.sleep(1000);
                }
                catch (final InterruptedException e) {
                    // Ignore
                }
                cmd.kill();
            }
        });
        killer.start();
        cmd.run();
        killer.join();
    }

    @Test
    public void testRunningCmdKillBeforeRun() throws IOException {
        final String[] sleep = new String[] { "sleep", "1" };
        final RunningCmd cmd = RunCmdUtils.newRunningCmd(sleep, this);
        cmd.kill();
        cmd.run();
    }
}
