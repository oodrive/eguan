package io.eguan.utils;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to run commands.
 *
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 */
public final class RunCmdUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunCmdUtils.class);

    // Use the default Charset for console input/ouput
    private static final Charset CONSOLE_CHARSET = Charset.defaultCharset();

    public static class RunningCmd {
        private final AtomicReference<Process> p;
        private final String[] cmdArray;
        private final Object peer;

        private RunningCmd(final String[] cmdArray, final Object peer) {
            this.p = new AtomicReference<>();
            this.cmdArray = Arrays.copyOf(cmdArray, cmdArray.length); // defensive copy of input array
            this.peer = peer;
        }

        /**
         * Runs the command. Throws an exception if the command fails (return code != 0).
         *
         * @throws IOException
         */
        public final void run() throws IOException {
            final Runtime r = Runtime.getRuntime();
            p.set(r.exec(cmdArray));
            doWait(p.get(), cmdArray, peer, true, false);
        }

        /**
         * Kill a running process.
         */
        public final void kill() {
            final Process process = p.get();
            if (process != null) {
                process.destroy();
            }
        }

    }

    /**
     * No instance.
     */
    private RunCmdUtils() {
        throw new AssertionError();
    }

    /**
     * Create a running command.
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @return the Running command
     */
    public static final RunningCmd newRunningCmd(final String[] cmdArray, final Object peer) {
        return new RunningCmd(cmdArray, peer);
    }

    /**
     * Runs the command. Throws an exception if the command fails (return code != 0).
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @return the output of the command
     *
     * @throws IOException
     *             if the command does not exist or if it fails
     */
    public static final void runCmd(final String[] cmdArray, final Object peer) throws IOException {
        runCmd(cmdArray, peer, false, false);
    }

    /**
     * Runs the command. Throws an exception if the command fails (return code != 0).
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @return the output of the command
     *
     * @throws IOException
     *             if the command does not exist or if it fails
     */
    public static final StringBuilder runCmd(final String[] cmdArray, final Object peer, final boolean getOutput)
            throws IOException {
        return runCmd(cmdArray, peer, false, getOutput);
    }

    /**
     * Runs the command. Throws an exception if the command fails (return code != 0).
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @param displayErr
     *            if <code>true</code>, display the command stderr in <code>System.err</code>.
     * @return the output of the command
     * @throws IOException
     *             if the command does not exist or if it fails
     */
    public static final StringBuilder runCmd(final String[] cmdArray, final Object peer, final boolean displayErr,
            final boolean getOutput) throws IOException {
        final Runtime r = Runtime.getRuntime();
        final Process p = r.exec(cmdArray);
        return doWait(p, cmdArray, peer, displayErr, getOutput);
    }

    /**
     * Runs the command. Throws an exception if the command fails (return code != 0). This version passes some input to
     * the command.
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @param input
     *            text written in the command input stream.
     * @param addEnv
     *            additional environment variables or <code>null</code>. The array is a list of key/value pairs.
     * @throws IOException
     *             if the command does not exist or if it fails
     */
    public static void runCmd(final String[] cmdArray, final Object peer, final String input, final String[] addEnv)
            throws IOException {
        runCmd(cmdArray, peer, input, addEnv, false);
    }

    /**
     * Runs the command. Throws an exception if the command fails (return code != 0). This version passes some input to
     * the command.
     *
     * @param cmdArray
     *            command and arguments.
     * @param peer
     *            object on which the command is run.
     * @param input
     *            text written in the command input stream.
     * @param addEnv
     *            additional environment variables or <code>null</code>. The array is a list of key/value pairs.
     * @param displayErr
     *            if <code>true</code>, display the command stderr in <code>System.err</code>.
     * @throws IOException
     */
    public static void runCmd(final String[] cmdArray, final Object peer, final String input, final String[] addEnv,
            final boolean displayErr) throws IOException {

        final ProcessBuilder processBuilder = new ProcessBuilder(cmdArray);
        if (addEnv != null) {
            // Add environment variables
            final int count = addEnv.length;
            if ((count % 2) != 0) {
                throw new IllegalArgumentException("addEnv length=" + addEnv.length);
            }
            final Map<String, String> env = processBuilder.environment();
            for (int i = 0; i < count; i += 2) {
                env.put(addEnv[i], addEnv[i + 1]);
            }
        }
        final Process p = processBuilder.start();
        try {
            final PrintStream pStdin = new PrintStream(p.getOutputStream(), false, CONSOLE_CHARSET.name());
            try {
                // Verbose mode
                if (displayErr) {
                    displayErr(p);
                }

                // Write input
                pStdin.print(input);
                pStdin.flush();

                final int exitValue = p.waitFor();
                if (exitValue != 0) {
                    throw new RunCmdErrorException(Arrays.toString(cmdArray) + " failed on " + peer + ", status="
                            + exitValue, exitValue);
                }
            }
            finally {
                pStdin.close();
            }
        }
        catch (final InterruptedException e) {
            throw new IOException(cmdArray[0] + " interrupted", e);
        }
    }

    /**
     * Gets the output of the executed command.
     *
     * @param process
     *            the given process
     * @param builder
     *            the builder which will contain the result
     *
     * @return the Thread which reads the output
     */
    private static final Thread getOutput(final Process process, final StringBuilder builder) {
        final InputStream is = process.getInputStream();
        final Thread outputPrinter = new Thread(new Runnable() {

            @Override
            public final void run() {
                final byte[] buf = new byte[1024];
                try {
                    int readLen;
                    while ((readLen = is.read(buf)) >= 0) {
                        builder.append(new String(buf, 0, readLen, CONSOLE_CHARSET).trim());
                    }
                }
                catch (final IOException e) {
                    LOGGER.warn("Exception ignored", e);
                }
                finally {
                    try {
                        is.close();
                    }
                    catch (final IOException e) {
                        // Ignored
                    }
                }
            }
        });
        outputPrinter.start();
        return outputPrinter;
    }

    /**
     * Display the stderr of the given process.
     *
     * @param process
     */
    private static final Thread displayErr(final Process process) {
        final InputStream is = process.getErrorStream();
        final Thread stderrPrinter = new Thread(new Runnable() {

            @Override
            public final void run() {
                final byte[] buf = new byte[1024];
                int readLen;
                try {
                    while ((readLen = is.read(buf)) >= 0) {
                        System.err.write(buf, 0, readLen);
                    }
                }
                catch (final IOException e) {
                    LOGGER.warn("Exception ignored", e);
                }
                finally {
                    try {
                        is.close();
                    }
                    catch (final IOException e) {
                        // Ignored
                    }
                }
            }
        });
        stderrPrinter.start();
        return stderrPrinter;
    }

    private final static StringBuilder doWait(final Process p, final String[] cmdArray, final Object peer,
            final boolean displayErr, final boolean getOutput) throws IOException {
        try {
            // Verbose mode
            Thread errorThread = null;
            if (displayErr) {
                errorThread = displayErr(p);
            }

            StringBuilder strBuilder = null;
            Thread outputThread = null;
            if (getOutput) {
                strBuilder = new StringBuilder();
                outputThread = getOutput(p, strBuilder);
            }

            final int exitValue = p.waitFor();

            if (errorThread != null) {
                errorThread.join();
            }
            if (outputThread != null) {
                outputThread.join();
            }

            if (exitValue != 0) {
                throw new RunCmdErrorException(Arrays.toString(cmdArray) + " failed on " + peer + ", status="
                        + exitValue, exitValue);
            }
            else {
                return strBuilder;
            }
        }
        catch (final InterruptedException e) {
            throw new IOException(cmdArray[0] + " interrupted", e);
        }
    }
}
