package io.eguan.main;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.rest.container.JettyConfigurationContext;
import io.eguan.rest.container.ServerAddressConfigKey;
import io.eguan.rest.container.ServerPortConfigKey;
import io.eguan.utils.LogUtils;
import io.eguan.utils.RunCmdErrorException;
import io.eguan.utils.RunCmdUtils;
import io.eguan.utils.RunCmdUtils.RunningCmd;
import io.eguan.vold.Vold;
import io.eguan.vold.adm.RestLauncher;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Eguan {

    static final Logger LOGGER = LoggerFactory.getLogger(Eguan.class);

    /**
     * Eguan shutdown hook.
     * 
     */
    class EguanShutdownHook implements Runnable {

        @Override
        public final void run() {

            cancelled.set(true);

            // Stop cinder process
            stopCinder();

            // Stop REST server
            stopRestServer();

            // Stop VOLD
            stopVold();
        }
    }

    /** No termination */
    private static final int EGUAN_OK = -1;
    /** Eguan already cancelled */
    private static final int EGUAN_CANCELLED = -2;

    /** Normal Termination */
    private static final int EXIT_END = 0;
    /** Invalid usage */
    private static final int EXIT_USAGE = 1;

    /** Config failed */
    private static final int EXIT_CONFIG_VOLD_FAILED = 2;
    private static final int EXIT_CONFIG_REST_FAILED = 3;
    private static final int EXIT_CONFIG_CINDER_FAILED = 4;

    /** Init failed */
    private static final int EXIT_INIT_VOLD_FAILED = 5;
    private static final int EXIT_INIT_REST_FAILED = 6;

    /** Start failed */
    private static final int EXIT_VOLD_START_FAILED = 7;
    private static final int EXIT_REST_START_FAILED = 8;
    private static final int EXIT_CONNECT_REST_FAILED = 9;

    /** Mark the eguan as cancelled */
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    @GuardedBy(value = "cancelled")
    private Vold vold;
    @GuardedBy(value = "cancelled")
    private RestLauncher restLauncher;
    @GuardedBy(value = "cancelled")
    private RunningCmd cinder;

    private static final long MAX_ACCEPTABLE_DURATION = 30 * 1000; // 30s
    private static final int MAX_COUNT_WITH_NONACCEPTABLE_DURATION = 10;

    private static final int MAX_RETRY_SERVER_CONNECTION = 18; // 10s*18 = 3 min
    private static final int WAIT_BETWEEN_CONNECTION = 10000; // 10s

    /**
     * Context to load as default from internal resources.
     */
    private static final String DEFAULT_CONTEXT_RESOURCE = "/vold-adm.xml";

    /**
     * 
     * @param args
     */
    public static void main(final String[] args) {

        // Get uncaught exceptions
        Thread.setDefaultUncaughtExceptionHandler(new EguanUncaughtExceptionHandler());

        // Configure SysLog appender for logback
        LogUtils.initSysLog();

        final Eguan eguan = new Eguan();

        if (args.length < 2) {
            LOGGER.error("Bad number of arguments");
            System.exit(EXIT_USAGE);
        }

        // Check vold directory
        final File voldDir = new File(args[0]);
        if (!voldDir.isDirectory()) {
            LOGGER.error(args[0] + " is not a directory");
            System.exit(EXIT_CONFIG_VOLD_FAILED);
        }

        // Run an extra command?
        final String[] cinderCmd;
        if (args.length >= 3) {
            cinderCmd = new String[] { args[2] };
            {// Check command exists and is executable
                final File cinderCmdFile = new File(cinderCmd[0]);
                if (!cinderCmdFile.canExecute()) {
                    LOGGER.error("Cinder path is not an executable cmd");
                    System.exit(EXIT_CONFIG_CINDER_FAILED);
                }
            }
        }
        else {
            cinderCmd = null;
        }

        // Shutdown hook
        final Thread hook = new Thread(eguan.new EguanShutdownHook(), "Eguan shutdown hook");
        Runtime.getRuntime().addShutdownHook(hook);

        // Start VOLD
        int status = eguan.startVold(voldDir);

        if (status == EGUAN_CANCELLED) {
            return;
        }
        else if (status != EGUAN_OK) {
            System.exit(status);
        }

        // Start REST
        status = eguan.startRestServer(args[1]);

        if (status == EGUAN_CANCELLED) {
            return;
        }
        else if (status != EGUAN_OK) {
            System.exit(status);
        }

        if (cinderCmd == null) {
            // No extra command, wait for end of the rest launcher
            status = eguan.waitRestServer();
        }
        else {
            // Start Cinder
            status = eguan.startCinder(cinderCmd);

        }
        if (status != EGUAN_CANCELLED) {
            System.exit(status);
        }
    }

    /**
     * Create, init and start a vold instance.
     * 
     * @param voldDir
     *            the vold directory
     * @return the status of the start
     */
    private final int startVold(final File voldDir) {
        final Vold voldTemp;

        synchronized (cancelled) {
            if (cancelled.get()) {
                return EGUAN_CANCELLED;
            }
            vold = new Vold(voldDir);
            assert vold != null;
            voldTemp = vold;
        }

        try {
            voldTemp.init(false);
            try {
                voldTemp.start();
            }
            catch (final Throwable t) {
                LOGGER.error("Failed to start vold", t);
                return EXIT_VOLD_START_FAILED;
            }
        }
        catch (final Throwable t) {
            LOGGER.error("Failed to initialize vold", t);
            return EXIT_INIT_VOLD_FAILED;
        }
        return EGUAN_OK;
    }

    /**
     * Stop and fini a vold instance.
     * 
     */
    private final void stopVold() {
        final Vold voldTemp;

        synchronized (cancelled) {
            voldTemp = vold;
            vold = null;
        }
        if (voldTemp != null) {
            LOGGER.info("VOLD shutdown requested");
            try {
                voldTemp.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD stop", t);
            }
            try {
                voldTemp.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD shutdown", t);
            }
        }
        LOGGER.info("VOLD shutdown completed");
    }

    /**
     * Create, init and start a REST server.
     * 
     * @param configPath
     *            the path which contains the config file for jetty.
     * 
     * @return the status of the start
     */
    private final int startRestServer(final String configPath) {

        if (cancelled.get()) {
            return EGUAN_CANCELLED;
        }
        try (final FileInputStream configInputStream = new FileInputStream(configPath);
                final InputStream contextInputStream = Eguan.class.getResourceAsStream(DEFAULT_CONTEXT_RESOURCE);
                final FileInputStream propertiesInputStream = new FileInputStream(configPath)) {

            final RestLauncher restLauncherTemp;

            synchronized (cancelled) {
                restLauncher = new RestLauncher(configInputStream, contextInputStream);
                restLauncherTemp = restLauncher;
            }
            try {
                restLauncherTemp.init();
                try {
                    restLauncherTemp.start();

                    // Try to connect to the REST server
                    int count = MAX_RETRY_SERVER_CONNECTION;
                    boolean succeed = false;
                    final SocketChannel socketChannel = SocketChannel.open();
                    try {
                        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                                propertiesInputStream, JettyConfigurationContext.getInstance());

                        InetAddress serverAddress = ServerAddressConfigKey.getInstance().getTypedValue(configuration);
                        final int serverPort = ServerPortConfigKey.getInstance().getTypedValue(configuration);
                        while (count-- != 0) {
                            try {
                                if (serverAddress.isAnyLocalAddress()) {
                                    serverAddress = InetAddress.getLoopbackAddress();
                                }
                                succeed = socketChannel.connect(new InetSocketAddress(serverAddress, serverPort));
                                break;
                            }
                            catch (final Throwable t) {
                                // Ignore and retry
                                LOGGER.error("Failed to connect REST server");
                            }
                            Thread.sleep(WAIT_BETWEEN_CONNECTION);
                        }
                    }
                    finally {
                        socketChannel.close();
                    }
                    if (!succeed) {
                        LOGGER.error("Failed to start rest");
                        return EXIT_CONNECT_REST_FAILED;
                    }
                }
                catch (final Throwable t) {
                    LOGGER.error("Failed to start rest", t);
                    return EXIT_REST_START_FAILED;
                }
            }
            catch (final Throwable t) {
                LOGGER.error("Failed to init rest", t);
                return EXIT_INIT_REST_FAILED;
            }
        }
        catch (final Throwable t) {
            LOGGER.error("Failed to init rest", t);
            return EXIT_CONFIG_REST_FAILED;
        }
        return EGUAN_OK;
    }

    /**
     * Stop the instance of the REST server.
     * 
     */
    private final void stopRestServer() {
        final RestLauncher restLauncherTemp;

        synchronized (cancelled) {
            restLauncherTemp = restLauncher;
            restLauncher = null;
        }
        if (restLauncherTemp != null) {
            LOGGER.info("Rest Launcher shutdown requested");
            try {
                restLauncherTemp.stop();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD stop", t);
            }
            try {
                restLauncherTemp.fini();
            }
            catch (final Throwable t) {
                LOGGER.warn("Error during VOLD shutdown", t);
            }
        }
        LOGGER.info("Rest Launcher shutdown completed");
    }

    /**
     * Wait the end of the REST server.
     * 
     * @return the status
     */
    private final int waitRestServer() {
        try {
            final RestLauncher restLauncherTemp;

            synchronized (cancelled) {
                if (cancelled.get()) {
                    return EGUAN_CANCELLED;
                }
                assert restLauncher != null;
                restLauncherTemp = restLauncher;
            }
            restLauncherTemp.join();
        }
        catch (final Throwable t) {
            LOGGER.error("Interrupted", t);
        }
        return EXIT_END;
    }

    /**
     * Create and start cinder.
     * 
     * @param cinderCmd
     *            the command used to launch cinder
     * @return the status
     */
    private final int startCinder(final String[] cinderCmd) {
        synchronized (cancelled) {
            if (cancelled.get()) {
                return EGUAN_CANCELLED;
            }
            cinder = RunCmdUtils.newRunningCmd(cinderCmd, Eguan.class);
        }
        return waitCinder();
    }

    /**
     * Stop the process cinder.
     */
    private final void stopCinder() {
        final RunningCmd cinderTmp;

        synchronized (cancelled) {
            cinderTmp = cinder;
            cinder = null;
        }
        if (cinderTmp != null) {
            LOGGER.info("Cinder shutdown requested");
            cinderTmp.kill();
        }
        LOGGER.info("Cinder shutdown completed");
    }

    /**
     * Wait the end and monitor cinder process.
     * 
     * @return the status
     */
    private final int waitCinder() {
        int count = 0;
        long startTime = 0;
        int exitValue = EXIT_END;

        while (count < MAX_COUNT_WITH_NONACCEPTABLE_DURATION) {

            final RunningCmd cinderTemp;
            synchronized (cancelled) {
                if (cancelled.get()) {
                    return EGUAN_CANCELLED;
                }
                assert cinder != null;
                cinderTemp = cinder;
            }
            try {
                startTime = System.currentTimeMillis();
                cinderTemp.run();
            }
            catch (final RunCmdErrorException e) {
                LOGGER.error("Failed to start cinder", e);
                exitValue = e.getExitValue();
            }
            catch (final Throwable t) {
                LOGGER.error("Failed to start cinder", t);
            }
            // cinder is not running
            final long duration = System.currentTimeMillis() - startTime;
            if (duration < MAX_ACCEPTABLE_DURATION) {
                count++;
            }
            else {
                count = 0;
            }
        }
        LOGGER.error("End of Cinder");
        // Return the last exit value
        return exitValue;
    }
}
