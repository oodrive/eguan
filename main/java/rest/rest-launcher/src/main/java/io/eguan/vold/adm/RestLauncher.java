package io.eguan.vold.adm;

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

import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.rest.container.JettyConfigurationContext;
import io.eguan.rest.container.ServletServer;
import io.eguan.rest.jaxrs.JaxRsAppContext;
import io.eguan.rest.jaxrs.JaxRsConfiguredApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launcher for the integrated REST runtime.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
@Immutable
public final class RestLauncher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestLauncher.class);

    /**
     * Configuration resource name to load as default.
     */
    private static final String DEFAULT_CONFIG_RESOURCE = "/" + RestLauncher.class.getSimpleName() + ".properties";

    /**
     * Context to load as default from internal resources.
     */
    private static final String DEFAULT_CONTEXT_RESOURCE = "/vold-adm.xml";

    /**
     * Command line usage message.
     */
    private static final String USAGE = String.format(
            "usage : java %s [<configuration file> <context initialization file>]",
            RestLauncher.class.getCanonicalName());

    /**
     * Lock guarding the initialized state, i.e. {@link RestLauncher#initialized} and {@link #servletServer}.
     */
    private final Object initLock = new Object();

    /**
     * Initialized state field.
     */
    @GuardedBy("initLock")
    private volatile boolean initialized = false;

    /**
     * Runtime servlet server instance.
     */
    @GuardedBy("initLock")
    private ServletServer servletServer;

    /**
     * Reference to the configuration input resource to load on {@link #init()}.
     */
    private final InputStream configInput;

    /**
     * Reference to the context input resource to load on {@link #init()}.
     */
    private final InputStream contextInput;

    /**
     * Constructs an instance initialized from default configuration and context resources.
     * 
     * @throws IllegalStateException
     *             if any of the default resources cannot be found or referenced
     */
    public RestLauncher() throws IllegalStateException {
        final Class<? extends RestLauncher> clazz = getClass();

        this.configInput = clazz.getResourceAsStream(DEFAULT_CONFIG_RESOURCE);
        this.contextInput = clazz.getResourceAsStream(DEFAULT_CONTEXT_RESOURCE);
    }

    /**
     * Constructs an instance from existing configuration and context files.
     * 
     * @param configInput
     *            a {@link URI} referencing the file from which to read a {@link MetaConfiguration} with a
     *            {@link JettyConfigurationContext}
     * @param contextInput
     *            a {@link URI} referencing a file in Jetty XML format configuring a {@link JaxRsAppContext} instance
     */
    public RestLauncher(@Nonnull final InputStream configInput, @Nonnull final InputStream contextInput) {
        this.configInput = Objects.requireNonNull(configInput);
        this.contextInput = Objects.requireNonNull(contextInput);
    }

    /**
     * Initializes the launcher.
     * 
     * @throws IllegalStateException
     *             if this instance cannot be initialized
     */
    public final void init() throws IllegalStateException {
        synchronized (initLock) {
            if (initialized) {
                LOGGER.warn("Already initialized");
                return;
            }
            try {
                final MetaConfiguration configuration = MetaConfiguration.newConfiguration(configInput,
                        JettyConfigurationContext.getInstance());
                final JaxRsConfiguredApplication jerseyApplication = new JaxRsConfiguredApplication(contextInput);
                servletServer = new ServletServer(configuration, jerseyApplication);
                servletServer.init();
                initialized = true;
            }
            catch (NullPointerException | IllegalArgumentException | IOException e) {
                LOGGER.error("Exception on initialization", e);
                throw new IllegalStateException(e);
            }
            catch (final ConfigValidationException ce) {
                final StringBuilder errMsg = new StringBuilder();
                for (final ValidationError currError : ce.getValidationReport()) {
                    errMsg.append(ValidationError.getFormattedErrorReport(currError));
                }
                LOGGER.error("Configuration exception on initialization: " + errMsg.toString(), ce);
                throw new IllegalStateException(ce);
            }
        }
    }

    /**
     * Returns the initialization state as a result from calls to {@link #init()} and {@link #fini()}.
     * 
     * @return the initialization state
     */
    public final boolean isInitialized() {
        return initialized;
    }

    /**
     * Resets this instance to an uninitialized state.
     * 
     * Calling this on an {@link #isInitialized() initialized} instance does nothing.
     */
    public final void fini() {
        synchronized (initLock) {
            if (!initialized) {
                LOGGER.warn("Not initialized");
                return;
            }
            servletServer.fini();
            servletServer = null;
            initialized = false;
        }
    }

    /**
     * Starts the launcher.
     * 
     * @throws IllegalStateException
     *             if the launcher is not {@link #isInitialized() initialized} or starting it fails
     */
    public final void start() throws IllegalStateException {
        synchronized (initLock) {
            if (!initialized) {
                throw new IllegalStateException("Not initialized");
            }
            try {
                servletServer.start();
            }
            catch (final Exception e) {
                LOGGER.error("Exception on start", e);
                // TODO: filter exceptions
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Returns the running state of the server.
     * 
     * In an {@link #isInitialized() uninitialized state}, this returns <code>false</code>.
     * 
     * @return <code>true</code> if the server is starting or started, <code>false</code> otherwise
     */
    public boolean isStarted() {
        synchronized (initLock) {
            return initialized && servletServer.isStarted();
        }
    }

    /**
     * Stops the server.
     * 
     * Does nothing if the server is not initialized.
     * 
     * @throws IllegalStateException
     *             if stopping the server fails
     */
    public final void stop() throws IllegalStateException {
        synchronized (initLock) {
            if (!initialized) {
                return;
            }
            try {
                servletServer.stop();
            }
            catch (final Exception e) {
                LOGGER.error("Exception on start", e);
                // TODO: filter exceptions
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Blocks until the launcher is {@link #stop() stopped}.
     * 
     * Returns immediately if the launcher is already stopped.
     * 
     * @throws InterruptedException
     *             if the launcher's execution is abnormally terminated
     * @throws IllegalStateException
     *             if the launcher is not {@link #isInitialized() initialized}
     */
    public final void join() throws InterruptedException {
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        servletServer.join();
    }

    /**
     * Launches the configured server with optional configuration and context files.
     * 
     * @param args
     *            command line parameters, see {@link #USAGE}
     */
    public static void main(final String[] args) {

        if ((args.length != 0) && (args.length != 2)) {
            System.out.println(USAGE);
            System.exit(1);
        }

        try {
            final RestLauncher launcher;
            if (args.length == 2) {
                final FileInputStream configInputStream = new FileInputStream(args[0]);
                final FileInputStream contextInputStream = new FileInputStream(args[1]);
                launcher = new RestLauncher(configInputStream, contextInputStream);
            }
            else {
                launcher = new RestLauncher();
            }

            launcher.init();

            try {

                launcher.start();
                try {
                    launcher.join();
                }
                finally {
                    launcher.stop();
                }
            }
            finally {
                launcher.fini();
            }
        }
        catch (final Throwable e) {
            LOGGER.error("REST server launch failed", e);
            System.exit(2);
        }
    }
}
