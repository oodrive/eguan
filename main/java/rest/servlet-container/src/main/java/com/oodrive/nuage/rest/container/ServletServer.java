package com.oodrive.nuage.rest.container;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.rest.jaxrs.JaxRsConfiguredApplication;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * JAX-RS application wrapper combining a {@link JettyConfigurationContext configuration} with a given
 * {@link WebAppContext} into a runnable server based on a Jetty {@link Server}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class ServletServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServletServer.class);

    /**
     * The possible states of the server.
     * 
     * 
     */
    public enum State {
        /**
         * State of failure after {@link ServletServer#start()} or {@link ServletServer#stop()} throws an exception.
         */
        FAILED,
        /**
         * Transitional state between {@link #STOPPED} and (@link #STARTED} or {@link #FAILED}, considered as running
         * state.
         */
        STARTING,
        /**
         * State of running after successfully returning from {@link ServletServer#start()}.
         */
        STARTED,
        /**
         * Transitional state between {@link #STARTED} and (@link #STOPPED} or {@link #FAILED}.
         */
        STOPPING,
        /**
         * State following an successful {@link ServletServer#init()} or {@link ServletServer#stop()}.
         */
        STOPPED,
        /**
         * State following construction or after a successful {@link ServletServer#fini()}.
         */
        NOT_INITIALIZED,
        /**
         * Default state provided after successful {@link ServletServer#init()}, if the running state of the server
         * cannot be determined.
         */
        UNKNOWN_RUNNING_STATE;
    }

    /**
     * The IP address to which the {@link #server} binds.
     * 
     * @see ServerAddressConfigKey
     */
    private final InetAddress serverAddress;

    /**
     * The TCP port to which the {@link #server} binds.
     */
    private final int serverPort;

    /**
     * Whether to include shutdown of the {@link #server} in Jetty's {@link ShutdownThread}.
     * 
     * @see JettyStopAtShutdownConfigKey
     */
    private final Boolean stopAtShutdown;

    /**
     * The {@link JaxRsConfiguredApplication} passed as constructor parameter.
     */
    private final JaxRsConfiguredApplication jaxRsApp;

    /**
     * Lock for all methods depending on the {@link #initialized} state.
     */
    private final Object stateLock = new Object();

    @GuardedBy("stateLock")
    private volatile boolean initialized = false;

    @GuardedBy("stateLock")
    private Server server;

    /**
     * The context path for rest
     */
    private final String restContextPath;

    /**
     * The context path for webui
     */
    private final String webuiContextPath;

    /**
     * The war name for webui
     */
    private final String webuiWarName;

    private static final Handler[] EMPTY_HANDLER_ARRAY = new Handler[0];

    /**
     * the root path for the REST servlet
     */
    private static final String REST_SERVLET_ROOTPATH = "/*";

    /**
     * The {@link URL} representing the web application's resource base.
     * 
     * A valid {@link URL} instance is least likely to be rejected by Jetty upon initialization.
     * 
     * @see WebAppContext#setResourceBase(String)
     */
    private final URL resourceBase;

    /**
     * Constructs a server instance from a {@link MetaConfiguration} and a given {@link WebAppContext}.
     * 
     * @param configuration
     *            a non-<code>null</code> {@link MetaConfiguration} initialized with a {@link JettyConfigurationContext}
     * @param jaxRsApp
     *            a non-<code>null</code>, initialized {@link JaxRsConfiguredApplication}
     */
    public ServletServer(@Nonnull final MetaConfiguration configuration,
            @Nonnull final JaxRsConfiguredApplication jaxRsApp) {
        this.jaxRsApp = Objects.requireNonNull(jaxRsApp);

        // gets bind address and port from the config to instantiate the server
        serverAddress = ServerAddressConfigKey.getInstance().getTypedValue(configuration);
        serverPort = ServerPortConfigKey.getInstance().getTypedValue(configuration);
        assert (serverAddress != null) && (serverPort > ServerPortConfigKey.MIN_VALUE);

        // gets shutdown behavior from the config
        stopAtShutdown = JettyStopAtShutdownConfigKey.getInstance().getTypedValue(configuration);
        assert (stopAtShutdown != null);

        restContextPath = RestContextPathConfigKey.getInstance().getTypedValue(configuration);
        assert (restContextPath != null);

        resourceBase = RestResourceBaseConfigKey.getInstance().getTypedValue(configuration);

        webuiContextPath = WebUiContextPathConfigKey.getInstance().getTypedValue(configuration);
        webuiWarName = WebUiWarNameConfigKey.getInstance().getTypedValue(configuration);
    }

    /**
     * Initializes the runtime state of the server.
     * 
     * This method transitions this instance into the {@link #isInitialized() initialized} state, where all runtime
     * parameters have been set and calling {@link #start()} won't throw an exception.
     * 
     * @throws IllegalStateException
     *             if configuration and/or initialization fails
     */
    public final void init() throws IllegalStateException {
        synchronized (stateLock) {
            if (initialized) {
                LOGGER.warn("Already initialized");
                return;
            }

            final ArrayList<Handler> handlerList = new ArrayList<Handler>();

            // REST
            final WebAppContext restWebContext = new WebAppContext();
            restWebContext.setResourceBase(resourceBase.toExternalForm());
            final ServletContainer jerseyServlet = new ServletContainer(jaxRsApp);
            restWebContext.setContextPath(restContextPath);
            restWebContext.addServlet(new ServletHolder(jerseyServlet), REST_SERVLET_ROOTPATH);
            handlerList.add(restWebContext);

            // WEBUI (optional)
            if (!webuiWarName.isEmpty()) {
                if (Files.exists(new File(webuiWarName).toPath())) {
                    final WebAppContext webuiWebContext = new WebAppContext();
                    webuiWebContext.setContextPath(webuiContextPath);
                    webuiWebContext.setWar(webuiWarName);
                    handlerList.add(webuiWebContext);
                }
                else {
                    LOGGER.error("Webui war: " + webuiWarName + " can not be found.");
                }
            }
            final ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(handlerList.toArray(EMPTY_HANDLER_ARRAY));

            server = new Server(new InetSocketAddress(serverAddress, serverPort));
            server.setHandler(contexts);
            server.setStopAtShutdown(stopAtShutdown);

            initialized = true;
        }
    }

    /**
     * Gets the initialization status of this instance.
     * 
     * @return initialization status
     */
    public final boolean isInitialized() {
        return initialized;
    }

    /**
     * Resets the server to the un-initialized state it's in right after construction.
     * 
     * Returns without change if the server is not yet initialized.
     * 
     * @throws IllegalStateException
     *             if the server is {@link #isStarted() started}
     */
    public final void fini() throws IllegalStateException {
        if (isStarted()) {
            throw new IllegalStateException("Server is running");
        }
        synchronized (stateLock) {
            if (!initialized) {
                LOGGER.warn("Not initialized");
                return;
            }
            server = null;
            initialized = false;
        }
    }

    /**
     * Starts the server.
     * 
     * @throws IllegalStateException
     *             if the server is not initalized
     * @throws Exception
     *             if starting the initalized server fails, including a transition to {@link State#FAILED}
     */
    public final void start() throws Exception {
        synchronized (stateLock) {
            if (!initialized) {
                throw new IllegalStateException("Not initialized");
            }
            server.start();
        }
    }

    /**
     * Stops the server.
     * 
     * Does nothing if the server is not initialized.
     * 
     * @throws Exception
     *             if stopping the running server fails, including a transition to {@link State#FAILED}
     */
    public final void stop() throws Exception {
        synchronized (stateLock) {
            if (!initialized) {
                return;
            }
            server.stop();
        }
    }

    /**
     * Returns the started status of the server.
     * 
     * @return <code>true</code> if the server is initialized and {@link #getExecutionState()} returns either
     *         {@link State#STARTING} or {@link State#STARTED}
     */
    public final boolean isStarted() {
        synchronized (stateLock) {
            return initialized && server.isRunning();
        }
    }

    /**
     * Returns the execution state as an {@link State} constant.
     * 
     * Once initialized, the running state of the server is returned.
     * 
     * @see State
     * 
     * @return an {@link State}
     */
    public final State getExecutionState() {
        synchronized (stateLock) {
            if (!initialized) {
                return State.NOT_INITIALIZED;
            }
            try {
                return State.valueOf(server.getState());
            }
            catch (final IllegalArgumentException ie) {
                return State.UNKNOWN_RUNNING_STATE;
            }
        }
    }

    /**
     * Blocks until the {@link ServletServer} is {@link State#STOPPED stopped}.
     * 
     * Returns immediately if the server is already stopped.
     * 
     * @throws InterruptedException
     *             if the thread's execution is abnormally terminated
     * @throws IllegalStateException
     *             if the {@link ServletServer} is not {@link #init() initialized}
     */
    public final void join() throws InterruptedException {
        // not taking the lock, as blocking here would render the server unstoppable
        if (!initialized) {
            throw new IllegalStateException("Not initialized");
        }
        server.join();
    }

}
