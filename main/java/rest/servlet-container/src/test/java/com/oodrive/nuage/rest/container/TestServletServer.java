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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

import com.oodrive.nuage.configuration.ConfigValidationException;
import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.configuration.ValidConfigurationContext.ContextTestHelper;
import com.oodrive.nuage.rest.container.ServletServer.State;
import com.oodrive.nuage.rest.jaxrs.JaxRsConfiguredApplication;

/**
 * Test for methods and uses of {@link ServletServer}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class TestServletServer {

    private static MetaConfiguration configuration;

    private static ContextTestHelper<JettyConfigurationContext> contextTestHelper;

    private static File contextFile;

    private static final List<State> startStates = Arrays.asList(new State[] { State.STARTED, State.STARTING });

    private static final List<State> stopStates = Arrays.asList(new State[] { State.STOPPED, State.STOPPING });

    @BeforeClass
    public static void setUpClass() throws InitializationError {
        contextTestHelper = new TestJettyConfigurationContext().getTestHelper();

        contextTestHelper.setUp();

        try {
            configuration = MetaConfiguration.newConfiguration(
                    ContextTestHelper.getPropertiesAsInputStream(contextTestHelper.getConfig()),
                    JettyConfigurationContext.getInstance());
        }
        catch (NullPointerException | IllegalArgumentException | IOException | ConfigValidationException e) {
            e.printStackTrace();
            throw new InitializationError(Arrays.asList(new Throwable[] { e }));
        }

        contextFile = new File("src/test/resources/testJaxRsContext.xml");
    }

    @AfterClass
    public static void tearDownClass() throws InitializationError {
        contextTestHelper.tearDown();
    }

    /**
     * Pre-instantiated server for tests.
     */
    private ServletServer testServer;

    private JaxRsConfiguredApplication jettyApp;

    @Before
    public final void setUp() throws InitializationError {
        try (FileInputStream contextInput = new FileInputStream(contextFile)) {
            jettyApp = new JaxRsConfiguredApplication(contextInput);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }
        testServer = new ServletServer(configuration, jettyApp);
        assertNotNull(testServer);
        assertFalse(testServer.isInitialized());
        assertFalse(testServer.isStarted());
    }

    @Test
    public final void testInitServer() throws IOException {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        // tests repeated initialization
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());
    }

    @Test
    public final void testInitServerWithoutResourceBase() throws IOException, NullPointerException,
            IllegalArgumentException, ConfigValidationException {

        final Properties customConfig = contextTestHelper.getConfig();
        customConfig.remove(contextTestHelper.getContext().getPropertyKey(RestResourceBaseConfigKey.getInstance()));

        final MetaConfiguration localConfiguration = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(customConfig), JettyConfigurationContext.getInstance());

        final ServletServer customServer = new ServletServer(localConfiguration, jettyApp);

        customServer.init();
        assertTrue(customServer.isInitialized());
        assertFalse(customServer.isStarted());

        customServer.fini();
        assertFalse(customServer.isInitialized());
        assertFalse(customServer.isStarted());
    }

    @Test
    public final void testFiniServer() throws IOException {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        testServer.fini();
        assertFalse(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        // tests repeated de-initialization
        testServer.fini();
        assertFalse(testServer.isInitialized());
        assertFalse(testServer.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public final void testFiniServerFailRunning() throws Exception {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        testServer.start();
        assertTrue(testServer.isStarted());

        try {
            testServer.fini();
        }
        finally {
            testServer.stop();
        }

    }

    @Test
    public final void testStartStopServer() throws Exception {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());
        assertEquals(State.STOPPED, testServer.getExecutionState());

        try {
            testServer.start();
            assertTrue(startStates.contains(testServer.getExecutionState()));
            assertTrue(testServer.isStarted());
        }
        finally {
            testServer.stop();
            assertTrue(stopStates.contains(testServer.getExecutionState()));
            assertFalse(testServer.isStarted());
        }

        // tests repeated call to stop()
        testServer.stop();
        assertTrue(stopStates.contains(testServer.getExecutionState()));
        assertFalse(testServer.isStarted());
    }

    @Test
    public final void testStopServerNoInit() throws Exception {
        assertFalse(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        testServer.stop();
        assertFalse(testServer.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public final void testStartServerFailInit() throws Exception {
        assertFalse(testServer.isInitialized());
        assertFalse(testServer.isStarted());

        testServer.start();
    }

    @Test(expected = BindException.class)
    public final void testStartServerFailBindException() throws Exception {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());
        assertEquals(State.STOPPED, testServer.getExecutionState());

        final ServletServer failServer = new ServletServer(configuration, jettyApp);

        try {
            testServer.start();
            assertTrue(startStates.contains(testServer.getExecutionState()));
            assertTrue(testServer.isStarted());

            // starts a second server with the same configuration, doomed to fail
            failServer.init();
            assertTrue(failServer.isInitialized());
            assertFalse(failServer.isStarted());
            failServer.start();
        }
        finally {
            assertFalse(failServer.isStarted());
            assertEquals(State.FAILED, failServer.getExecutionState());

            testServer.stop();
            assertTrue(stopStates.contains(testServer.getExecutionState()));
            assertFalse(testServer.isStarted());
        }
    }

    @Test
    public final void testGetExecutionStateBeforeInit() {
        assertFalse(testServer.isInitialized());
        assertEquals(State.NOT_INITIALIZED, testServer.getExecutionState());
    }

    // TODO: generalize and refactor tests to allow arbitrary numbers of threads
    @Test
    public final void testJoinTwoThreads() {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());
        assertEquals(State.STOPPED, testServer.getExecutionState());

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        final CountDownLatch startedLatch = new CountDownLatch(1);
        final CountDownLatch joinedLatch = new CountDownLatch(1);

        final Future<Boolean> startStopTask = executor.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                testServer.start();
                assertTrue(testServer.isStarted());
                startedLatch.countDown();

                joinedLatch.await(10, TimeUnit.SECONDS);
                Thread.sleep(2000);

                testServer.stop();
                assertFalse(testServer.isStarted());

                return true;
            }
        });

        final Future<Boolean> joinTask = executor.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                startedLatch.await(10, TimeUnit.SECONDS);
                assertTrue(testServer.isStarted());

                joinedLatch.countDown();
                testServer.join();
                assertFalse(testServer.isStarted());

                return true;
            }
        });

        try {
            assertTrue(startStopTask.get(20, TimeUnit.SECONDS));
            assertTrue(joinTask.get(20, TimeUnit.SECONDS));
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            // all exceptions end up here
            throw new AssertionError(e);
        }

        executor.shutdown();
        try {
            assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
        }
        catch (final InterruptedException e) {
            assertTrue(executor.shutdownNow().isEmpty());
        }
    }

    @Test
    public final void testJoinStopped() {
        testServer.init();
        assertTrue(testServer.isInitialized());
        assertFalse(testServer.isStarted());
        assertEquals(State.STOPPED, testServer.getExecutionState());

        final ExecutorService executor = Executors.newFixedThreadPool(1);
        final Future<Boolean> joinTask = executor.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                testServer.join();
                return true;
            }
        });

        try {
            assertTrue(joinTask.get(10, TimeUnit.SECONDS));
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            // all exceptions end up here
            throw new AssertionError(e);
        }
    }

    @Test(expected = IllegalStateException.class)
    public final void testJoinFailNotInitialized() throws InterruptedException {
        assertFalse(testServer.isInitialized());
        testServer.join();
    }

}
