package com.oodrive.nuage.vold.adm;

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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

/**
 * Test for {@link RestLauncher}'s methods.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestRestLauncher {

    /**
     * Pre-instantiated launcher for tests.
     */
    private RestLauncher testLauncher;

    @Before
    public final void setUp() throws InitializationError {

        testLauncher = new RestLauncher();
        assertNotNull(testLauncher);
        assertFalse(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());
    }

    @Test
    public final void testInitLauncher() throws IOException {

        initTestLauncher();

        // tests repeated initialization
        testLauncher.init();
        assertTrue(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());
    }

    @Test
    public final void testFiniLauncher() throws IOException {

        initTestLauncher();

        testLauncher.fini();
        assertFalse(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());

        // tests repeated de-initialization
        testLauncher.fini();
        assertFalse(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public final void testFiniLauncherFailStarted() throws Exception {

        initTestLauncher();

        testLauncher.start();
        assertTrue(testLauncher.isStarted());

        try {
            testLauncher.fini();
        }
        finally {
            try {
                testLauncher.stop();
            }
            catch (final IllegalStateException ie) {
                // re-throws the exception as it could be mistaken for the expected one
                throw new AssertionError(ie);
            }
        }
    }

    @Test
    public final void testStartStopLauncher() throws Exception {

        initTestLauncher();

        try {
            testLauncher.start();
            assertTrue(testLauncher.isStarted());
        }
        finally {
            testLauncher.stop();
            assertFalse(testLauncher.isStarted());
        }

        // tests repeated call to stop()
        testLauncher.stop();
        assertFalse(testLauncher.isStarted());
    }

    @Test
    public final void testStopLauncherNoInit() throws Exception {
        assertFalse(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());

        testLauncher.stop();
        assertFalse(testLauncher.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public final void testStartLauncherFailInit() throws Exception {
        assertFalse(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());

        testLauncher.start();
    }

    @Test(expected = IllegalStateException.class)
    public final void testStartLauncherFailBindToSameAddressAndPort() throws Exception {

        initTestLauncher();

        final RestLauncher failServer = new RestLauncher();

        try {
            testLauncher.start();
            assertTrue(testLauncher.isStarted());

            // starts a second server with the same configuration, doomed to fail
            failServer.init();
            assertTrue(failServer.isInitialized());
            assertFalse(failServer.isStarted());
            failServer.start();
        }
        finally {
            assertFalse(failServer.isStarted());

            testLauncher.stop();
            assertFalse(testLauncher.isStarted());
        }
    }

    /**
     * Initializes the {@link #testLauncher}.
     */
    private final void initTestLauncher() {
        testLauncher.init();
        assertTrue(testLauncher.isInitialized());
        assertFalse(testLauncher.isStarted());
    }

    // TODO: add multi-threaded and join()tests

}
