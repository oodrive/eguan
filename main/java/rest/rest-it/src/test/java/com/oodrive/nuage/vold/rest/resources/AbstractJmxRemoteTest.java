package com.oodrive.nuage.vold.rest.resources;

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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.vold.model.VoldTestHelper;
import com.oodrive.nuage.vold.model.VoldTestHelper.CompressionType;

/**
 * Abstract superclass of all classes testing the JMX client-based REST resource implementations.
 * 
 * At present this leverages the {@link JmxTestHelper} functions to identify the owner UUID and set up the client.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public abstract class AbstractJmxRemoteTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJmxRemoteTest.class);

    private static JmxTestHelper jmxTestHelper;

    private static VoldTestHelper voldTestHelper;

    private static String ownerUuid;

    @BeforeClass
    public static final void setUpJmxTestHelper() throws Exception {

        voldTestHelper = new VoldTestHelper(CompressionType.no, HashAlgorithm.MD5, Boolean.FALSE);
        voldTestHelper.createTemporary();
        voldTestHelper.start();

        Assert.assertTrue(voldTestHelper.waitMXBeanRegistration(voldTestHelper.newVvrManagerObjectName()));

        jmxTestHelper = new JmxTestHelper();
        ownerUuid = jmxTestHelper.resolveVvrOwnerUuid();
        jmxTestHelper.setUp();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Successful setup; ownerUuid=" + ownerUuid);
        }
    }

    @AfterClass
    public static final void tearDownJmxTestHelper() throws InitializationError, IOException {
        if (jmxTestHelper != null) {
            jmxTestHelper.tearDown();
        }

        try {
            voldTestHelper.stop();
        }
        finally {
            voldTestHelper.destroy();
        }
    }

    /**
     * Gets the owner UUID which is a mandatory query parameter for any request.
     * 
     * @return the ownerUuid
     */
    protected static final String getOwnerUuid() {
        return ownerUuid;
    }

    /**
     * Gets the configured {@link JmxTestHelper}.
     * 
     * @return the jmxTestHelper
     */
    protected static final JmxTestHelper getJmxTestHelper() {
        return jmxTestHelper;
    }

}
