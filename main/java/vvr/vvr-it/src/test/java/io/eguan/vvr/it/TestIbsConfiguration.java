package io.eguan.vvr.it;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.ibs.Ibs;
import io.eguan.ibs.IbsFactory;
import io.eguan.vvr.configuration.AbstractVvrCommonFixture;
import io.eguan.vvr.configuration.IbsConfigurationContext;
import io.eguan.vvr.configuration.TestValidCommonConfigurationContext;
import io.eguan.vvr.configuration.keys.IbsHotDataConfigKey;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for methods of {@link IbsConfigurationContext}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestIbsConfiguration extends AbstractVvrCommonFixture {

    /**
     * The temporary config file to which to save IBS configuration.
     */
    private File tmpConfigFile;

    @Before
    public final void setUp() throws IOException {
        this.tmpConfigFile = File.createTempFile(TestValidCommonConfigurationContext.class.getSimpleName(), "conf");
    }

    @After
    public final void tearDown() throws IOException {
        Files.deleteIfExists(tmpConfigFile.toPath());
    }

    /**
     * Tests successful execution of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)}
     * with Ibs instance creation.
     */
    @Test
    public final void testStoreIbsConfigCreateIbs() throws RuntimeException, IOException, ConfigValidationException {

        final IbsConfigurationContext targetContext = IbsConfigurationContext.getInstance();

        final MetaConfiguration configuration = getConfiguration();

        try (FileOutputStream output = new FileOutputStream(tmpConfigFile)) {
            targetContext.storeIbsConfig(configuration, output);
        }

        // IBS create
        {
            final Ibs result = IbsFactory.createIbs(tmpConfigFile);
            assertNotNull(result);
            assertEquals(IbsHotDataConfigKey.getInstance().getTypedValue(configuration), result.isHotDataEnabled());

            result.start();
            assertTrue(result.isStarted());

            result.close();
            assertTrue(result.isClosed());
        }
        // IBS open
        {
            final Ibs result = IbsFactory.openIbs(tmpConfigFile);
            assertNotNull(result);
            assertEquals(IbsHotDataConfigKey.getInstance().getTypedValue(configuration), result.isHotDataEnabled());

            result.start();
            assertTrue(result.isStarted());

            result.close();
            assertTrue(result.isClosed());
        }
    }
}
