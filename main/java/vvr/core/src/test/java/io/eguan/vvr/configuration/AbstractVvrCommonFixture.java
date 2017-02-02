package io.eguan.vvr.configuration;

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

import io.eguan.configuration.AbstractConfigurationContext;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidConfigurationContext.ContextTestHelper;
import io.eguan.nrs.TestValidNrsConfigurationContext;
import io.eguan.nrs.TestValidNrsConfigurationMountedContext;
import io.eguan.utils.mapper.TestValidFileMapperConfigurationContext;
import io.eguan.vvr.configuration.keys.BlockSizeConfigKey;
import io.eguan.vvr.persistence.configuration.TestValidPersistenceConfigurationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.model.InitializationError;

/**
 * class to accommodate any common fixture used in a significant subset of tests.
 * <p>
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public abstract class AbstractVvrCommonFixture {

    /**
     * Default total block count for devices.
     */
    protected static final int DEFAULT_TOTAL_BLOCK_COUNT = 1024;

    /**
     * Array of {@link ContextTestHelper} to include in the generation of the test {@link #configuration}.
     */
    private static final ContextTestHelper<?>[] CONFIG_CONTEXTS_HELPERS = new ContextTestHelper<?>[] {
            new TestValidCommonConfigurationContext().getTestHelper(),
            new TestValidFileMapperConfigurationContext().getTestHelper(),
            new TestValidIbsConfigurationContext().getTestHelper(),
            new TestValidNrsConfigurationContext().getTestHelper(),
            new TestValidPersistenceConfigurationContext().getTestHelper() };

    private static final ContextTestHelper<?>[] CONFIG_CONTEXTS_HELPERS_ERR = new ContextTestHelper<?>[] {
            new TestValidCommonConfigurationContext().getTestHelper(),
            new TestValidFileMapperConfigurationContext().getTestHelper(),
            new TestValidIbsConfigurationContext().getTestErrHelper(),
            new TestValidNrsConfigurationContext().getTestHelper(),
            new TestValidPersistenceConfigurationContext().getTestHelper() };

    private final ContextTestHelper<?>[] configContexts;

    protected AbstractVvrCommonFixture() {
        this(false);
    }

    protected AbstractVvrCommonFixture(final boolean helpersErr) {
        super();
        this.configContexts = helpersErr ? CONFIG_CONTEXTS_HELPERS_ERR : CONFIG_CONTEXTS_HELPERS;
    }

    protected AbstractVvrCommonFixture(final String helpersNrsFsType, final String helpersNrsMntOptions) {
        super();
        this.configContexts = new ContextTestHelper<?>[] { new TestValidCommonConfigurationContext().getTestHelper(),
                new TestValidFileMapperConfigurationContext().getTestHelper(),
                new TestValidIbsConfigurationContext().getTestHelper(),
                new TestValidNrsConfigurationMountedContext(helpersNrsFsType, helpersNrsMntOptions).getTestHelper(),
                new TestValidPersistenceConfigurationContext().getTestHelper() };
    }

    /**
     * The test configuration.
     * 
     * This instance is reinitialized for each test as temporary NRS and IBS directories need to be created and
     * destroyed properly.
     */
    private MetaConfiguration configuration;

    /**
     * Default block size provided by the test configuration.
     * 
     * @see #setUpConfiguration()
     */
    private int defaultBlockSize;

    @Before
    public final void setUpConfiguration() throws InitializationError {
        for (final ContextTestHelper<?> currHelper : configContexts) {
            currHelper.setUp();
        }

        final Properties props = new Properties();
        final ArrayList<AbstractConfigurationContext> contextList = new ArrayList<AbstractConfigurationContext>();

        for (final ContextTestHelper<?> currHelper : configContexts) {
            props.putAll(currHelper.getConfig());
            contextList.add(currHelper.getContext());
        }

        try {
            configuration = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(props),
                    contextList.toArray(new AbstractConfigurationContext[contextList.size()]));
        }
        catch (NullPointerException | IllegalArgumentException | IOException | ConfigValidationException e) {
            throw new InitializationError(e);
        }

        defaultBlockSize = BlockSizeConfigKey.getInstance().getTypedValue(configuration);
    }

    @After
    public final void tearDownConfiguration() throws InitializationError {
        for (final ContextTestHelper<?> currHelper : configContexts) {
            currHelper.tearDown();
        }
    }

    protected final MetaConfiguration getConfiguration() {
        return configuration;
    }

    protected final int getDefaultBlockSize() {
        return defaultBlockSize;
    }

}
