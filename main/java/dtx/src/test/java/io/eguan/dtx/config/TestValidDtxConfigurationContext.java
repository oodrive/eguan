package io.eguan.dtx.config;

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

import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.dtx.config.DtxConfigurationContext;
import io.eguan.dtx.config.DtxJournalFileDirConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperAbsoluteDurationConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperAbsoluteSizeConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperMaxDurationConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperMaxSizeConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperPurgeDelayConfigKey;
import io.eguan.dtx.config.DtxTaskKeeperPurgePeriodConfigKey;
import io.eguan.dtx.config.DtxTransactionTimeoutConfigKey;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;

/**
 * {@link ValidConfigurationContext} implementation with {@link DtxConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestValidDtxConfigurationContext extends ValidConfigurationContext {

    private static final Long TASK_KEEPER_ABSOLUTE_DURATION_VALUE = Long.valueOf(2592000000L);

    private static final Integer TASK_KEEPER_ABSOLUTE_SIZE_VALUE = Integer.valueOf(1100);

    private static final Long TASK_KEEPER_MAX_DURATION_VALUE = Long.valueOf(1209500000L);

    private static final Integer TASK_KEEPER_MAX_SIZE_VALUE = Integer.valueOf(510);

    private static final Long TASK_KEEPER_PURGE_DELAY_VALUE = Long.valueOf(36472850L);

    private static final Long TASK_KEEPER_PURGE_PERIOD_VALUE = Long.valueOf(43200000L);

    private static final Long DTX_TX_TIMEOUT_VALUE = Long.valueOf(20500L);

    private static final ContextTestHelper<DtxConfigurationContext> TEST_HELPER;
    static {
        TEST_HELPER = new ContextTestHelper<DtxConfigurationContext>(DtxConfigurationContext.getInstance()) {

            @Override
            public final void setUp() throws InitializationError {
                // nothing
            }

            @Override
            public final void tearDown() throws InitializationError {
                // nothing
            }

            @Override
            public final Properties getConfig() {
                final Properties result = new Properties();
                result.setProperty(TEST_HELPER.getPropertyKey(DtxJournalFileDirConfigKey.getInstance()), "journals");
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperAbsoluteDurationConfigKey.getInstance()),
                        TASK_KEEPER_ABSOLUTE_DURATION_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperAbsoluteSizeConfigKey.getInstance()),
                        TASK_KEEPER_ABSOLUTE_SIZE_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperMaxDurationConfigKey.getInstance()),
                        TASK_KEEPER_MAX_DURATION_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperMaxSizeConfigKey.getInstance()),
                        TASK_KEEPER_MAX_SIZE_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperPurgeDelayConfigKey.getInstance()),
                        TASK_KEEPER_PURGE_DELAY_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTaskKeeperPurgePeriodConfigKey.getInstance()),
                        TASK_KEEPER_PURGE_PERIOD_VALUE.toString());
                result.setProperty(TEST_HELPER.getPropertyKey(DtxTransactionTimeoutConfigKey.getInstance()),
                        DTX_TX_TIMEOUT_VALUE.toString());
                return result;
            }
        };
    }

    /**
     * Sets up class fixture.
     * 
     * @throws InitializationError
     *             if initialization fails
     */
    @BeforeClass
    public static final void setUpClass() throws InitializationError {
        TEST_HELPER.setUp();
    }

    /**
     * Tears down class fixture.
     * 
     * @throws InitializationError
     *             if shutdown fails even partially
     */
    @AfterClass
    public static final void tearDownClass() throws InitializationError {
        TEST_HELPER.tearDown();
    }

    @Override
    public final ContextTestHelper<DtxConfigurationContext> getTestHelper() {
        return TEST_HELPER;
    }

}
