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

import io.eguan.configuration.AbstractConfigurationContext;

/**
 * Configuration context for DTX-specific configuration parameters.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class DtxConfigurationContext extends AbstractConfigurationContext {

    private static final String NAME = "io.eguan.dtx";

    private static final DtxConfigurationContext INSTANCE = new DtxConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #DtxConfigurationContext()}
     */
    public static final DtxConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an {@link DtxConfigurationContext} instance with the given list of
     * {@link io.eguan.configuration.AbstractConfigKey}s.
     * 
     */
    protected DtxConfigurationContext() {
        super(NAME, DtxJournalFileDirConfigKey.getInstance(), DtxTransactionTimeoutConfigKey.getInstance(),
                DtxTaskKeeperAbsoluteDurationConfigKey.getInstance(), DtxTaskKeeperAbsoluteSizeConfigKey.getInstance(),
                DtxTaskKeeperMaxDurationConfigKey.getInstance(), DtxTaskKeeperMaxSizeConfigKey.getInstance(),
                DtxTaskKeeperPurgeDelayConfigKey.getInstance(), DtxTaskKeeperPurgePeriodConfigKey.getInstance());
    }

}
