package com.oodrive.nuage.nrs;

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

import com.oodrive.nuage.configuration.AbstractConfigurationContext;

/**
 * Configuration context for the NRS files.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class NrsConfigurationContext extends AbstractConfigurationContext {

    protected static final String NAME = "com.oodrive.nuage.nrs";

    private static final NrsConfigurationContext INSTANCE = new NrsConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #NrsConfigurationContext()}
     */
    public static final NrsConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an instance with the given {@link #NAME} and all keys references by this context.
     */
    private NrsConfigurationContext() {
        super(NAME, BlkCacheDirectoryConfigKey.getInstance(), ImagesFileDirectoryConfigKey.getInstance(),
                NrsClusterSizeConfigKey.getInstance(), NrsStorageConfigKey.getInstance(),
                RemainingSpaceCreateLimitConfigKey.getInstance());
    }

}
