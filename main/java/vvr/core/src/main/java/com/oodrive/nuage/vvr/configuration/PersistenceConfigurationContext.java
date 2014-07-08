package com.oodrive.nuage.vvr.configuration;

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

import com.oodrive.nuage.configuration.AbstractConfigurationContext;
import com.oodrive.nuage.vvr.configuration.keys.DeviceFileDirectoryConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.SnapshotFileDirectoryConfigKey;

/**
 * Context for configuration keys specific to VVR persistence.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class PersistenceConfigurationContext extends AbstractConfigurationContext {

    protected static final String NAME = "com.oodrive.nuage.vvr.persistence";

    private static final PersistenceConfigurationContext INSTANCE = new PersistenceConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #PersistenceConfigurationContext()}
     */
    public static final PersistenceConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an instance with the given {@link #NAME} and all keys references by this context.
     */
    private PersistenceConfigurationContext() {
        super(NAME, DeviceFileDirectoryConfigKey.getInstance(), SnapshotFileDirectoryConfigKey.getInstance());
    }

}
