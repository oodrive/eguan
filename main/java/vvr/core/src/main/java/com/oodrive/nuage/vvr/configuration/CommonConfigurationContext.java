package com.oodrive.nuage.vvr.configuration;

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
import com.oodrive.nuage.vvr.configuration.keys.BlockSizeConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.DeletedConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.DescriptionConfigkey;
import com.oodrive.nuage.vvr.configuration.keys.HashAlgorithmConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.NameConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.NodeConfigKey;
import com.oodrive.nuage.vvr.configuration.keys.StartedConfigKey;

/**
 * Context for configuration keys common to all VVR modules.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class CommonConfigurationContext extends AbstractConfigurationContext {

    protected static final String NAME = "com.oodrive.nuage.vvr.common";

    private static final CommonConfigurationContext INSTANCE = new CommonConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #CommonConfigurationContext()}
     */
    public static final CommonConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an instance with the given {@link #NAME} and all keys references by this context.
     */
    private CommonConfigurationContext() {
        super(NAME, NameConfigKey.getInstance(), DescriptionConfigkey.getInstance(), BlockSizeConfigKey.getInstance(),
                HashAlgorithmConfigKey.getInstance(), NodeConfigKey.getInstance(), StartedConfigKey.getInstance(),
                DeletedConfigKey.getInstance());
    }

}
