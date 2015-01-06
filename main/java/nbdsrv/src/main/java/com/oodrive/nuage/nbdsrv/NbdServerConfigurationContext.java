package com.oodrive.nuage.nbdsrv;

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
 * Configuration context for the NBD server.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public final class NbdServerConfigurationContext extends AbstractConfigurationContext {

    protected static final String NAME = "com.oodrive.nuage.nbdsrv";

    private static final NbdServerConfigurationContext INSTANCE = new NbdServerConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #NbdServerConfigurationContext()}
     */
    public static final NbdServerConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an instance with the given {@link #NAME} and all keys references by this context.
     */
    private NbdServerConfigurationContext() {
        super(NAME, NbdServerPortConfigKey.getInstance(), NbdServerInetAddressConfigKey.getInstance(),
                NbdServerTrimConfigKey.getInstance());
    }
}
