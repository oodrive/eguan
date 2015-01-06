package com.oodrive.nuage.rest.container;

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
 * Context for configuration keys for Jetty configuration.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class JettyConfigurationContext extends AbstractConfigurationContext {

    private static final String NAME = "com.oodrive.nuage.jetty";

    private static final JettyConfigurationContext INSTANCE = new JettyConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #JettyConfigurationContext()}
     */
    public static final JettyConfigurationContext getInstance() {
        return INSTANCE;
    }

    protected JettyConfigurationContext() throws IllegalArgumentException, NullPointerException {
        super(NAME, ServerAddressConfigKey.getInstance(), ServerPortConfigKey.getInstance(),
                JettyStopAtShutdownConfigKey.getInstance(), RestContextPathConfigKey.getInstance(),
                RestResourceBaseConfigKey.getInstance(), WebUiContextPathConfigKey.getInstance(), WebUiWarNameConfigKey
                        .getInstance());
    }

}
