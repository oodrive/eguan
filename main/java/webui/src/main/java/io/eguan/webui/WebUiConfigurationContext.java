package io.eguan.webui;

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

/**
 * Context for configuration keys for webui configuration.
 * 
 * @author
 * 
 * @author oodrive
 * @author ebredzinski
 */
public final class WebUiConfigurationContext extends AbstractConfigurationContext {

    private static final String NAME = "io.eguan.webui";

    private static final WebUiConfigurationContext INSTANCE = new WebUiConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #WebUiConfigurationContext()}
     */
    public static final WebUiConfigurationContext getInstance() {
        return INSTANCE;
    }

    protected WebUiConfigurationContext() throws IllegalArgumentException, NullPointerException {
        super(NAME, JmxServerUrlConfigKey.getInstance());
    }

}
