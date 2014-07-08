package com.oodrive.nuage.rest.container;

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

import com.oodrive.nuage.configuration.StringConfigKey;

/**
 * Key defining the root to which to bind the single web application context.
 * 
 * <table border='1'>
 * <tr>
 * <th>NAME</th>
 * <th>DESCRIPTION</th>
 * <th>REQUIRED</th>
 * <th>UNIT</th>
 * <th>TYPE</th>
 * <th>DEFAULT</th>
 * <th>MIN</th>
 * <th>MAX</th>
 * </tr>
 * <tr>
 * <td>{@value #NAME}</td>
 * <td>The path of the web application war.</td>
 * <td>FALSE</td>
 * <td>war path</td>
 * <td>String</td>
 * <td></td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class WebUiWarNameConfigKey extends StringConfigKey {

    protected static final String NAME = "webui.warname";

    private static final String DEFAULT_VALUE = "";

    private static final WebUiWarNameConfigKey INSTANCE = new WebUiWarNameConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServletRootPathConfigKey()}
     */
    public static final WebUiWarNameConfigKey getInstance() {
        return INSTANCE;
    }

    private WebUiWarNameConfigKey() {
        super(NAME);
    }

    @Override
    protected final String getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
