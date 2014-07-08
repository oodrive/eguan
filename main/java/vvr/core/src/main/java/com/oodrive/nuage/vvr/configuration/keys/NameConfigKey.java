package com.oodrive.nuage.vvr.configuration.keys;

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
 * 
 * Key holding the name of the VVR.
 * 
 * <table border='1'>
 * <tr>
 * <th>NAME</th>
 * <th>DESCRIPTION</th>
 * <th>REQUIRED</th>
 * <th>TYPE</th>
 * <th>DEFAULT</th>
 * </tr>
 * <tr>
 * <td>{@value #NAME}</td>
 * <td>The name of the VVR.</td>
 * <td>FALSE</td>
 * <td>String</td>
 * <td>""</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * 
 */
public class NameConfigKey extends StringConfigKey {

    protected static final String NAME = "name";

    private static final String DEFAULT_VALUE = "";

    private static final NameConfigKey INSTANCE = new NameConfigKey();

    protected NameConfigKey() {
        super(NAME);

    }

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #NameConfigKey()}
     */
    public static final NameConfigKey getInstance() {
        return INSTANCE;
    }

    @Override
    protected String getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
