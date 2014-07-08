package com.oodrive.nuage.vold;

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

import com.oodrive.nuage.configuration.BooleanConfigKey;

/**
 * Key defining if the NBD server should be started by default.
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
 * <td>Defines if the NBD server should be started by default.</td>
 * <td>FALSE</td>
 * <td>either "true", "yes" or "false", "no" (case insensitive)</td>
 * <td>{@link Boolean}</td>
 * <td>true</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 */
public final class EnableNbdConfigKey extends BooleanConfigKey {

    private static final String NAME = "nbd.started";

    private static final EnableNbdConfigKey INSTANCE = new EnableNbdConfigKey();

    public static EnableNbdConfigKey getInstance() {
        return INSTANCE;
    }

    private EnableNbdConfigKey() {
        super(NAME);
    }

    @Override
    protected final Boolean getDefaultValue() {
        return Boolean.FALSE;
    }

}