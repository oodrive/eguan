package io.eguan.vvr.configuration.keys;

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

import io.eguan.configuration.BooleanConfigKey;

/**
 * Key which indicates if the VVR is deleted. The VVR is not more in use and its purge is in progress.
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
 * <td>Indicates that the VVR is deleted.</td>
 * <td>FALSE</td>
 * <td>Boolean</td>
 * <td>False</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class DeletedConfigKey extends BooleanConfigKey {

    protected static final String NAME = "deleted";

    private static final Boolean DEFAULT_VALUE = Boolean.FALSE;

    private static final DeletedConfigKey INSTANCE = new DeletedConfigKey();

    protected DeletedConfigKey() {
        super(NAME);

    }

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #StartedConfigKey()}
     */
    public static final DeletedConfigKey getInstance() {
        return INSTANCE;
    }

    @Override
    protected Boolean getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
