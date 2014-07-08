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

import com.oodrive.nuage.configuration.IntegerConfigKey.PositiveIntegerConfigKey;
import com.oodrive.nuage.configuration.MetaConfiguration;

/**
 * Key defining the levelDB block size, mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.
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
 * <td>LevelDB block size, mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.</td>
 * <td>FALSE</td>
 * <td>number of records (0&nbsp;=&nbsp;unlimited)</td>
 * <td>int</td>
 * <td>0</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class IbsLdbBlockSizeConfigKey extends PositiveIntegerConfigKey implements IbsConfigKey {

    private static final String NAME = "ldb.blocksize";

    private static final String IBS_CONFIG_KEY = "ldb_block_size";

    private static final int MAX_VALUE = Integer.MAX_VALUE;

    private static final int DEFAULT_VALUE = 4096;

    private static final IbsLdbBlockSizeConfigKey INSTANCE = new IbsLdbBlockSizeConfigKey();

    public static IbsLdbBlockSizeConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsLdbBlockSizeConfigKey() throws IllegalArgumentException {
        super(NAME, MAX_VALUE, false);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

    @Override
    public final String getBackendConfigKey() {
        return IBS_CONFIG_KEY;
    }

    @Override
    public final String getBackendConfigValue(final MetaConfiguration configuration) {
        return valueToString(getTypedValue(configuration));
    }

}
