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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.IntegerConfigKey.PositiveIntegerConfigKey;

/**
 * Key defining the size of a delay level, i.e. a number of leveldb instances in excess of the threshold triggering a
 * write delay, mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.
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
 * <td>Size of a delay level, i.e. a number of leveldb instances in excess of the threshold triggering a write delay.</td>
 * <td>FALSE</td>
 * <td>number of leveldb instances</td>
 * <td>int</td>
 * <td>{@value #DEFAULT_VALUE}</td>
 * <td>1</td>
 * <td>{@value #MAX_VALUE}</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class IbsBufferWriteDelayLevelSize extends PositiveIntegerConfigKey implements IbsConfigKey {

    private static final String NAME = "ldb.writedelaylevelsize";

    private static final String IBS_CONFIG_KEY = "buffer_write_delay_level_size";

    private static final int MAX_VALUE = Integer.MAX_VALUE;

    private static final int DEFAULT_VALUE = 5;

    private static final IbsBufferWriteDelayLevelSize INSTANCE = new IbsBufferWriteDelayLevelSize();

    public static IbsBufferWriteDelayLevelSize getInstance() {
        return INSTANCE;
    }

    private IbsBufferWriteDelayLevelSize() throws IllegalArgumentException {
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
