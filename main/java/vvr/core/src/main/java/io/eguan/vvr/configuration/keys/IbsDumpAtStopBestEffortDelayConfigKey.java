package io.eguan.vvr.configuration.keys;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.IntegerConfigKey.PositiveIntegerConfigKey;

/**
 * Key defining the delay of best effort when stopping and Ibs, i.e. the time spend to dump IbpGen database at stop
 * mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.
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
 * <td>The delay in seconds, i.e. the time spend to dump IbpGen database at stop.</td>
 * <td>FALSE</td>
 * <td>seconds</td>
 * <td>int</td>
 * <td>{@value #DEFAULT_VALUE}</td>
 * <td>1</td>
 * <td>{@value #MAX_VALUE}</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author jmcaba
 * 
 */
public final class IbsDumpAtStopBestEffortDelayConfigKey extends PositiveIntegerConfigKey implements IbsConfigKey {

    private static final String NAME = "dumpAtStopBestEffortDelay";

    private static final String IBS_CONFIG_KEY = "dump_at_stop_best_effort_delay";

    private static final int MAX_VALUE = Integer.MAX_VALUE;

    private static final int DEFAULT_VALUE = 5;

    private static final IbsDumpAtStopBestEffortDelayConfigKey INSTANCE = new IbsDumpAtStopBestEffortDelayConfigKey();

    public static IbsDumpAtStopBestEffortDelayConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsDumpAtStopBestEffortDelayConfigKey() throws IllegalArgumentException {
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
