package io.eguan.dtx.config;

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

import io.eguan.configuration.LongConfigKey;

/**
 * Key defining the maximum duration for a done task.
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
 * <td>Value of the maximum duration to keep a task in the cache. Only maximumSize tasks with maximum duration are kept
 * in the cache</td>
 * <td>FALSE</td>
 * <td>ms</td>
 * <td>long</td>
 * <td>{@value #DEFAULT_VALUE}</td>
 * <td>{@value #MIN_VALUE}</td>
 * <td>{@value #MAX_VALUE}</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author ebredzinski
 * @author pwehrle
 * 
 */
public final class DtxTaskKeeperMaxDurationConfigKey extends LongConfigKey {

    private static final String NAME = "taskkeeper.maxduration";

    private static final long MAX_VALUE = 2629743830L; // 1 month

    private static final long MIN_VALUE = 5000; // 5 seconds

    private static final long DEFAULT_VALUE = 1209600000L; // 2 weeks

    private static final DtxTaskKeeperMaxDurationConfigKey INSTANCE = new DtxTaskKeeperMaxDurationConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServerPortConfigKey()}
     */
    public static final DtxTaskKeeperMaxDurationConfigKey getInstance() {
        return INSTANCE;
    }

    private DtxTaskKeeperMaxDurationConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Long getDefaultValue() {
        return Long.valueOf(DEFAULT_VALUE);
    }

}
