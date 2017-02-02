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

import io.eguan.configuration.IntegerConfigKey;

/**
 * Key defining the absolute number of done tasks in the cache.
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
 * <td>Number of done tasks in the cache</td>
 * <td>FALSE</td>
 * <td>number of done tasks</td>
 * <td>int</td>
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
public final class DtxTaskKeeperAbsoluteSizeConfigKey extends IntegerConfigKey {

    private static final String NAME = "taskkeeper.absolutesize";

    private static final int MAX_VALUE = 5000;

    private static final int MIN_VALUE = 10;

    private static final int DEFAULT_VALUE = 1000;

    private static final DtxTaskKeeperAbsoluteSizeConfigKey INSTANCE = new DtxTaskKeeperAbsoluteSizeConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServerPortConfigKey()}
     */
    public static final DtxTaskKeeperAbsoluteSizeConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as a strict {@link DtxTaskKeeperAbsoluteSizeConfigKey} with the {@link #NAME},
     * {@link #DEFAULT_VALUE} and {@link #MAX_VALUE}.
     */
    private DtxTaskKeeperAbsoluteSizeConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

}
