package com.oodrive.nuage.dtx.config;

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

import java.util.Calendar;

import com.oodrive.nuage.configuration.LongConfigKey;

/**
 * Key defining the delay before started the tasks purge.
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
 * <td>Time before started the task purge</td>
 * <td>FALSE</td>
 * <td>ms</td>
 * <td>long</td>
 * <td>midnight the same day {@see TaskKeeperConstants}</td>
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
public final class DtxTaskKeeperPurgeDelayConfigKey extends LongConfigKey {

    private static final String NAME = "taskkeeper.purgedelay";

    private static final long MAX_VALUE = 86400000L; // 24h

    private static final long MIN_VALUE = 0;

    private static final long DEFAULT_VALUE;
    static {
        final Calendar c = Calendar.getInstance();
        final long now = c.getTimeInMillis();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        // the next day
        c.add(Calendar.DAY_OF_MONTH, 1);
        DEFAULT_VALUE = c.getTimeInMillis() - now;
    }

    private static final DtxTaskKeeperPurgeDelayConfigKey INSTANCE = new DtxTaskKeeperPurgeDelayConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServerPortConfigKey()}
     */
    public static final DtxTaskKeeperPurgeDelayConfigKey getInstance() {
        return INSTANCE;
    }

    private DtxTaskKeeperPurgeDelayConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Long getDefaultValue() {
        return Long.valueOf(DEFAULT_VALUE);
    }

}
