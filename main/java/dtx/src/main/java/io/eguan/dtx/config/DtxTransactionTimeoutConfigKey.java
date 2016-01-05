package io.eguan.dtx.config;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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
 * Key defining the transaction timeout.
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
 * <td>DTX distributed transaction timeout.</td>
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
 * @author pwehrle
 * 
 */
public final class DtxTransactionTimeoutConfigKey extends LongConfigKey {

    private static final String NAME = "transaction.timeout";

    private static final long MAX_VALUE = 120000; // 2 minutes

    private static final long MIN_VALUE = 10000; // 10 seconds (hazelcast timeout)

    private static final long DEFAULT_VALUE = 20000; // 20 seconds

    private static final DtxTransactionTimeoutConfigKey INSTANCE = new DtxTransactionTimeoutConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServerPortConfigKey()}
     */
    public static final DtxTransactionTimeoutConfigKey getInstance() {
        return INSTANCE;
    }

    private DtxTransactionTimeoutConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Long getDefaultValue() {
        return Long.valueOf(DEFAULT_VALUE);
    }

}
