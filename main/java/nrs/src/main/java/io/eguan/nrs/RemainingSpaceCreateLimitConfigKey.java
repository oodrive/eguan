package io.eguan.nrs;

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
 * The percentage of storage capacity left on the storage volume used for persistence from which the storage provider
 * should refuse to create new objects.
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
 * <td>The percentage of storage capacity left on the storage volume used for persistence from which the storage
 * provider should refuse to create new objects.</td>
 * <td>FALSE</td>
 * <td>percentage</td>
 * <td>int</td>
 * <td>1</td>
 * <td>0</td>
 * <td>99</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class RemainingSpaceCreateLimitConfigKey extends IntegerConfigKey {

    protected static final String NAME = "remaining.space.create.limit";

    private static final int MAX_VALUE = 99;

    private static final int MIN_VALUE = 0;

    private static final int DEFAULT_VALUE = 1;

    private static final RemainingSpaceCreateLimitConfigKey INSTANCE = new RemainingSpaceCreateLimitConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #RemainingSpaceCreateLimitConfigKey()}
     */
    public static final RemainingSpaceCreateLimitConfigKey getInstance() {
        return INSTANCE;
    }

    private RemainingSpaceCreateLimitConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

}
