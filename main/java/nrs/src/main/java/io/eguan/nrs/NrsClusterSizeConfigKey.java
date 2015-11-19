package io.eguan.nrs;

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

import io.eguan.configuration.IntegerConfigKey;

/**
 * 
 * The size of clusters NRS file data are internally aligned to in bytes.
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
 * <td>The size of clusters NRS file data are internally aligned to in bytes.</td>
 * <td>FALSE</td>
 * <td>bytes</td>
 * <td>int</td>
 * <td>4096</td>
 * <td>1024</td>
 * <td>32768</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class NrsClusterSizeConfigKey extends IntegerConfigKey {

    protected static final String NAME = "cluster.size";

    private static final int MAX_VALUE = 32768;

    private static final int MIN_VALUE = 1024;

    private static final int DEFAULT_VALUE = 4096;

    private static final NrsClusterSizeConfigKey INSTANCE = new NrsClusterSizeConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #NrsClusterSizeConfigKey()}
     */
    public static final NrsClusterSizeConfigKey getInstance() {
        return INSTANCE;
    }

    private NrsClusterSizeConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

}
