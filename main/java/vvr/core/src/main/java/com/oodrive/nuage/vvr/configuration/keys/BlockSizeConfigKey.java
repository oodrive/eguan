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

import com.oodrive.nuage.configuration.IntegerConfigKey;

/**
 * Key holding the block size for the entire VVR in bytes as a {@link PositiveIntegerConfigKey positive integer}.
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
 * <td>The device block size in bytes.</td>
 * <td>TRUE</td>
 * <td>bytes</td>
 * <td>int</td>
 * <td>4096</td>
 * <td>1</td>
 * <td>{@link Integer#MAX_VALUE}</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class BlockSizeConfigKey extends IntegerConfigKey {

    protected static final String NAME = "device.block.size";

    private static final int MAX_VALUE = 64 * 1024;

    private static final int MIN_VALUE = 512;

    private static final int DEFAULT_VALUE = 4096;

    private static final BlockSizeConfigKey INSTANCE = new BlockSizeConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #BlockSizeConfigKey()}
     */
    public static final BlockSizeConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as a strict {@link PositiveIntegerConfigKey} with the {@link #NAME},
     * {@link #DEFAULT_VALUE} and {@link #MAX_VALUE}.
     */
    private BlockSizeConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    public final Integer getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
