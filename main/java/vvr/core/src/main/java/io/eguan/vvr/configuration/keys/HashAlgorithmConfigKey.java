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

import io.eguan.configuration.EnumConfigKey;
import io.eguan.hash.HashAlgorithm;

/**
 * Key holding the default hash algorithm used by the VVR as a {@link EnumConfigKey enum key} taking all constants of
 * {@link HashAlgorithm}.
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
 * <td>The default hash algorithm used by the VVR for computing data block hash keys.</td>
 * <td>FALSE</td>
 * <td>constants defined by {@link HashAlgorithm}</td>
 * <td>{@link String}</td>
 * <td>MD5</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class HashAlgorithmConfigKey extends EnumConfigKey<HashAlgorithm> {

    protected static final String NAME = "hash.algorithm";

    private static final HashAlgorithm DEFAULT_VALUE = HashAlgorithm.MD5;

    private static final Class<HashAlgorithm> ENUM_TYPE = HashAlgorithm.class;

    private static final HashAlgorithmConfigKey INSTANCE = new HashAlgorithmConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #HashAlgorithmConfigKey()}
     */
    public static final HashAlgorithmConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as an {@link EnumConfigKey} with the {@link #NAME}, {@link #DEFAULT_VALUE} and
     * {@link #ENUM_TYPE}.
     */
    private HashAlgorithmConfigKey() {
        super(NAME, ENUM_TYPE);
    }

    @Override
    protected final HashAlgorithm getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
