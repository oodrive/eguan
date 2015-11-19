package io.eguan.utils.mapper;

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
 * The length of the prefix the {@link FileMapper.Type#DEEP DEEP} {@link FileMapper} uses to create sub-directories.
 * 
 * The value set for this key multiplied by the value set for {@link DirStructureDepthConfigKey} must not exceed 31
 * (i.e. the length of a hex-encoded UUID - 1).
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
 * <td>The length of the prefix the {@link FileMapper.Type#DEEP DEEP} {@link FileMapper} uses to create sub-directories.
 * </td>
 * <td>FALSE</td>
 * <td>characters of filenames</td>
 * <td>short</td>
 * <td>2</td>
 * <td>1</td>
 * <td>31</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class DirPrefixLengthConfigKey extends IntegerConfigKey {

    protected static final String NAME = "dir.prefix.length";

    private static final int MAX_VALUE = 31;

    private static final int MIN_VALUE = 1;

    private static final int DEFAULT_VALUE = 2;

    private static final DirPrefixLengthConfigKey INSTANCE = new DirPrefixLengthConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #DirPrefixLengthConfigKey()}
     */
    public static final DirPrefixLengthConfigKey getInstance() {
        return INSTANCE;
    }

    private DirPrefixLengthConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

}
