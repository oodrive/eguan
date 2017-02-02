package io.eguan.utils.mapper;

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
 * The directory structure depth for the {@link FileMapper.Type#DEEP DEEP} {@link FileMapper}.
 * 
 * Values associated to this key denote the number of directory levels to insert between the base directory and the file
 * itself, helping spread files over the structure so as not to hit file system limits too early. The value set for this
 * key multiplied by the value set for {@link DirPrefixLengthConfigKey} must not exceed 31 (i.e. the length of a
 * hex-encoded UUID - 1).
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
 * <td>The directory structure depth for the {@link FileMapper.Type#DEEP DEEP} {@link FileMapper}</td>
 * <td>FALSE</td>
 * <td>directory levels</td>
 * <td>short</td>
 * <td>1</td>
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
public final class DirStructureDepthConfigKey extends IntegerConfigKey {

    protected static final String NAME = "dir.structure.depth";

    private static final int MAX_VALUE = 31;

    private static final int MIN_VALUE = 1;

    private static final int DEFAULT_VALUE = 1;

    private static final DirStructureDepthConfigKey INSTANCE = new DirStructureDepthConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #DirStructureDepthConfigKey()}
     */
    public static final DirStructureDepthConfigKey getInstance() {
        return INSTANCE;
    }

    private DirStructureDepthConfigKey() {
        super(NAME, MIN_VALUE, MAX_VALUE);
    }

    @Override
    protected final Integer getDefaultValue() {
        return Integer.valueOf(DEFAULT_VALUE);
    }

}
