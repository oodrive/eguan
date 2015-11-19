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

import io.eguan.configuration.EnumConfigKey;
import io.eguan.utils.mapper.FileMapper.Type;

/**
 * 
 * The {@link FileMapper} implementation to use as a {@link EnumConfigKey enum key} taking all constants of
 * {@link FileMapper.Type}.
 * 
 * 
 * The {@link FileMapper.Type#DEEP} implementation takes the values assigned to {@link DirPrefixLengthConfigKey} and
 * {@link DirStructureDepthConfigKey} into account.
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
 * <td>The {@link FileMapper} implementation to use.</td>
 * <td>TRUE</td>
 * <td>constants defined by {@link FileMapper.Type}</td>
 * <td>{@link String}</td>
 * <td>DEEP</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class FileMapperConfigKey extends EnumConfigKey<FileMapper.Type> {

    protected static final String NAME = "filemapper";

    private static final Type DEFAULT_VALUE = FileMapper.Type.DEEP;

    private static final Class<FileMapper.Type> ENUM_TYPE = FileMapper.Type.class;

    private static final FileMapperConfigKey INSTANCE = new FileMapperConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed with {@link #FileMapperConfigKey()}
     */
    public static final FileMapperConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as an {@link EnumConfigKey} with the {@link #NAME}, {@link #DEFAULT_VALUE} and
     * {@link #ENUM_TYPE}.
     */
    private FileMapperConfigKey() {
        super(NAME, ENUM_TYPE);
    }

    @Override
    protected final Type getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
