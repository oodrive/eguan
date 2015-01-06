package com.oodrive.nuage.nrs;

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

import java.io.File;

import com.oodrive.nuage.configuration.FileConfigKey;

/**
 * Key holding the path to store the persistence of a repository.
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
 * <td>Path to store the persistence.</td>
 * <td>TRUE</td>
 * <td>directory path</td>
 * <td>{@link String}</td>
 * <td>N/A</td>
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
public final class NrsStorageConfigKey extends FileConfigKey {

    protected static final String NAME = "storage";

    private static final NrsStorageConfigKey INSTANCE = new NrsStorageConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #NrsStorageConfigKey()}
     */
    public static final NrsStorageConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as a {@link FileConfigKey} using the unique name {@value #NAME}.
     */
    private NrsStorageConfigKey() {
        super(NAME, true, true, true);
    }

    @Override
    public final File getDefaultValue() {
        return null;
    }

}
