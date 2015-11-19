package io.eguan.dtx.config;

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

import io.eguan.configuration.FileConfigKey;

import java.io.File;

/**
 * Key holding the path relative to a resource manager's persistence location in which to store journal files.
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
 * <td>Path relative to a vold's persistent storage directory in which to store journal files.</td>
 * <td>FALSE</td>
 * <td>relative directory path</td>
 * <td>{@link String}</td>
 * <td>{@value #DEFAULT_DIR_PATH}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxJournalFileDirConfigKey extends FileConfigKey {

    /**
     * The locally unique property key suffix for this value.
     */
    protected static final String NAME = "journal.directory";

    private static final String DEFAULT_DIR_PATH = "journal";

    private static final File DEFAULT_VALUE = new File(DEFAULT_DIR_PATH);

    private static final DtxJournalFileDirConfigKey INSTANCE = new DtxJournalFileDirConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #JournalFileDirConfigKey()}
     */
    public static final DtxJournalFileDirConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as a {@link FileConfigKey} using the unique name {@value #NAME}.
     */
    private DtxJournalFileDirConfigKey() {
        super(NAME, false, false, false);
    }

    @Override
    public final File getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
