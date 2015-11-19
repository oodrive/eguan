package io.eguan.configuration;

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

import io.eguan.configuration.FileListConfigKey;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Test extension of the abstract {@link FileListConfigKey} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class FileListTestConfigKey extends FileListConfigKey {

    private static final String NAME = "filelist.test.key";

    private static final String SEPARATOR = ";";

    public FileListTestConfigKey() {
        super(NAME, SEPARATOR, false, false, false);
    }

    @Override
    protected final Object getDefaultValue() {
        return new ArrayList<File>(Arrays.asList(new File[] { new File("/usr"), new File("/lib") }));
    }

}
