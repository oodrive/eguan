package com.oodrive.nuage.utils.mapper;

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

import java.io.File;

/**
 * Flat {@link FileMapper} implementation.
 * 
 * This implementation does not partition the given file names into sub-directories, but basically just concatenates the
 * uuid string to the base path.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class FlatFileMapper implements FileMapper {

    /** Parent directory for all the elements to map */
    private final File baseDir;

    /**
     * Create a new flat {@link FileMapper}.
     * 
     * @param baseDir
     *            Parent directory for all the elements to map
     */
    FlatFileMapper(final File baseDir) {
        super();
        this.baseDir = baseDir;
    }

    @Override
    public final File mapIdToFile(final CharSequence id) {
        return new File(baseDir, id.toString());
    }

}
