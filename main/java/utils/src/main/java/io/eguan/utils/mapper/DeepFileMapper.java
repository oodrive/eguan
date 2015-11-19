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

import java.io.File;

/**
 * {@link FileMapper} implementation for mapping logical filenames to a hierarchy of subdirectories similar to the one
 * <a href='http://www.git-scm.com'>Git</a> uses.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
final class DeepFileMapper implements FileMapper {
    /** Parent directory for all the elements to map */
    private final File baseDir;

    /** Minimal length of the id */
    private final int minlength;

    /** The directory prefix length. */
    private final int dirPrefixLength;

    /** The directory structure depth. */
    private final int dirTreeDepth;

    /**
     * Constructs an instance with the given parameters.
     */
    DeepFileMapper(final File baseDir, final int minlength, final int dirPrefixLength, final int dirTreeDepth) {
        super();
        this.baseDir = baseDir;
        this.minlength = minlength;

        // Need at least one char for the file name
        if ((dirPrefixLength * dirTreeDepth) > (minlength - 1)) {
            throw new IllegalArgumentException("dirPrefixLength=" + dirPrefixLength + ", dirTreeDepth=" + dirTreeDepth);
        }

        this.dirPrefixLength = dirPrefixLength;
        this.dirTreeDepth = dirTreeDepth;
    }

    @Override
    public final File mapIdToFile(final CharSequence id) {
        assert id.length() >= minlength;

        int start = 0;
        int end = dirPrefixLength;
        File result = baseDir;
        for (int i = 0; i < dirTreeDepth; i++) {
            // Add one directory level
            result = new File(result, id.subSequence(start, end).toString());
            // Next sequence
            start += dirPrefixLength;
            end += dirPrefixLength;
        }
        // Last sequence: filename
        return new File(result, id.subSequence(start, id.length()).toString());
    }
}
