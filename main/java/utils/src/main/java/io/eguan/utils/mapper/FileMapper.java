package io.eguan.utils.mapper;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.configuration.MetaConfiguration;

import java.io.File;

import javax.annotation.Nonnull;

/**
 * Interface for classes mapping file names to paths in an organized directory structure.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public interface FileMapper {

    /**
     * Type identifiers for knows implementations of the {@link FileMapper} interface.
     */
    public static enum Type {

        /**
         * Type identifier for the Git-like {@link FileMapper} implementation.
         */
        DEEP() {

            @Override
            public final FileMapper newInstance(final File baseDir, final int minlength,
                    final MetaConfiguration configuration) {
                final int dirPrefixLength = DirPrefixLengthConfigKey.getInstance().getTypedValue(configuration)
                        .intValue();
                assert dirPrefixLength > 0;

                final int dirTreeDepth = DirStructureDepthConfigKey.getInstance().getTypedValue(configuration)
                        .intValue();
                assert dirTreeDepth > 0;

                return new DeepFileMapper(baseDir, minlength, dirPrefixLength, dirTreeDepth);
            }

        },

        /**
         * Type identifier for the FLAT {@link FileMapper}, which does nothing really.
         */
        FLAT() {

            @Override
            public final FileMapper newInstance(final File baseDir, final int minlength,
                    final MetaConfiguration configuration) {
                return new FlatFileMapper(baseDir);
            }

        };

        /**
         * Provides a new instance of the {@link FileMapper} implementation associated to this literal.
         * 
         * @param baseDir
         *            base directory containing the files
         * @param minlength
         *            minimum length of a file id
         * @return a functional {@link FileMapper} instance
         */
        public abstract FileMapper newInstance(final File baseDir, final int minlength, MetaConfiguration configuration);
    }

    /**
     * Maps a given base path and a file name to an path.
     * 
     * @param id
     *            id to map
     * @return an absolute path pointing to the transformed file, without any check for existence or access rights
     * @throws NullPointerException
     *             if id is null
     */
    File mapIdToFile(@Nonnull final CharSequence id) throws NullPointerException;

}
