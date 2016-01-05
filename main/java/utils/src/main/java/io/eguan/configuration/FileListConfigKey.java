package io.eguan.configuration;

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

import static io.eguan.configuration.ValidationError.NO_ERROR;

import java.io.File;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Implementation of {@link MultiValuedConfigKey} to accept an {@link ArrayList} of {@link File}s.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public abstract class FileListConfigKey extends MultiValuedConfigKey<ArrayList<File>, File> {

    /**
     * Utility instance of {@link FileConfigKey} for strictly checking existence, directory and writable state with
     * {@link #performAdditionalValueChecks(ArrayList)}.
     */
    private final FileConfigKey valCheckFileKey;

    /**
     * Constructs an instance bound to contain a {@link ArrayList<File>}.
     * 
     * @param name
     *            the unique name for this key
     * @param separator
     *            the separator to expect/insert between values
     * @param checkDirectory
     *            whether to check if the target file is a directory
     * @param checkExistence
     *            whether to check for the target file's existence
     * @param checkWritable
     *            whether to check if the target file is writable by the current user
     * @see FileConfigKey#checkValue(Object)
     */
    @SuppressWarnings("unchecked")
    public FileListConfigKey(final String name, final String separator, final boolean checkDirectory,
            final boolean checkExistence, final boolean checkWritable) {

        /*
         * unchecked cast warning suppression intentional and limited to narrowing the class of
         * ArrayList<File>().getClass() to the exact match for the super constructor. TODO: remove as soon as Java
         * allows ArrayList<File>.class
         */
        super(name, separator, (Class<ArrayList<File>>) new ArrayList<File>().getClass(), File.class);

        valCheckFileKey = new FileConfigKey("internal.value.checking", checkDirectory, checkExistence, checkWritable) {

            @Override
            protected final Object getDefaultValue() {
                return null;
            }
        };
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
        if (value == null) {
            return "";
        }
        if (value instanceof ArrayList) {
            final String stringList = String.valueOf(value);
            return stringList.substring(1, stringList.length() - 1).replace(", ", getSeparator());
        }
        else {
            throw new IllegalArgumentException("Not an ArrayList");
        }
    }

    @Override
    protected final ArrayList<File> getCollectionFromValueList(final ArrayList<File> values) {
        return values;
    }

    @Override
    protected final File getItemValueFromString(final String value) {
        final String cleanValue = Objects.requireNonNull(value).trim();
        if (cleanValue.isEmpty()) {
            throw new IllegalArgumentException("Empty file path");
        }
        return new File(cleanValue);
    }

    @Override
    protected final ArrayList<File> makeDefensiveCopy(final ArrayList<File> value) {
        return value == null ? null : new ArrayList<File>(value);
    }

    @Override
    protected final ValidationError performAdditionalValueChecks(final ArrayList<File> value) {
        boolean checkPassed = true;
        final StringBuilder errorMessage = new StringBuilder();

        ValidationError report = NO_ERROR;
        for (final File currFile : value) {
            report = valCheckFileKey.checkValue(currFile);
            checkPassed &= (report == NO_ERROR);
            if (!checkPassed) {
                errorMessage.append("; " + report.getErrorMessage());
            }
        }
        return checkPassed ? NO_ERROR : new ValidationError(ValidationError.ErrorType.VALUE_INVALID, null, this, value,
                errorMessage.toString());
    }

}
