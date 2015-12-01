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

import static io.eguan.configuration.ValidationError.NO_ERROR;
import io.eguan.configuration.ValidationError.ErrorType;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;

/**
 * {@link ConfigKey} implementation capable of handling values representing (paths to) {@link File}s.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
public abstract class FileConfigKey extends AbstractConfigKey {

    /**
     * Whether to check if the path points to a directory.
     */
    private final boolean checkDirectory;

    /**
     * Whether to enforce existence upon checking a value.
     */
    private final boolean checkExistence;

    /**
     * Whether to enforce a value {@link File} to be writable by the current user.
     */
    private final boolean checkWritable;

    /**
     * Internal constructor forwarding construction to {@link AbstractConfigKey#AbstractConfigKey(String)} with
     * additional parameters to add optional {@link #checkValue(Object) checks} for directory, existence or writable
     * state.
     * 
     * @param name
     *            the unique name for this key
     * @param checkDirectory
     *            if <code>true</code>, {@link #checkValue(Object)} will generate an error if the passed {@link File} is
     *            not a directory
     * @param checkExistence
     *            if <code>true</code>, {@link #checkValue(Object)} will generate an error if the passed {@link File}
     *            does not exist
     * @param checkWritable
     *            if <code>true</code>, {@link #checkValue(Object)} will generate an error if the passed {@link File} is
     *            not writable by the current user
     */
    protected FileConfigKey(final String name, final boolean checkDirectory, final boolean checkExistence,
            final boolean checkWritable) {
        super(name);
        this.checkDirectory = checkDirectory;
        this.checkExistence = checkExistence;
        this.checkWritable = checkWritable;
    }

    @Override
    public final File getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return (File) configuration.getValue(this);
    }

    @Override
    protected final File parseValue(final String value) throws IllegalArgumentException, NullPointerException {
        if (value.isEmpty()) {
            // Returns the default value or null when there is no default value
            final File defaultValue = (File) getDefaultValue();
            return defaultValue;
        }
        return new File(value);
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException {
        if (value == null) {
            return "";
        }
        if (value instanceof File) {
            return ((File) value).toString();
        }
        else {
            throw new IllegalArgumentException("Not a File");
        }

    }

    @Override
    public final ValidationError checkValue(final Object value) {

        ValidationError report = checkForNullAndRequired(value);

        if (report != NO_ERROR) {
            return (report.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : report;
        }

        report = checkSameClass(value, File.class);

        if (report != NO_ERROR) {
            // return the class error ignoring additional checks
            return report;
        }

        boolean checkPassed = true;
        final File fileValue = (File) value;
        final StringBuilder errorMessage = new StringBuilder("file " + fileValue);

        if (checkExistence && !fileValue.exists()) {
            errorMessage.append(" does not exist");
            checkPassed = false;
        }

        if (checkDirectory && !fileValue.isDirectory()) {
            errorMessage.append(" is not a directory");
            checkPassed = false;
        }

        if (checkWritable && !Files.isWritable(fileValue.toPath())) {
            errorMessage.append(" is not writable by the current user");
            checkPassed = false;
        }

        return checkPassed ? NO_ERROR : new ValidationError(ErrorType.VALUE_INVALID, null, this, value,
                errorMessage.toString());

    }

}
