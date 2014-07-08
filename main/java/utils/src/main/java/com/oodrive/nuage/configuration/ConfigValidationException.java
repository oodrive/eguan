package com.oodrive.nuage.configuration;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.concurrent.Immutable;

import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Exception class with detailed error report to pass to the caller only if the configuration is invalid.
 * 
 * Though this is technically not an exception to recover from (Item 58, Effective Java, 2nd edition), leaving this a
 * checked exception allows calling code to print the detailed error report before shutting down.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
@Immutable
public final class ConfigValidationException extends Exception {

    private static final long serialVersionUID = -8666629170221217567L;

    /**
     * The message included as {@link Exception#getMessage() exception message}.
     */
    private static final String MESSAGE = "Configuration validation failed";

    /**
     * The list of {@link ValidationError}s representing the error trace.
     */
    private final List<ValidationError> errors;

    private final List<ErrorType> errorTypes;

    /**
     * Constructs a {@link ConfigValidationException} from a report containing {@link ConfigKey} and associated error
     * messages.
     * 
     * @param errorReport
     *            a non-empty {@link List} of {@link ValidationError} with matching error messages
     * @throws IllegalArgumentException
     *             if the provided error report is null or empty
     * @throws NullPointerException
     *             if the error report is {@code null}
     */
    protected ConfigValidationException(final List<ValidationError> errorReport) throws IllegalArgumentException,
            NullPointerException {
        super(MESSAGE);
        if (Objects.requireNonNull(errorReport).isEmpty()) {
            throw new IllegalArgumentException("Error report is empty");
        }
        errors = Collections.unmodifiableList(errorReport);
        // construct the error type list
        final ArrayList<ErrorType> errTypes = new ArrayList<ErrorType>();
        for (final ValidationError currError : errors) {
            errTypes.add(currError.getType());
        }
        errorTypes = Collections.unmodifiableList(errTypes);
    }

    /**
     * Gets the error report.
     * 
     * @return a {@link List} of {@link ValidationError} objects containing the {@link ConfigurationContext context},
     *         {@link ConfigKey key}, value and error message relevant to each error
     */
    public final List<ValidationError> getValidationReport() {
        return errors;
    }

    /**
     * Gets a list of all error types present in the {@link #getValidationReport()}.
     * 
     * @return a (possibly empty) list of {@link ErrorType} objects, each appearing the number of times it is present in
     *         the complete report
     */
    public final List<ErrorType> getErrorTypeList() {
        return errorTypes;
    }

    /**
     * Gets the errors of a certain {@link ErrorType} from the complete {@link #getValidationReport() report}.
     * 
     * @param type
     *            the {@link ErrorType type} to search for
     * @return a (possibly empty) list of {@link ValidationError}s
     */
    public final List<ValidationError> getErrorsByType(final ErrorType type) {
        final ArrayList<ValidationError> result = new ArrayList<ValidationError>();
        for (final ValidationError currError : errors) {
            if (currError.getType() == type) {
                result.add(currError);
            }
        }
        return result;
    }

}