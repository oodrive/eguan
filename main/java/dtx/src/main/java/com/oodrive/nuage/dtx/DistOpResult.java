package com.oodrive.nuage.dtx;

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

import java.io.Serializable;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;

/**
 * Result container for distributed operations.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
@Immutable
final class DistOpResult implements Serializable {

    private static final long serialVersionUID = -868371510052389373L;

    static final DistOpResult NO_ERROR = new DistOpResult(0);

    private final int exitStatus;

    private final String message;

    /**
     * Constructs a simple instance from a given exit status.
     * 
     * @param exitStatus
     *            0 to signify no error, non-zero for an unspecified error
     */
    DistOpResult(final int exitStatus) {
        this(exitStatus, "");
    }

    /**
     * Constructs an instance from a given exit status and message.
     * 
     * @param exitStatus
     *            0 to signify no error, non-zero for an error return
     * @param message
     *            the non-<code>null</code> message describing the exit status, empty {@link String}s are replaced by
     *            generic 'no error' or 'unspecified error' messages
     */
    DistOpResult(final int exitStatus, @Nonnull final String message) {
        this.exitStatus = exitStatus;
        this.message = message.isEmpty() ? (exitStatus == 0 ? "No error" : "Unspecified error") : message;
    }

    /**
     * Constructs an instance representing an error return.
     * 
     * @param exitStatus
     *            a non-zero exit status
     * @param error
     *            the non-<code>null</code> {@link Throwable} instance causing the return of an error
     */
    DistOpResult(final int exitStatus, @Nonnull final Throwable error) {
        this(exitStatus, error.getClass().getName() + (error.getMessage() == null ? "" : ": " + error.getMessage()));
    }

    /**
     * Gets the exit status of the operation.
     * 
     * @return 0 if the operation succeeded without error and non-zero values for different error conditions
     */
    final int getExitStatus() {
        return exitStatus;
    }

    /**
     * Gets the return message matching the exit status.
     * 
     * @return a non-empty message explaining the result status
     */
    final String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(DistOpResult.class).add("exitStatus", this.exitStatus)
                .add("message", this.message).toString();
    }
}
