package com.oodrive.nuage.nrs;

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

import java.io.IOException;

/**
 * Exception on Nrs files.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public class NrsException extends IOException {

    /**
     * the class serial version UID.
     * <p>
     */
    private static final long serialVersionUID = -4360857070195404880L;

    /**
     * Constructs an {@code NrsException} with {@code null} as its error detail message.
     */
    public NrsException() {
        super();
    }

    /**
     * Constructs an {@code NrsException} with the specified detail message.
     * 
     * @param message
     *            The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     */
    public NrsException(final String message) {
        super(message);
    }

    /**
     * Constructs an {@code NrsException} with the specified cause.
     * 
     * @param cause
     *            The cause (which is saved for later retrieval by the {@link #getCause()} method). (A null value is
     *            permitted, and indicates that the cause is nonexistent or unknown.)
     * 
     */
    public NrsException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an {@code NrsException} with the specified detail message and cause.
     * 
     * Note that the detail message associated with {@code cause} is <i>not</i> automatically incorporated into this
     * exception's detail message.
     * 
     * @param message
     *            The detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     * 
     * @param cause
     *            The cause (which is saved for later retrieval by the {@link #getCause()} method). (A null value is
     *            permitted, and indicates that the cause is nonexistent or unknown.)
     */
    public NrsException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
