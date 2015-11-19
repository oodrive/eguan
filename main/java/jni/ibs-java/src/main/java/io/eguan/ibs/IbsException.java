package io.eguan.ibs;

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

/**
 * Exception thrown when an operation on an IBS fails.
 * 
 * @author oodrive
 * @author llambert
 */
public final class IbsException extends RuntimeException {
    // Not serialized
    private static final long serialVersionUID = 1L;

    /** Optional IBS error code */
    private IbsErrorCode errorCode;

    IbsException() {
        super();
    }

    IbsException(final String message) {
        super(message);
    }

    IbsException(final String message, final IbsErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    IbsException(final Throwable cause) {
        super(cause);
    }

    IbsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    IbsException(final String message, final Throwable cause, final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /**
     * Gets the optional IBS error code.
     * 
     * @return the IBS error code or <code>null</code>
     */
    public final IbsErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return super.toString() + (errorCode == null ? "" : " [errorCode=" + errorCode + "]");
    }
}
