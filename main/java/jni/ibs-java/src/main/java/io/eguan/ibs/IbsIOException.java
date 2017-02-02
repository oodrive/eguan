package io.eguan.ibs;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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
 * Thrown when access to the IBS data fails.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class IbsIOException extends IOException {
    // Not serialized
    private static final long serialVersionUID = 1L;

    /** Mandatory IBS error code */
    private final IbsErrorCode errorCode;

    IbsIOException(final String message, final IbsErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    IbsIOException(final IbsErrorCode errorCode) {
        super();
        this.errorCode = errorCode;
    }

    IbsIOException(final String message, final IbsErrorCode errorCode, final Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    /**
     * Gets the IBS error code.
     * 
     * @return the IBS error code
     */
    public final IbsErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return super.toString() + " [errorCode=" + errorCode + "]";
    }

}
