package io.eguan.vold.rest.errors;

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

import javax.annotation.concurrent.Immutable;
import javax.ws.rs.core.Response.Status;

/**
 * Custom {@link RuntimeException} adding an internal log message and a HTTP status code to return to the client.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public final class CustomResourceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String logMessage;

    private final Status httpStatus;

    protected CustomResourceException(final String message, final String logMessage, final Status httpStatus) {
        this(message, logMessage, httpStatus, null);
    }

    protected CustomResourceException(final String message, final String logMessage, final Status httpStatus,
            final Throwable cause) {
        super(message, cause);
        this.logMessage = logMessage;
        this.httpStatus = httpStatus;
    }

    public final String getLogMessage() {
        return logMessage;
    }

    public final Status getHttpStatus() {
        return httpStatus;
    }

}
