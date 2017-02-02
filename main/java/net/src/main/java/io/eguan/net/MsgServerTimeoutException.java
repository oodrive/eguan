package io.eguan.net;

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

/**
 * Thrown when a request timed out.
 * 
 * @author oodrive
 * 
 */
public class MsgServerTimeoutException extends Exception {
    private static final long serialVersionUID = -4292368114160060613L;

    MsgServerTimeoutException() {
        super();
    }

    MsgServerTimeoutException(final String message, final Throwable cause, final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    MsgServerTimeoutException(final String message, final Throwable cause) {
        super(message, cause);
    }

    MsgServerTimeoutException(final String message) {
        super(message);
    }

    MsgServerTimeoutException(final Throwable cause) {
        super(cause);
    }

}
