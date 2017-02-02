package io.eguan.vold.rest.errors;

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

import javax.ws.rs.core.Response.Status;

/**
 * Factory for {@link CustomResourceException} returning Client Error (4xx) HTTP status codes.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class ClientErrorFactory {

    private ClientErrorFactory() {
        throw new AssertionError("Not instantiable");
    }

    public static final CustomResourceException newNotFoundException(final String message, final String logMessage) {
        return new CustomResourceException(message, logMessage, Status.NOT_FOUND);
    }

    public static final CustomResourceException newBadRequestException(final String message, final String logMessage,
            final Throwable cause) {
        return new CustomResourceException(message, logMessage, Status.BAD_REQUEST, cause);
    }

    public static final CustomResourceException newForbiddenException(final String message, final String logMessage) {
        return new CustomResourceException(message, logMessage, Status.FORBIDDEN);
    }

}
