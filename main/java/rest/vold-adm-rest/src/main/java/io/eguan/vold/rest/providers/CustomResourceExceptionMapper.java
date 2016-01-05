package io.eguan.vold.rest.providers;

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

import io.eguan.vold.rest.errors.CustomResourceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * {@link ExceptionMapper} to map {@link CustomResourceException}s to a response with message and the provided HTTP
 * response status code.
 * 
 * This class must be included as a {@link Provider} in the JAX-RS application context with any class throwing
 * {@link CustomResourceException}s.
 * 
 * @see CustomResourceException
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Provider
public final class CustomResourceExceptionMapper implements ExceptionMapper<CustomResourceException> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomResourceExceptionMapper.class);

    /**
     * The {@link MediaType} to advertise for the {@link Response}'s entity.
     * 
     * Set to {@link MediaType#TEXT_PLAIN_TYPE} by default for an unformatted error message.
     */
    private static final MediaType RESPONSE_ENTITY_TYPE = MediaType.TEXT_PLAIN_TYPE;

    @Override
    public final Response toResponse(final CustomResourceException exception) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Mapping exception: " + exception);
        }

        final String logMsg = exception.getLogMessage();
        if (!Strings.isNullOrEmpty(logMsg)) {
            LOGGER.error(logMsg, exception);
        }

        return Response.status(exception.getHttpStatus()).entity(exception.getMessage()).type(RESPONSE_ENTITY_TYPE)
                .build();
    }

}
