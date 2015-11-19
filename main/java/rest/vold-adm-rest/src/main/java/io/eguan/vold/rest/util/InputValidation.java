package io.eguan.vold.rest.util;

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

import io.eguan.vold.rest.errors.ClientErrorFactory;
import io.eguan.vold.rest.errors.CustomResourceException;

import java.util.UUID;

/**
 * Utility methods for input data checks.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class InputValidation {

    private InputValidation() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Gets a {@link UUID} from the provided {@link String}, throwing a bad request exception on failure.
     * 
     * @param uuidString
     *            the {@link String} to parse
     * @return a valid {@link UUID} instance
     * @throws CustomResourceException
     *             with the return status "Bad Request" if the argument cannot be parsed
     * @see UUID#fromString(String)
     */
    public static final UUID getUuidFromString(final String uuidString) throws CustomResourceException {
        try {
            return UUID.fromString(uuidString);
        }
        catch (final NullPointerException | IllegalArgumentException ie) {
            throw ClientErrorFactory.newBadRequestException("Bad request due to invalid ID", "Failed to parse UUID "
                    + uuidString, ie);
        }
    }
}
