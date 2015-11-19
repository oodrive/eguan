package io.eguan.vold.model;

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
 * Thrown when the {@link VvrManager} failed to manage
 * {@link io.eguan.vvr.repository.core.api.VersionedVolumeRepository}s.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public final class VvrManagementException extends Exception {
    private static final long serialVersionUID = -3059464845634405121L;

    public VvrManagementException() {
        super();
    }

    public VvrManagementException(final String message) {
        super(message);
    }

    public VvrManagementException(final Throwable cause) {
        super(cause);
    }

    public VvrManagementException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public VvrManagementException(final String message, final Throwable cause, final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
