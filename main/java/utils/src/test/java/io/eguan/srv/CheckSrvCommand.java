package io.eguan.srv;

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

import io.eguan.utils.unix.UnixNbdTarget;

import java.io.IOException;

public interface CheckSrvCommand {

    /**
     * Check if the trim command has been executed correctly.
     * 
     * @param unixNbdClient
     *            the client
     * @param waiting
     *            the waiting number of trim
     * @param compareExactly
     *            tells if the actual number of trim must be equal to the expected number, or if it must be only
     *            positive
     * @throws IOException
     * 
     */
    public void checkTrim(final UnixNbdTarget unixNbdClient, final long expected, final boolean compareExactly)
            throws IOException;

}
