package io.eguan.nbdsrv;

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

import io.eguan.srv.AbstractServerMXBean;

/**
 * MXBean definitions for the {@link NbdServer}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 */
public interface NbdServerMXBean extends AbstractServerMXBean {

    /**
     * Gets the list of exports.
     * 
     * @return a read-only view of some attributes of the exports registered in the server.
     */
    NbdExportAttributes[] getExports();

    /**
     * Tells if trim is enabled.
     * 
     * @return <code>true</code> if trim is enabled, false otherwise
     */
    boolean isTrimEnabled();

    /**
     * Set trim enabled
     * 
     * @param enabled
     *            <code>true</code> if trim is enabled, false otherwise.
     */
    void setTrimEnabled(final boolean enabled);

}
