package io.eguan.nbdsrv;

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

import io.eguan.srv.AbstractServerConfig;

import java.net.InetAddress;

/**
 * class for a nbd server configuration.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class NbdServerConfig extends AbstractServerConfig {

    /** Tell if trim is enabled */
    private boolean trimEnabled;

    NbdServerConfig(final InetAddress address, final int port, final boolean trimEnabled) {
        super(address, port);
        this.trimEnabled = trimEnabled;
    }

    /**
     * Tells if trim is enabled.
     * 
     * @return <code>true</code> if trim is enabled, <code>false</code> otherwise
     */
    final boolean isTrimEnabled() {
        return trimEnabled;
    }

    /**
     * Set trim enabled.
     * 
     * @param trimEnabled
     *            the new state of the trim
     */
    final void setTrimEnabled(final boolean trimEnabled) {
        this.trimEnabled = trimEnabled;
    }

    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (trimEnabled ? 1231 : 1237);
        return result;
    }

    @Override
    public final boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        final NbdServerConfig other = (NbdServerConfig) obj;
        if (trimEnabled != other.trimEnabled)
            return false;
        return true;
    }

    @Override
    public final NbdServerConfig clone() {
        return (NbdServerConfig) super.clone();
    }
}
