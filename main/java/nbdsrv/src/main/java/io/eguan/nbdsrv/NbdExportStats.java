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

import javax.annotation.concurrent.Immutable;

/**
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class NbdExportStats {

    /** Name of the target */
    private final String name;
    /** Number of connections */
    private final int connectionCount;
    /** Size in bytes */
    private final long size;
    /** True if the export is read only */
    private final boolean readOnly;

    protected NbdExportStats(final String targetName, final int connectionCount, final long size, final boolean readOnly) {
        this.name = targetName;
        this.connectionCount = connectionCount;
        this.size = size;
        this.readOnly = readOnly;
    }

    /**
     * Gets the name of the target
     * 
     * @return the target name
     */
    public final String getName() {
        return name;
    }

    /**
     * Number of opened connections to the target.
     * 
     * @return number of opened connections.
     */
    public final int getConnectionCount() {
        return connectionCount;
    }

    /**
     * Size of the target in bytes.
     * 
     * @return size of the target.
     */
    public final long getSize() {
        return size;
    }

    /**
     * Tells if the export is read only.
     * 
     * @return <code>true</code> if the export is read only
     */
    public final boolean isReadOnly() {
        return readOnly;
    }

}
