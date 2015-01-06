package com.oodrive.nuage.nbdsrv;

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

import java.beans.ConstructorProperties;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Attributes of an export. This is a snapshot of the attributes of the export.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public final class NbdExportAttributes {

    /** Name of the export */
    private final String name;
    /** Number of connections */
    private final int connectionCount;
    /** Size in bytes */
    private final long size;
    /** Tell if the export is read only */
    private final boolean readOnly;

    @ConstructorProperties({ "name", "connectionCount", "size" })
    protected NbdExportAttributes(@Nonnull final String name, @Nonnegative final int connectionCount,
            @Nonnegative final long size, final boolean readOnly) {
        super();
        this.name = Objects.requireNonNull(name);
        if (connectionCount < 0) {
            throw new IllegalArgumentException("connectionCount");
        }
        this.connectionCount = connectionCount;
        if (size <= 0) {
            throw new IllegalArgumentException("size");
        }
        this.size = size;

        this.readOnly = readOnly;
    }

    /**
     * Gets the name of the export.
     * 
     * @return the export name.
     */
    public final String getName() {
        return name;
    }

    /**
     * Number of opened connections to the export.
     * 
     * @return number of opened connections.
     */
    public final int getConnectionCount() {
        return connectionCount;
    }

    /**
     * Size of the export in bytes.
     * 
     * @return size of the export.
     */
    public final long getSize() {
        return size;
    }

    /**
     * Tells if the exports is read only.
     * 
     * @return true if the export is read-only
     */
    public final boolean isReadOnly() {
        return readOnly;
    }

}
