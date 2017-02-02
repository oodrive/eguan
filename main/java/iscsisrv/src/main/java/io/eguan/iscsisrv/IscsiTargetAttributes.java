package io.eguan.iscsisrv;

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

import java.beans.ConstructorProperties;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Attributes of a target. This is a snapshot of the attributes of the target.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
@Immutable
public final class IscsiTargetAttributes {

    /** Name of the target */
    private final String name;
    /** Alias of the target */
    private final String alias;
    /** Number of connections */
    private final int connectionCount;
    /** Size in bytes */
    private final long size;
    /** Tells if the target is read only */
    private final boolean readOnly;

    @ConstructorProperties({ "name", "alias", "connectionCount", "size" })
    protected IscsiTargetAttributes(@Nonnull final String name, @Nullable final String alias,
            @Nonnegative final int connectionCount, @Nonnegative final long size, final boolean readOnly) {
        super();
        this.name = Objects.requireNonNull(name);
        this.alias = alias == null ? "" : alias;
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
     * Gets the name of the target
     * 
     * @return the target name
     */
    public final String getName() {
        return name;
    }

    /**
     * Gets the alias name of the target.
     * 
     * @return target alias
     */
    public final String getAlias() {
        return alias;
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
     * Tells if the target is read only.
     * 
     * @return <code>true</code> if the target is read only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

}
