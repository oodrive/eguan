package io.eguan.vvr.repository.core.api;

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

import io.eguan.nrs.NrsFile;

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Result of a block key extended lookup.
 *
 * @author oodrive
 * @author llambert
 *
 */
public class BlockKeyLookupEx {

    /** Marker instance, can be used when a lookup has failed */
    public static final BlockKeyLookupEx NOT_FOUND = new BlockKeyLookupEx();

    private final byte[] key;
    private final boolean sourceCurrent;
    private final UUID sourceNode;

    private BlockKeyLookupEx() {
        super();
        this.key = new byte[0];
        this.sourceCurrent = false;
        this.sourceNode = new UUID(0, 0);
    }

    public BlockKeyLookupEx(@Nonnull final byte[] key) {
        super();
        this.key = Objects.requireNonNull(key);
        this.sourceCurrent = true;
        this.sourceNode = null;
    }

    /**
     * @param key
     * @param sourceNode
     *            node on which the {@link NrsFile} source was created/filled
     */
    public BlockKeyLookupEx(@Nonnull final byte[] key, @Nonnull final UUID sourceNode) {
        super();
        this.key = Objects.requireNonNull(key);
        this.sourceCurrent = false;
        this.sourceNode = sourceCurrent ? null : Objects.requireNonNull(sourceNode);
    }

    /**
     * The key found, not <code>null</code>.
     *
     * @return the key found
     */
    public final byte[] getKey() {
        return key;
    }

    /**
     * Tells if the key have been found in the current storage source.
     *
     * @return <code>true</code> if the key was found in the current block key storage source.
     */
    public final boolean isSourceCurrent() {
        return sourceCurrent;
    }

    public final UUID getSourceNode() {
        return sourceNode;
    }

}
