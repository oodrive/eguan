package io.eguan.vvr.persistence.repository;

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

import io.eguan.nrs.NrsFile;
import io.eguan.vvr.repository.core.api.BlockKeyLookupEx;

import java.util.UUID;

/**
 * Enhanced {@link BlockKeyLookupEx}, telling which {@link NrsFile} contains the key.
 *
 * @author oodrive
 * @author llambert
 */
public final class NrsBlockKeyLookupEx extends BlockKeyLookupEx {

    private final NrsFile nrsFileSource;

    public NrsBlockKeyLookupEx(final byte[] key, final NrsFile nrsFileSource) {
        super(key);
        this.nrsFileSource = nrsFileSource;
    }

    public NrsBlockKeyLookupEx(final byte[] key, final NrsFile nrsFileSource, final UUID sourceNode) {
        super(key, sourceNode);
        this.nrsFileSource = nrsFileSource;
    }

    /**
     * The {@link NrsFile} source.
     *
     * @return the source file, not <code>null</code>.
     */
    public final NrsFile getNrsFileSource() {
        return nrsFileSource;
    }

}
