package io.eguan.dtx;

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

import java.util.UUID;

/**
 * Abstract class used for additionnal Info for {@link DtxTask}. Keeping in the extra datas of the {@link TaskKeeper}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author pwehrle
 * 
 */
public abstract class DtxTaskInfo {

    private final String source;

    /**
     * Constructs an instance for a given source.
     * 
     * @param source
     *            the source's ID
     */
    protected DtxTaskInfo(final String source) {
        this.source = source;
    }

    /**
     * Constructs an instance from a given source's ID.
     * 
     * @param source
     *            the source's {@link UUID}
     */
    protected DtxTaskInfo(final UUID source) {
        this(source.toString());
    }

    /**
     * Gets the source ID.
     * 
     * @return the ID given at construction
     */
    public final String getSource() {
        return source;
    }
}
