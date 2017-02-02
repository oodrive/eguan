package io.eguan.dtx.events;

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

import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

/**
 * Superclass for all DTX related events.
 * 
 * @param <T>
 *            the type of the source object
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
@Immutable
public abstract class DtxEvent<T> {

    private final T source;
    private final long timestamp;

    /**
     * Constructor to be overridden by subclasses with custom values.
     * 
     * @param source
     *            the source of the event
     * @param timestamp
     *            the timestamp in milliseconds
     */
    protected DtxEvent(final T source, final long timestamp) {
        this.source = source;
        this.timestamp = timestamp;
    }

    /**
     * Gets the source of the event.
     * 
     * @return the source if one was defined, <code>null</code> otherwise
     */
    public final T getSource() {
        return source;
    }

    /**
     * Gets the timestamp set at construction of the event.
     * 
     * @return the epoch time in milliseconds set at construction
     */
    public final long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets a {@link ToStringHelper} for overriding {@link Object#toString()} in implementing classes.
     * 
     * @return a pre-configured {@link ToStringHelper}
     */
    protected final ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this).omitNullValues().add("source", source)
                .add("timestamp", timestamp);
    }
}
