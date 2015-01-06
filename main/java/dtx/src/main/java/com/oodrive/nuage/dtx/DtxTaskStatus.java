package com.oodrive.nuage.dtx;

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

/**
 * Status of a task.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
// TODO: rename/refactor this to properly separate (public) task states from internal transaction states
public enum DtxTaskStatus {

    /** The transaction is submitted, ready to start. */
    PENDING(false),

    /** The transaction is being started in every node. */
    STARTED(false),

    /** The transaction is being prepared in every node. */
    PREPARED(false),

    /** The transaction is committed. */
    COMMITTED(true),

    /** The transaction is rolled back. */
    ROLLED_BACK(true),

    /** The transaction's existence or its state cannot be determined. */
    UNKNOWN(false);

    private final boolean done;

    private DtxTaskStatus(final boolean done) {
        this.done = done;
    }

    /**
     * Tells if a task that has this status is done.
     * 
     * @return <code>true</code> if the status represents a done task.
     */
    public final boolean isDone() {
        return done;
    }

}
