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

import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxNodeState;

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Event relative to the life cycle of a {@link DtxManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxNodeEvent extends DtxEvent<DtxManager> {

    /**
     * The state prior to the transition.
     */
    private final DtxNodeState previousState;

    /**
     * The state after the transition.
     */
    private final DtxNodeState newState;

    /**
     * Constructs an instance for a given {@link DtxManager}.
     * 
     * @param source
     *            the source {@link DtxManager}
     * @param previousState
     *            the non-<code>null</code> {@link DtxNodeState} it was in before
     * @param newState
     *            the result {@link DtxNodeState} of the represented transition
     * @throws NullPointerException
     *             if any argument is <code>null</code>
     */
    @ParametersAreNonnullByDefault
    public DtxNodeEvent(final DtxManager source, final DtxNodeState previousState, final DtxNodeState newState)
            throws NullPointerException {
        super(Objects.requireNonNull(source), System.currentTimeMillis());
        this.previousState = Objects.requireNonNull(previousState);
        this.newState = Objects.requireNonNull(newState);
    }

    /**
     * Gets the previous state.
     * 
     * @return a {@link DtxNodeState}
     */
    public final DtxNodeState getPreviousState() {
        return previousState;
    }

    /**
     * Gets the new state.
     * 
     * @return a {@link DtxNodeState}
     */
    public final DtxNodeState getNewState() {
        return newState;
    }

    @Override
    public final String toString() {
        return toStringHelper().add("previousState", previousState).add("newState", newState).toString();
    }

}
