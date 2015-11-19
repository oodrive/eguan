package io.eguan.dtx;

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
 * The state used to describe the status of the local cluster node.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public enum DtxNodeState {
    /**
     * Uninitialized state before {@link DtxManager#init()} has been called or after a successful
     * {@link DtxManager#fini()}.
     */
    NOT_INITIALIZED,
    /**
     * State after a successful {@link DtxManager#init()}.
     */
    INITIALIZED,
    /**
     * State after a successful {@link DtxManager#start()}.
     */
    STARTED,
    /**
     * State after receiving being disconnected.
     */
    // TODO: find out if and when this happens and adapt behavior in {@link #mapHazelcastState(LifecycleState)} and
    // {@link DtxManager#hzLifecycleListener}
    DISCONNECTED,
    /**
     * State after any of the transition operations has produced an error.
     */
    FAILED;
}
