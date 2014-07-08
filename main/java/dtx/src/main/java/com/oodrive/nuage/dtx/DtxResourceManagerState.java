package com.oodrive.nuage.dtx;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
 * States for the resource manager to represent operational constraints when accessing them.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public enum DtxResourceManagerState {

    /**
     * State returned when the requested resource manager is not registered.
     */
    UNREGISTERED,
    /**
     * Default state, entered on registration and whenever information from other instances is needed to proceed.
     */
    UNDETERMINED,
    /**
     * State entered whenever a {@link DtxResourceManager}s last completed transaction ID is less than the last known
     * completed ID for that resource manager.
     */
    LATE,
    /**
     * State entered while synchronizing, i.e. only accepting transactions leading up to a transaction ID considered
     * up-to-date.
     */
    SYNCHRONIZING,
    /**
     * Mandatory state to cross after synchronization or startup, in any case before entering an up-to-date state,
     * designed for additional checks and processing.
     */
    POST_SYNC_PROCESSING,
    /**
     * The only state where transactions are accepted from any initiator, only entered when this instance has
     * successfully caught up.
     */
    UP_TO_DATE;
}