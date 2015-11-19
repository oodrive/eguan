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

import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTDOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.SHUTTING_DOWN;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTING;
import static io.eguan.dtx.DtxResourceManagerState.LATE;
import static io.eguan.dtx.DtxResourceManagerState.POST_SYNC_PROCESSING;
import static io.eguan.dtx.DtxResourceManagerState.SYNCHRONIZING;
import static io.eguan.dtx.DtxResourceManagerState.UNDETERMINED;
import static io.eguan.dtx.DtxResourceManagerState.UNREGISTERED;
import static io.eguan.dtx.DtxResourceManagerState.UP_TO_DATE;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerState;
import io.eguan.dtx.events.DtxEvent;
import io.eguan.dtx.events.DtxResourceManagerEvent;
import io.eguan.dtx.events.HazelcastNodeEvent;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.LifecycleEvent.LifecycleState;

/**
 * Common event listener implementations for testing purposes.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxEventListeners {

    private DtxEventListeners() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Superclass for all event listeners recording assertion errors within their handling methods.
     * 
     * 
     */
    abstract static class ErrorCollectionDtxEvtListener {

        private final List<AssertionError> assertErrors = new CopyOnWriteArrayList<AssertionError>();

        /**
         * Registers an {@link AssertionError} to be retrieved by {@link #getAssertErrors()} later.
         * 
         * @param ae
         *            an {@link AssertionError} triggered during event analysis
         * @param event
         *            the event triggering the error
         */
        protected void registerAssertionError(final AssertionError ae, final DtxEvent<?> event) {
            assertErrors.add(new AssertionError("Event caused assertion error; event=" + event, ae));
        }

        /**
         * Gets the collected assertion errors.
         * 
         * @return a list of {@link AssertionError}s
         */
        final List<AssertionError> getAssertErrors() {
            return assertErrors;
        }

        /**
         * Checks for the current presence of assertion errors.
         * 
         * @param targetLogger
         *            the {@link Logger} for printing detailed error information
         * @throws AssertionError
         *             if any assertion errors are present
         */
        final void checkForAssertErrors(final Logger targetLogger) throws AssertionError {
            for (final AssertionError error : assertErrors) {
                targetLogger.error("State transition assertion failed", error);
            }
            assertTrue("State transition assertion errors: " + assertErrors.size(), assertErrors.isEmpty());
        }

    }

    /**
     * Superclass for event listeners counting down a given {@link CountDownLatch}.
     * 
     * 
     */
    abstract static class LatchCountingDtxEvtListener extends ErrorCollectionDtxEvtListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(LatchCountingDtxEvtListener.class);

        private final CountDownLatch latch;

        /**
         * Internal constructor taking the target {@link CountDownLatch} as argument.
         * 
         * @param latch
         *            a non-<code>null</code> {@link CountDownLatch}
         */
        protected LatchCountingDtxEvtListener(final CountDownLatch latch) {
            this.latch = Objects.requireNonNull(latch);
        }

        /**
         * Counts down the latch submitted at construction.
         */
        protected void countDownLatch() {
            LOGGER.debug("Counting down latch; count=" + latch.getCount());
            latch.countDown();
        }
    }

    /**
     * Event listener generating an {@link AssertionError} on receiving a predefined set of event types.
     * 
     * 
     */
    static final class ErrorGeneratingEventListener extends ErrorCollectionDtxEvtListener {

        private final List<Class<?>> eventClasses;

        /**
         * Constructs an instance for a given set of {@link DtxEvent} classes.
         * 
         * @param targetEventClasses
         *            a set of target {@link Class classes}
         */
        ErrorGeneratingEventListener(final Class<?>... targetEventClasses) {
            this.eventClasses = Arrays.asList(targetEventClasses);
        }

        /**
         * Intercepts all {@link DtxEvent}s.
         * 
         * @param event
         *            the target {@link DtxEvent}
         */
        @Subscribe
        public final void eventOccurred(final DtxEvent<?> event) {
            final Class<?> eventClass = event.getClass();
            for (final Class<?> currClass : eventClasses) {
                if (currClass.isAssignableFrom(eventClass)) {
                    registerAssertionError(new AssertionError("Event not supposed to occur; event=" + event
                            + ", matching event class=" + currClass), event);
                }
            }
        }
    };

    /**
     * Event listener counting resource manager events.
     * 
     * 
     */
    static final class ResMgrStateCountListener extends LatchCountingDtxEvtListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(ResMgrStateCountListener.class);

        private final UUID resMgrId;

        /**
         * Construct an instance for a given latch and an optional resource id.
         * 
         * @param latch
         *            the target {@link CountDownLatch}
         * @param resMgrId
         *            an optional {@link UUID}
         */
        ResMgrStateCountListener(final CountDownLatch latch, final UUID resMgrId) {
            super(latch);
            this.resMgrId = resMgrId;
        }

        /**
         * Handles all resource manager state changes and registers {@link AssertionError}s for non-authorized state
         * transitions.
         * 
         * @param event
         *            the target {@link DtxResourceManagerEvent}
         */
        @Subscribe
        public final void resMgrStateChanged(final DtxResourceManagerEvent event) {
            final DtxResourceManagerState newState = event.getNewState();
            final UUID resId = event.getResourceManagerId();
            if ((resMgrId != null) && !resMgrId.equals(resId)) {
                return;
            }

            final DtxResourceManagerState previousState = event.getPreviousState();
            try {
                // asserts expected state transitions
                assertResMgrStateTransitions(previousState, newState);
            }
            catch (final AssertionError e) {
                LOGGER.error("Assertion failed; resId=" + resId + ", previous=" + previousState + ", new=" + newState,
                        e);
                registerAssertionError(e, event);
            }

            countDownLatch();
            return;
        }
    }

    /**
     * Event listener counting Hazelcast cluster node events.
     * 
     * 
     */
    static final class HazelcastNodeCountListener extends LatchCountingDtxEvtListener {

        /**
         * Constructs an instance with a given latch.
         * 
         * @param latch
         *            the target {@link CountDownLatch}
         */
        HazelcastNodeCountListener(final CountDownLatch latch) {
            super(latch);
        }

        /**
         * Handles all {@link HazelcastNodeEvent}s.
         * 
         * @param event
         *            the triggered {@link HazelcastNodeEvent}
         */
        @Subscribe
        public final void nodeStateChanged(final HazelcastNodeEvent event) {
            final LifecycleState previousState = event.getPreviousState();
            final LifecycleState newState = event.getNewState();

            // asserts expected state transitions
            assertTrue(null != previousState || STARTING == newState || STARTED == newState);
            assertTrue(STARTING != previousState || STARTED == newState);
            assertTrue(SHUTTING_DOWN != previousState || SHUTDOWN == newState);

            countDownLatch();
            return;
        }
    }

    /**
     * Event listener checking all state transitions and counting down the latch once for each new transition to a
     * target {@link DtxResourceManagerState}.
     * 
     * 
     */
    static final class StateCountListener extends LatchCountingDtxEvtListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(SeparateStateCountListener.class);

        private final Multimap<UUID, DtxManager> resMgrMap;
        private final DtxResourceManagerState targetState;

        /**
         * Constructs an instance counting down the given latch for a given set of resource manager IDs.
         * 
         * @param stateCountLatch
         *            the latch to count down on reaching the target {@link DtxResourceManagerState}
         * @param targetState
         *            the {@link DtxResourceManagerState} to count down upon reaching
         * @param resMgrMap
         *            a {@link Multimap} of resource manager IDs mapped to {@link DtxManager}s
         */
        StateCountListener(final CountDownLatch stateCountLatch, final DtxResourceManagerState targetState,
                final Multimap<UUID, DtxManager> resMgrMap) {
            super(stateCountLatch);
            this.resMgrMap = resMgrMap;
            this.targetState = targetState;
        }

        /**
         * Handles all events related to a {@link DtxResourceManager}'s state changes.
         * 
         * @param event
         *            the triggered {@link DtxResourceManagerEvent}
         */
        @Subscribe
        @AllowConcurrentEvents
        public final void resMgrStateChanged(final DtxResourceManagerEvent event) {

            LOGGER.debug("State changed; event=" + event);

            final DtxManager targetDtxManager = event.getSource();
            final UUID targetResMgrId = event.getResourceManagerId();

            if (!resMgrMap.get(targetResMgrId).contains(targetDtxManager)) {
                return;
            }

            final DtxResourceManagerState oldState = event.getPreviousState();
            final DtxResourceManagerState newState = event.getNewState();

            try {
                assertResMgrStateTransitions(oldState, newState);
            }
            catch (final AssertionError ae) {
                registerAssertionError(ae, event);
            }

            if (targetState == newState) {
                countDownLatch();
            }

        }
    }

    /**
     * Event listener checking all state transitions and counting down latches associated to a resource manager and each
     * {@link DtxManager} once for each new transition to a target {@link DtxResourceManagerState}.
     * 
     * 
     */
    static final class SeparateStateCountListener extends ErrorCollectionDtxEvtListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(SeparateStateCountListener.class);

        private final DtxResourceManagerState targetState;

        private final Table<UUID, DtxManager, CountDownLatch> latchTable;

        /**
         * Constructs an instance counting down the given latch for a given set of resource manager IDs.
         * 
         * @param latchTable
         *            a table assoicating a resource manager's {@link UUID} and a {@link DtxManager} to a target
         *            {@link CountDownLatch} to be counted down
         * @param targetState
         *            the {@link DtxResourceManagerState} to count down upon reaching
         */
        SeparateStateCountListener(final Table<UUID, DtxManager, CountDownLatch> latchTable,
                final DtxResourceManagerState targetState) {
            this.latchTable = latchTable;
            this.targetState = targetState;
        }

        /**
         * Handles all events related to a {@link DtxResourceManager}'s state changes.
         * 
         * @param event
         *            the triggered {@link DtxResourceManagerEvent}
         */
        @Subscribe
        @AllowConcurrentEvents
        public final void resMgrStateChanged(final DtxResourceManagerEvent event) {

            LOGGER.debug("State changed; event=" + event);

            final DtxManager targetDtxManager = event.getSource();
            final UUID targetResMgrId = event.getResourceManagerId();

            final DtxResourceManagerState oldState = event.getPreviousState();
            final DtxResourceManagerState newState = event.getNewState();

            try {
                assertResMgrStateTransitions(oldState, newState);
            }
            catch (final AssertionError ae) {
                registerAssertionError(ae, event);
            }

            final CountDownLatch targetLatch = latchTable.get(targetResMgrId, targetDtxManager);
            if ((targetLatch != null) && (targetState == newState)) {
                targetLatch.countDown();
            }

        }
    }

    /**
     * Event listener checking all state transitions and counting down the latch once for each new transition to one of
     * the target {@link DtxResourceManagerState}s.
     * 
     * 
     */
    static final class StateSetCountListener extends LatchCountingDtxEvtListener {

        private static final Logger LOGGER = LoggerFactory.getLogger(SeparateStateCountListener.class);

        private final Multimap<UUID, DtxManager> resMgrMap;
        private final List<DtxResourceManagerState> targetStates;

        /**
         * Constructs an instance counting down the given latch for a given set of resource manager IDs.
         * 
         * @param stateCountLatch
         *            the latch to count down on reaching the target {@link DtxResourceManagerState}
         * @param targetStates
         *            the {@link List} of {@link DtxResourceManagerState}s to count down upon reaching
         * @param resMgrMap
         *            a {@link Multimap} of resource manager IDs mapped to {@link DtxManager}s
         */
        StateSetCountListener(final CountDownLatch stateCountLatch, final List<DtxResourceManagerState> targetStates,
                final Multimap<UUID, DtxManager> resMgrMap) {
            super(stateCountLatch);
            this.resMgrMap = resMgrMap;
            this.targetStates = targetStates;
        }

        /**
         * Handles all events related to a {@link DtxResourceManager}'s state changes.
         * 
         * @param event
         *            the triggered {@link DtxResourceManagerEvent}
         */
        @Subscribe
        @AllowConcurrentEvents
        public final void resMgrStateChanged(final DtxResourceManagerEvent event) {

            LOGGER.debug("State changed; event=" + event);

            final DtxManager targetDtxManager = event.getSource();
            final UUID targetResMgrId = event.getResourceManagerId();

            if (!resMgrMap.get(targetResMgrId).contains(targetDtxManager)) {
                return;
            }

            final DtxResourceManagerState oldState = event.getPreviousState();
            final DtxResourceManagerState newState = event.getNewState();

            try {
                assertResMgrStateTransitions(oldState, newState);
            }
            catch (final AssertionError ae) {
                registerAssertionError(ae, event);
            }

            if (targetStates.contains(newState)) {
                countDownLatch();
            }

        }
    }

    /**
     * Runs a set of assertions covering legal state transitions.
     * 
     * @param previousState
     *            the initial state
     * @param newState
     *            the newly taken state to validate against possible transitions
     * @throws AssertionError
     *             if an illegal state transition was detected
     */
    private static final void assertResMgrStateTransitions(final DtxResourceManagerState previousState,
            final DtxResourceManagerState newState) throws AssertionError {
        final String errMsg = "Illegal state transition. previous state=" + previousState + ", new state=" + newState;

        // UNREGISTERED -> UNDETERMINED
        assertTrue(errMsg, UNREGISTERED != previousState || UNDETERMINED == newState);

        // UNDETERMINED -> POST_SYNC_PROCESSING | LATE | UNREGISTERED
        assertTrue(errMsg, UNDETERMINED != previousState || POST_SYNC_PROCESSING == newState || LATE == newState
                || UNREGISTERED == newState);

        // POST_SYNC_PROCESSING -> UP_TO_DATE | UNDETERMINED | UNREGISTERED
        assertTrue(errMsg, POST_SYNC_PROCESSING != previousState || UP_TO_DATE == newState || UNDETERMINED == newState
                || UNREGISTERED == newState);

        // LATE -> SYNCHRONIZING | UNREGISTERED
        assertTrue(errMsg, LATE != previousState || SYNCHRONIZING == newState || UNREGISTERED == newState);

        // UP_TO_DATE -> LATE | UNDETERMINED | UNREGISTERED
        assertTrue(errMsg, UP_TO_DATE != previousState || LATE == newState || UNDETERMINED == newState
                || UNREGISTERED == newState);

        // SYNCHRONIZING -> UNDETERMINED | UNREGISTERED
        assertTrue(errMsg, SYNCHRONIZING != previousState || UNDETERMINED == newState || UNREGISTERED == newState);

    }

}
