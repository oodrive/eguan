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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;

import org.hamcrest.Matcher;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

/**
 * Utilities for configuring mockito mocks.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class DtxMockUtils {

    /**
     * Maximum verification timeout for mock implementations.
     */
    static final int VERIFY_TIMEOUT_MS = 20000;

    private DtxMockUtils() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Matcher wrapping a {@link Comparator} comparing to a reference object of the given type.
     * 
     * The {@link Comparator} determines the matching result by returning either 0 for matching objects or any non-zero
     * value otherwise upon calling {@link Comparator#compare(Object, Object)}.
     * 
     * 
     * @param <T>
     *            the type for which a {@link Comparator} must be provided
     */
    static final class CompMatcher<T> extends ArgumentMatcher<T> {
        private final T original;
        private final Comparator<T> comparator;

        /**
         * Constructs an instance with a reference object and a {@link Comparator}.
         * 
         * @param original
         *            a non-<code>null</code> reference object of type T
         * @param comparator
         *            a non-<code>null</code> {@link Comparator}
         */
        CompMatcher(@Nonnull final T original, @Nonnull final Comparator<T> comparator) {
            this.original = Objects.requireNonNull(original);
            this.comparator = Objects.requireNonNull(comparator);
        }

        @SuppressWarnings("unchecked")
        @Override
        public final boolean matches(final Object argument) {
            if (argument == null) {
                return false;
            }
            return this.comparator.compare(original, (T) argument) == 0;
        }
    }

    /**
     * Matcher for comparing the {@link DtxResourceManagerContext#getResourceManagerId() resource manager ID} contained
     * in a {@link DtxResourceManagerContext} instance.
     * 
     * 
     */
    static final class ResourceIdMatcher extends ArgumentMatcher<DtxResourceManagerContext> {

        private final UUID referenceId;

        /**
         * Constructs an instance with a reference ID against which to compare.
         * 
         * @param referenceId
         *            a non-<code>null</code> {@link UUID} to compare against
         */
        ResourceIdMatcher(@Nonnull final UUID referenceId) {
            this.referenceId = Objects.requireNonNull(referenceId);
        }

        @Override
        public final boolean matches(final Object argument) {
            if (!(argument instanceof DtxResourceManagerContext)) {
                return false;
            }
            return referenceId.equals(((DtxResourceManagerContext) argument).getResourceManagerId());
        }

    }

    /**
     * Basic matcher only used to produce appropriate generic type arguments for Mockito.argThat(Matcher) calls.
     * 
     * 
     * @param <T>
     *            the generic type to produce
     */
    static final class WrapMatcher<T> extends ArgumentMatcher<T> {

        private final Matcher<?> wrapped;

        /**
         * Constructs a new instance wrapping the provided {@link Matcher}.
         * 
         * @param wrapped
         *            the non-<code>null</code> {@link Matcher} to wrap
         */
        WrapMatcher(@Nonnull final Matcher<?> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public final boolean matches(final Object argument) {
            return wrapped.matches(argument);
        }

    }

    /**
     * Matcher for {@link DtxResourceManagerContext}s being in one of the declared valid states.
     * 
     * Any {@link DtxResourceManagerContext} whose {@link DtxResourceManagerContext#getTxStatus()} returns one of the
     * valid states matches.
     * 
     * 
     */
    static final class TxPositiveStateMatcher extends ArgumentMatcher<DtxResourceManagerContext> {

        private final List<DtxTaskStatus> validStates;

        /**
         * Constructs an instance with respect to a list of valid states.
         * 
         * @param validStates
         *            a list of {@link DtxTaskStatus}
         */
        TxPositiveStateMatcher(final DtxTaskStatus... validStates) {
            this.validStates = Arrays.asList(validStates);
        }

        @Override
        public final boolean matches(final Object argument) {
            if (!(argument instanceof DtxResourceManagerContext)) {
                return false;
            }
            return validStates.contains(((DtxResourceManagerContext) argument).getTxStatus());
        }
    }

    /**
     * Matcher for {@link DtxResourceManagerContext}s not being in one of the declared invalid states.
     * 
     * Any {@link DtxResourceManagerContext} whose {@link DtxResourceManagerContext#getTxStatus()} does not return one
     * of the invalid states matches.
     * 
     * 
     */
    static final class TxNegativeStateMatcher extends ArgumentMatcher<DtxResourceManagerContext> {

        private final List<DtxTaskStatus> invalidStates;

        /**
         * Constructs an instance with respect to a list of invalid states.
         * 
         * @param invalidStates
         *            a list of {@link DtxTaskStatus}
         */
        TxNegativeStateMatcher(final DtxTaskStatus... invalidStates) {
            this.invalidStates = Arrays.asList(invalidStates);
        }

        @Override
        public final boolean matches(final Object argument) {
            if (!(argument instanceof DtxResourceManagerContext)) {
                return false;
            }
            return !invalidStates.contains(((DtxResourceManagerContext) argument).getTxStatus());
        }
    }

    /**
     * Verifies successful in-order execution of one transaction on a given {@link DtxResourceManager}.
     * 
     * @param targetResMgr
     *            the target {@link DtxResourceManager}
     * @param totalTimes
     *            the total of times a transaction completed successfully on the given mock
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    static final void verifySuccessfulTxExecution(final DtxResourceManager targetResMgr, final int totalTimes)
            throws XAException {
        verify(targetResMgr, timeout(VERIFY_TIMEOUT_MS).times(totalTimes)).commit(any(DtxResourceManagerContext.class));
        verify(targetResMgr, atLeast(totalTimes)).start(any(byte[].class));
        verify(targetResMgr, atLeast(totalTimes)).prepare(any(DtxResourceManagerContext.class));

        // skips checking for invocation order on multiple invocations as a perfect order cannot be guaranteed with
        // concurrently executed transactions
        if (totalTimes > 1) {
            return;
        }
        final InOrder orderVerifier = inOrder(targetResMgr);
        orderVerifier.verify(targetResMgr).start(any(byte[].class));
        orderVerifier.verify(targetResMgr).prepare(any(DtxResourceManagerContext.class));
        orderVerifier.verify(targetResMgr).commit(any(DtxResourceManagerContext.class));
    }

    /**
     * Verifies a transaction was rolled back on the given {@link DtxResourceManager}.
     * 
     * @param targetResMgr
     *            the target {@link DtxResourceManager}
     * @param totalTimes
     *            the total number of calls to verify
     * @param afterPrepare
     *            if the rollback call must be done after prepare
     * @throws XAException
     *             if mock setup fails, not part of this test
     */
    static final void verifyRollbackOnTx(final DtxResourceManager targetResMgr, final int totalTimes,
            final boolean afterPrepare) throws XAException {
        verify(targetResMgr, timeout(VERIFY_TIMEOUT_MS).times(totalTimes)).rollback(
                any(DtxResourceManagerContext.class));
        verify(targetResMgr, atLeast(totalTimes)).start(any(byte[].class));
        if (afterPrepare) {
            verify(targetResMgr, atLeast(totalTimes)).prepare(any(DtxResourceManagerContext.class));
        }
        verify(targetResMgr, never()).commit(any(DtxResourceManagerContext.class));

        final InOrder orderVerifier = inOrder(targetResMgr);
        orderVerifier.verify(targetResMgr).start(any(byte[].class));
        if (afterPrepare) {
            orderVerifier.verify(targetResMgr).prepare(any(DtxResourceManagerContext.class));
        }
        orderVerifier.verify(targetResMgr).rollback(any(DtxResourceManagerContext.class));
    }

}
