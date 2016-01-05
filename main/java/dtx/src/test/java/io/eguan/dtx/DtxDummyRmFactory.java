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

import static io.eguan.dtx.DtxTaskStatus.COMMITTED;
import static io.eguan.dtx.DtxTaskStatus.PREPARED;
import static io.eguan.dtx.DtxTaskStatus.ROLLED_BACK;
import static io.eguan.dtx.DtxTaskStatus.STARTED;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.DtxTaskStatus;
import io.eguan.dtx.TransactionManager;
import io.eguan.dtx.DtxMockUtils.CompMatcher;
import io.eguan.dtx.DtxMockUtils.TxNegativeStateMatcher;
import io.eguan.dtx.DtxMockUtils.TxPositiveStateMatcher;
import io.eguan.dtx.DtxMockUtils.WrapMatcher;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.transaction.xa.XAException;

import org.hamcrest.Matcher;
import org.mockito.internal.matchers.And;
import org.mockito.internal.matchers.InstanceOf;
import org.mockito.internal.matchers.Not;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.LifecycleService;

/**
 * Factory for creating {@link DtxResourceManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxDummyRmFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DtxDummyRmFactory.class);

    /**
     * Default (fake) transaction payload to be used with mocks.
     */
    public static final byte[] DEFAULT_PAYLOAD = "fakePayload".getBytes();

    /**
     * Bad payload correctly rejected by mocks.
     */
    public static final byte[] BAD_PAYLOAD = "badPayload".getBytes();

    /**
     * A constant default resource {@link UUID}.
     */
    public static final UUID DEFAULT_RES_UUID = UUID.fromString("84f63d7e-9081-11e2-b663-180373e16ea9");

    /**
     * A simple implementation of {@link DtxResourceManagerContext} to be returned by mocks.
     */
    public static final DtxResourceManagerContext DEFAULT_TX_CONTEXT = new DefaultContext(DEFAULT_RES_UUID);

    private DtxDummyRmFactory() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * The default {@link Answer} for a {@link DtxResourceManager#start(byte[])} call.
     * 
     * 
     */
    public static final class DefaultStartAnswer implements Answer<DtxResourceManagerContext> {

        private final UUID resourceId;

        /**
         * Creates an {@link Answer} returning a {@link DtxResourceManagerContext} instance matching the given resource
         * {@link UUID}.
         * 
         * @param resourceId
         *            a non-<code>null</code> {@link UUID}
         */
        DefaultStartAnswer(@Nonnull final UUID resourceId) {
            this.resourceId = Objects.requireNonNull(resourceId);
        }

        @Override
        public final DtxResourceManagerContext answer(final InvocationOnMock invocation) throws Throwable {
            return doAnswer(invocation, resourceId);
        }

        /**
         * Build and return the default answer for the given invocation.
         * 
         * @param invocation
         *            a valid {@link InvocationOnMock}
         * @param resourceId
         *            the {@link UUID} of the target resource manager
         * @return an instance of {@link DtxResourceManagerContext} matching the invocation
         */
        static final DtxResourceManagerContext doAnswer(final InvocationOnMock invocation,
                @Nonnull final UUID resourceId) {
            final Object[] args = invocation.getArguments();
            if (args.length != 1) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            return new FakeDtxResourceManagerContext(resourceId, STARTED);
        }
    }

    /**
     * The default {@link Answer} returned for a {@link DtxResourceManager#prepare(DtxResourceManagerContext)} call.
     * 
     * 
     */
    public static final class DefaultPrepareAnswer implements Answer<Boolean> {

        @Override
        public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
            return doAnswer(invocation);
        }

        /**
         * Build and return the default answer for the given invocation.
         * 
         * @param invocation
         *            a valid {@link InvocationOnMock}
         * @return {@link Boolean#TRUE} if the invocation is valid, {@value Boolean#FALSE} otherwise
         */
        static final Boolean doAnswer(final InvocationOnMock invocation) {
            final Object[] args = invocation.getArguments();
            if (args.length != 1) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            ((DtxResourceManagerContext) args[0]).setTxStatus(PREPARED);
            return Boolean.TRUE;
        }

    }

    /**
     * The default {@link Answer} returned for a {@link DtxResourceManager#commit(DtxResourceManagerContext)} call.
     * 
     * 
     */
    public static final class DefaultCommitAnswer implements Answer<Void> {

        @Override
        public final Void answer(final InvocationOnMock invocation) throws Throwable {
            return doAnswer(invocation);
        }

        /**
         * Perform the default answering operations for the given invocation.
         * 
         * @param invocation
         *            a valid {@link InvocationOnMock}
         * @return <code>null</code>
         */
        static final Void doAnswer(final InvocationOnMock invocation) {
            final Object[] args = invocation.getArguments();
            if (args.length != 1) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            ((DtxResourceManagerContext) invocation.getArguments()[0]).setTxStatus(COMMITTED);
            return null;
        }
    }

    /**
     * The default {@link Answer} returned for a {@link DtxResourceManager#rollback(DtxResourceManagerContext)} call.
     * 
     * 
     */
    public static final class DefaultRollbackAnswer implements Answer<Void> {

        @Override
        public final Void answer(final InvocationOnMock invocation) throws Throwable {
            return doAnswer(invocation);
        }

        /**
         * Perform the default answering operations for the given invocation.
         * 
         * @param invocation
         *            a valid {@link InvocationOnMock}
         * @return <code>null</code>
         */
        static final Void doAnswer(final InvocationOnMock invocation) {
            final Object[] args = invocation.getArguments();
            if (args.length != 1) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            ((DtxResourceManagerContext) invocation.getArguments()[0]).setTxStatus(ROLLED_BACK);
            return null;
        }
    }

    /**
     * Special {@link Answer} for the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method that shuts
     * down the target Hazelcast node.
     * 
     * 
     */
    public static final class NodeShutdownStartAnswer implements Answer<DtxResourceManagerContext> {

        private static final int BARRIER_WAIT_TIMEOUT_S = 10;

        private final LifecycleService targetHzLifecycle;
        private final CyclicBarrier barrier;

        /**
         * Constructs an instance applied to the given {@link LifecycleService}, synchronizing the shutdown on the
         * optional {@link CyclicBarrier}.
         * 
         * @param targetHzLifecycle
         *            the non-<code>null</code> {@link LifecycleService} to shut down
         * @param barrier
         *            the optional {@link CyclicBarrier} to synchronize the shutdown on
         */
        NodeShutdownStartAnswer(@Nonnull final LifecycleService targetHzLifecycle, final CyclicBarrier barrier) {
            this.targetHzLifecycle = Objects.requireNonNull(targetHzLifecycle);
            this.barrier = barrier;
        }

        @Override
        public final DtxResourceManagerContext answer(final InvocationOnMock invocation) throws Throwable {
            // optionally waits on the barrier
            if (barrier != null) {
                LOGGER.debug("waiting for barrier in thread " + Thread.currentThread().getName());
                barrier.await(BARRIER_WAIT_TIMEOUT_S, TimeUnit.SECONDS);
                LOGGER.debug("barrier gone through in thread " + Thread.currentThread().getName());
            }
            // shuts down the Hazelcast peer
            targetHzLifecycle.shutdown();
            assertFalse(targetHzLifecycle.isRunning());
            return null;
        }
    }

    /**
     * Special {@link Answer} for the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method that shuts
     * down the target Hazelcast node.
     * 
     * 
     */
    public static final class NodeShutdownPrepareAnswer implements Answer<Boolean> {

        private static final int BARRIER_WAIT_TIMEOUT_S = 10;

        private final LifecycleService targetHzLifecycle;
        private final CyclicBarrier barrier;

        /**
         * Constructs an instance applied to the given {@link LifecycleService}, synchronizing the shutdown on the
         * optional {@link CyclicBarrier}.
         * 
         * @param targetHzLifecycle
         *            the non-<code>null</code> {@link LifecycleService} to shut down
         * @param barrier
         *            the optional {@link CyclicBarrier} to synchronize the shutdown on
         */
        NodeShutdownPrepareAnswer(@Nonnull final LifecycleService targetHzLifecycle, final CyclicBarrier barrier) {
            this.targetHzLifecycle = Objects.requireNonNull(targetHzLifecycle);
            this.barrier = barrier;
        }

        @Override
        public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
            // optionally waits on the barrier
            if (barrier != null) {
                LOGGER.debug("waiting for barrier in thread " + Thread.currentThread().getName());
                barrier.await(BARRIER_WAIT_TIMEOUT_S, TimeUnit.SECONDS);
                LOGGER.debug("barrier gone through in thread " + Thread.currentThread().getName());
            }
            // shuts down the Hazelcast peer
            targetHzLifecycle.shutdown();
            assertFalse(targetHzLifecycle.isRunning());
            return Boolean.FALSE;
        }
    }

    /**
     * Wrapping {@link Answer} implementation that can be toggled to throw an exception or the result of an
     * {@link Answer} given at construction.
     * 
     * @param <T>
     *            the return type for the wrapped {@link Answer} type
     * 
     * 
     */
    public static final class ToggleAnswer<T> implements Answer<T> {

        private volatile boolean triggerError = false;
        private final int throwMe;
        private final Answer<T> defaultAnswer;

        /**
         * Constructs a generic instance.
         * 
         * @param initialErrorToggle
         *            <code>true</code> if exceptions shall be thrown, <code>false</code> for normal behavior
         * @param errorCode
         *            the error code of the {@link XAException} to throw
         * @param defaultAnswer
         *            the default {@link Answer} to which to redirect calls when not throwing exceptions
         */
        ToggleAnswer(final boolean initialErrorToggle, final int errorCode, final Answer<T> defaultAnswer) {
            triggerError = initialErrorToggle;
            this.throwMe = errorCode;
            this.defaultAnswer = Objects.requireNonNull(defaultAnswer);
        }

        /**
         * Toggles the exception throwing behavior.
         */
        public void toggleErrorTrigger() {
            triggerError = !triggerError;
        }

        @Override
        public T answer(final InvocationOnMock invocation) throws Throwable {
            if (triggerError) {
                throw new XAException(throwMe);
            }
            return defaultAnswer.answer(invocation);
        }

    }

    private static final class DefaultContext extends DtxResourceManagerContext {

        protected DefaultContext(final UUID resourceManagerId) throws NullPointerException {
            super(resourceManagerId);
        }
    }

    /**
     * Creates a mock {@link DtxResourceManager} that correctly mimics error-less transaction processing.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    static DtxResourceManager newResMgrThatDoesEverythingRight(final UUID resourceId) throws XAException {
        if (resourceId != null) {
            return new DtxResourceManagerBuilder().setId(resourceId).build();
        }
        return new DtxResourceManagerBuilder().build();
    }

    /**
     * Creates a mock {@link DtxResourceManager} that fails on calls to {@link DtxResourceManager#start(byte[])}.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @param throwMe
     *            the {@link XAException} to throw
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    static DtxResourceManager newResMgrFailingOnStart(final UUID resourceId, final XAException throwMe)
            throws XAException {
        return new DtxResourceManagerBuilder().setId(resourceId).setStart(throwMe, null).build();
    }

    /**
     * Creates a mock {@link DtxResourceManager} that fails on calls to
     * {@link DtxResourceManager#prepare(DtxResourceManagerContext)}.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @param throwMe
     *            the {@link XAException} to throw
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    static DtxResourceManager newResMgrFailingOnPrepare(final UUID resourceId, final XAException throwMe)
            throws XAException {
        return new DtxResourceManagerBuilder().setId(resourceId).setPrepare(throwMe, null).build();
    }

    /**
     * Creates a mock {@link DtxResourceManager} that fails on calls to
     * {@link DtxResourceManager#commit(DtxResourceManagerContext)}.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @param throwMe
     *            the {@link XAException} to throw
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    static DtxResourceManager newResMgrFailingOnCommit(final UUID resourceId, final XAException throwMe)
            throws XAException {
        return new DtxResourceManagerBuilder().setId(resourceId).setCommit(throwMe, null).build();
    }

    /**
     * Creates a mock {@link DtxResourceManager} that fails on calls to
     * {@link DtxResourceManager#rollback(DtxResourceManagerContext)}.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @param throwMe
     *            the {@link XAException} to throw
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    static DtxResourceManager newResMgrFailingOnRollback(final UUID resourceId, final XAException throwMe)
            throws XAException {
        return new DtxResourceManagerBuilder().setId(resourceId).setRollback(throwMe, null).build();
    }

    /**
     * Creates a mock {@link DtxResourceManager} that fails on calls to
     * {@link DtxResourceManager#rollback(DtxResourceManagerContext)}.
     * 
     * @param resourceId
     *            the optional ID to set for the result {@link DtxResourceManager}
     * @param txManager
     *            the {@link TransactionManager} to unregister from during the prepare operation
     * @return a functional {@link DtxResourceManager}
     * @throws XAException
     *             if construction fails
     */
    @ParametersAreNonnullByDefault
    static DtxResourceManager newResMgrUnregisteringOnPrepare(final UUID resourceId, final TransactionManager txManager)
            throws XAException {
        final Answer<Boolean> answer = new Answer<Boolean>() {
            @Override
            public final Boolean answer(final InvocationOnMock invocation) throws Throwable {
                final Boolean result = DefaultPrepareAnswer.doAnswer(invocation);
                txManager.unregisterResourceManager(resourceId);
                return result;
            }
        };
        return new DtxResourceManagerBuilder().setId(resourceId).setPrepare(null, answer).build();
    }

    /**
     * Builder class for mock {@link DtxResourceManager} instances.
     * 
     * 
     */
    public static final class DtxResourceManagerBuilder {

        private final DtxResourceManager result;
        private final CompMatcher<byte[]> notPayloadMatcher;
        private final WrapMatcher<DtxResourceManagerContext> notFakeCtxMatcher;

        private boolean setIdDone = false;
        private boolean setStartDone = false;
        private boolean setPrepareDone = false;
        private boolean setCommitDone = false;
        private boolean setRollbackDone = false;
        private boolean setPostSyncDone = false;

        /**
         * Matcher for both context class and resource ID.
         */
        private And ctxIdMatcher;

        /**
         * Matcher for any context instance.
         */
        private WrapMatcher<DtxResourceManagerContext> anyCtxMatcher;

        /**
         * Constructs a new unconfigured instance.
         */
        public DtxResourceManagerBuilder() {
            result = mock(DtxResourceManager.class);

            notPayloadMatcher = new CompMatcher<byte[]>(DEFAULT_PAYLOAD, new Comparator<byte[]>() {

                @Override
                public final int compare(final byte[] o1, final byte[] o2) {
                    return Arrays.equals(o1, o2) ? -1 : 0;
                }
            });

            notFakeCtxMatcher = new WrapMatcher<DtxResourceManagerContext>(new Not(new InstanceOf(
                    FakeDtxResourceManagerContext.class)));

            anyCtxMatcher = new WrapMatcher<DtxResourceManagerContext>(new InstanceOf(DtxResourceManagerContext.class));
        }

        /**
         * Defines the {@link DtxResourceManager}'s ID.
         * 
         * @param resourceId
         *            the {@link UUID} returned by {@link DtxResourceManager#getId()}, if <code>null</code> a random
         *            value is set
         * @return the configured builder
         */
        public final DtxResourceManagerBuilder setId(final UUID resourceId) {
            // defines getId() behavior
            final UUID resultId = resourceId == null ? UUID.randomUUID() : resourceId;
            when(result.getId()).thenReturn(resultId);
            this.ctxIdMatcher = new And(
                    Arrays.asList(new Matcher[] { new InstanceOf(FakeDtxResourceManagerContext.class),
                            new DtxMockUtils.ResourceIdMatcher(resultId) }));
            setIdDone = true;
            return this;
        }

        /**
         * Defines {@link DtxResourceManager#start(DtxResourceManagerContext)} behavior.
         * 
         * @param xe
         *            a {@link XAException} to throw, default behavior is assumed when <code>null</code>
         * @param customAnswer
         *            a specific {@link Answer} to return on a perfectly valid call, or <code>null</code> for the
         *            default answer
         * @return the configured builder
         * @throws XAException
         *             if configuration fails
         */
        public final DtxResourceManagerBuilder setStart(final XAException xe,
                final Answer<DtxResourceManagerContext> customAnswer) throws XAException {

            if (!setIdDone) {
                throw new XAException("ID was not set before setting start behavior");
            }

            // default behavior for null
            when(result.start(null)).thenThrow(new NullPointerException());

            if (xe != null) {
                doThrow(xe).when(result).start(argThat(new WrapMatcher<byte[]>(new InstanceOf(byte[].class))));
                setStartDone = true;
                return this;
            }
            // if no exception is given, set normal behavior
            final Answer<DtxResourceManagerContext> answer = (customAnswer != null) ? customAnswer
                    : new DefaultStartAnswer(result.getId());
            doAnswer(answer).when(result).start(eq(DEFAULT_PAYLOAD));
            doThrow(new XAException(XAException.XAER_INVAL)).when(result).start(argThat(notPayloadMatcher));
            setStartDone = true;
            return this;
        }

        /**
         * Defines {@link DtxResourceManager#prepare(DtxResourceManagerContext)} behavior.
         * 
         * @param xe
         *            a {@link XAException} to throw, default behavior is assumed when <code>null</code>
         * @param customAnswer
         *            a specific {@link Answer} to return on a perfectly valid call, or <code>null</code> for the
         *            default answer
         * @return the configured builder
         * @throws XAException
         *             if configuration fails
         */
        public final DtxResourceManagerBuilder setPrepare(final XAException xe, final Answer<Boolean> customAnswer)
                throws XAException {
            if (!setIdDone) {
                throw new XAException("ID was not set before setting prepare behavior");
            }

            // default behavior for null
            when(result.prepare(null)).thenThrow(new NullPointerException());

            if (xe != null) {
                doThrow(xe).when(result).prepare(argThat(anyCtxMatcher));
                setPrepareDone = true;
                return this;
            }

            final WrapMatcher<DtxResourceManagerContext> startedMatcher = new WrapMatcher<DtxResourceManagerContext>(
                    new And(Arrays.asList(new Matcher[] { this.ctxIdMatcher, new TxPositiveStateMatcher(STARTED) })));
            final TxNegativeStateMatcher noPrepareStateMatcher = new TxNegativeStateMatcher(STARTED);

            final Answer<Boolean> normalAnswer = (customAnswer != null) ? customAnswer : new DefaultPrepareAnswer();
            doAnswer(normalAnswer).when(result).prepare(argThat(startedMatcher));
            doThrow(new XAException(XAException.XAER_INVAL)).when(result).prepare(argThat(notFakeCtxMatcher));
            doThrow(new XAException(XAException.XAER_PROTO)).when(result).prepare(argThat(noPrepareStateMatcher));

            setPrepareDone = true;
            return this;
        }

        /**
         * Defines {@link DtxResourceManager#commit(DtxResourceManagerContext)} behavior.
         * 
         * @param xe
         *            a {@link XAException} to throw, default behavior is assumed when <code>null</code>
         * @param customAnswer
         *            a specific {@link Answer} to return on a perfectly valid call, or <code>null</code> for the
         *            default answer
         * @return the configured builder
         * @throws XAException
         *             if configuration fails
         */
        public final DtxResourceManagerBuilder setCommit(final XAException xe, final Answer<Void> customAnswer)
                throws XAException {

            if (!setIdDone) {
                throw new XAException("ID was not set before setting commit behavior");
            }

            // default behavior for null
            doThrow(new NullPointerException()).when(result).commit(null);

            if (xe != null) {
                doThrow(xe).when(result).commit(argThat(anyCtxMatcher));
                setCommitDone = true;
                return this;
            }

            final WrapMatcher<DtxResourceManagerContext> preparedMatcher = new WrapMatcher<DtxResourceManagerContext>(
                    new And(Arrays.asList(new Matcher[] { this.ctxIdMatcher, new TxPositiveStateMatcher(PREPARED) })));

            final TxNegativeStateMatcher noCommitStateMatcher = new TxNegativeStateMatcher(PREPARED);

            final Answer<Void> answer = (customAnswer != null) ? customAnswer : new DefaultCommitAnswer();
            doAnswer(answer).when(result).commit(argThat(preparedMatcher));
            doThrow(new XAException(XAException.XAER_INVAL)).when(result).commit(argThat(notFakeCtxMatcher));
            doThrow(new XAException(XAException.XAER_PROTO)).when(result).commit(argThat(noCommitStateMatcher));

            setCommitDone = true;
            return this;
        }

        /**
         * Defines {@link DtxResourceManager#rollback(DtxResourceManagerContext)} behavior.
         * 
         * @param xe
         *            a {@link XAException} to throw, default behavior is assumed when <code>null</code>
         * @param customAnswer
         *            a specific {@link Answer} to return on a perfectly valid call, or <code>null</code> for the
         *            default answer
         * @return the configured builder
         * @throws XAException
         *             if configuration fails
         */
        public final DtxResourceManagerBuilder setRollback(final XAException xe, final Answer<Void> customAnswer)
                throws XAException {
            if (!setIdDone) {
                throw new XAException("ID was not set before setting rollback behavior");
            }
            // default behavior for null
            doThrow(new NullPointerException()).when(result).rollback(null);

            if (xe != null) {
                doThrow(xe).when(result).rollback(argThat(anyCtxMatcher));
                setRollbackDone = true;
                return this;
            }

            final WrapMatcher<DtxResourceManagerContext> rollbackMatcher = new WrapMatcher<DtxResourceManagerContext>(
                    new And(Arrays.asList(new Matcher[] { this.ctxIdMatcher,
                            new TxPositiveStateMatcher(STARTED, PREPARED) })));

            final TxNegativeStateMatcher noRollbackStateMatcher = new TxNegativeStateMatcher(STARTED, PREPARED);

            final Answer<Void> answer = (customAnswer != null) ? customAnswer : new DefaultRollbackAnswer();
            doAnswer(answer).when(result).rollback(argThat(rollbackMatcher));
            doThrow(new XAException(XAException.XAER_INVAL)).when(result).rollback(argThat(notFakeCtxMatcher));
            doThrow(new XAException(XAException.XAER_PROTO)).when(result).rollback(argThat(noRollbackStateMatcher));

            setRollbackDone = true;
            return this;
        }

        /**
         * Defines {@link DtxResourceManager#rollback(DtxResourceManagerContext)} behavior.
         * 
         * @param e
         *            a {@link XAException} to throw, default behavior is assumed when <code>null</code>
         * @param customAnswer
         *            a specific {@link Answer} to return on a perfectly valid call, or <code>null</code> for the
         *            default answer
         * @return the configured builder
         * @throws Exception
         *             if configuration fails
         */
        public final DtxResourceManagerBuilder setPostSync(final Exception e, final Answer<Void> customAnswer)
                throws Exception {
            if (!setIdDone) {
                throw new XAException("ID was not set before setting rollback behavior");
            }

            // set behavior for exception case
            if (e != null) {
                doThrow(e).when(result).processPostSync();
                setPostSyncDone = true;
                return this;
            }

            if (customAnswer != null) {
                doAnswer(customAnswer).when(result).processPostSync();
            }
            else {
                doNothing().when(result).processPostSync();
            }
            setPostSyncDone = true;
            return this;
        }

        /**
         * Builds the configured {@link DtxResourceManager} instance.
         * 
         * @return a functional mock {@link DtxResourceManager}
         * @throws XAException
         *             if construction fails
         */
        public final DtxResourceManager build() throws XAException {
            if (!setIdDone) {
                setId(null);
            }
            if (!setStartDone) {
                setStart(null, null);
            }
            if (!setPrepareDone) {
                setPrepare(null, null);
            }
            if (!setCommitDone) {
                setCommit(null, null);
            }
            if (!setRollbackDone) {
                setRollback(null, null);
            }
            if (!setPostSyncDone) {
                try {
                    setPostSync(null, null);
                }
                catch (final Exception e) {
                    LOGGER.warn("Exception while setting up mock", e);
                }
            }
            return result;
        }
    }

    /**
     * {@link DtxResourceManagerContext} implementation to mimic a resource manager specific format.
     * 
     * 
     */
    private static class FakeDtxResourceManagerContext extends DtxResourceManagerContext {

        /**
         * Constructor adding the resource manager's ID and initial state to the context.
         * 
         * @param resourceManagerId
         *            the non-<code>null</code> {@link UUID} of the resource manager
         * @param status
         *            the {@link DtxTaskStatus} to set, will default to {@link DtxTaskStatus#PENDING} if given
         *            <code>null</code>
         * @throws NullPointerException
         *             if the resource manager ID parameter is <code>null</code>
         */
        protected FakeDtxResourceManagerContext(final UUID resourceManagerId, final DtxTaskStatus status)
                throws NullPointerException {
            super(resourceManagerId, status);
        }

    }
}
