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

import static com.oodrive.nuage.dtx.DtxTaskStatus.COMMITTED;
import static com.oodrive.nuage.dtx.DtxTaskStatus.PENDING;
import static com.oodrive.nuage.dtx.DtxTaskStatus.PREPARED;
import static com.oodrive.nuage.dtx.DtxTaskStatus.ROLLED_BACK;
import static com.oodrive.nuage.dtx.DtxTaskStatus.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Random;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;

import org.junit.Test;

/**
 * Abstract superclass for all implementations of {@link DtxResourceManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
// TODO: parameterize the test with respect to context states triggering protocol errors
public abstract class TestDtxResourceManager {

    private static final int BAD_PAYLOAD_LENGTH = 384;

    /**
     * Gets an instance of the concrete implementation to test.
     * 
     * @return a functional {@link DtxResourceManager} instance
     * @throws XAException
     *             in case resource manager construction fails
     */
    @Nonnull
    protected abstract DtxResourceManager getResourceManagerInstance() throws XAException;

    /**
     * Gets the opaque binary payload to include as operation in the {@link DtxResourceManager#start(byte[])} call.
     * 
     * @return a valid byte array payload
     */
    @Nonnull
    protected abstract byte[] getPayload();

    /**
     * Tests the {@link DtxResourceManager#start(byte[])} method.
     * 
     * Failure due to a <code>null</code> payload must be attributed to the concrete test's implementation of
     * {@link #getPayload()}.
     * 
     * @throws NullPointerException
     *             if the payload is null, not part of this test.
     * @throws XAException
     *             not part of this test
     */
    @Test
    public final void testStart() throws NullPointerException, XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());
    }

    /**
     * Tests the {@link DtxResourceManager#start(byte[])} method's failure due to a <code>null</code> payload.
     * 
     * @throws NullPointerException
     *             expected for this test
     * @throws XAException
     *             not part of this test
     */
    @Test(expected = NullPointerException.class)
    public final void testStartFailPayloadNull() throws NullPointerException, XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        target.start(null);
    }

    /**
     * Tests the {@link DtxResourceManager#start(byte[])} method's failure due to a bad payload made of random bytes.
     * 
     * @throws NullPointerException
     *             not part of this test
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testStartFailInvalidPayload() throws NullPointerException, XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final Random rnd = new Random(System.currentTimeMillis());
        final byte[] fakePayload = new byte[rnd.nextInt(BAD_PAYLOAD_LENGTH) + 1];
        rnd.nextBytes(fakePayload);

        try {
            target.start(fakePayload);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_INVAL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method.
     * 
     * @throws XAException
     *             if the prepare operation fails, not part of this test
     */
    @Test
    public final void testPrepare() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        assertEquals(Boolean.TRUE, target.prepare(ctx));
        assertEquals(DtxTaskStatus.PREPARED, ctx.getTxStatus());
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method's failure due to an invalid
     * context argument.
     * 
     * @throws XAException
     *             if the prepare operation fails, not part of this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailBadContext() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        final DtxResourceManagerContext badCtx = new DtxResourceManagerContext(UUID.randomUUID(), STARTED) {
        };

        try {
            target.prepare(badCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_INVAL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#PENDING} status of the context.
     * 
     * @throws XAException
     *             if the prepare operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailPending() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext pendingCtx = new DtxResourceManagerContext(target.getId(), PENDING) {
        };

        try {
            target.prepare(pendingCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#PREPARED} status of the context.
     * 
     * @throws XAException
     *             if the prepare operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailPrepared() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext preparedCtx = new DtxResourceManagerContext(target.getId(), PREPARED) {
        };

        try {
            target.prepare(preparedCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#COMMITTED} status of the context.
     * 
     * @throws XAException
     *             if the prepare operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailCommitted() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext committedCtx = new DtxResourceManagerContext(target.getId(), COMMITTED) {
        };

        try {
            target.prepare(committedCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#prepare(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#ROLLED_BACK} status of the context.
     * 
     * @throws XAException
     *             if the prepare operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testPrepareFailRolledBack() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        target.rollback(ctx);
        assertEquals(DtxTaskStatus.ROLLED_BACK, ctx.getTxStatus());
        try {
            target.prepare(ctx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method.
     * 
     * @throws XAException
     *             if the commit operation fails, not part of this test
     */
    @Test
    public final void testCommit() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        target.prepare(ctx);
        assertEquals(DtxTaskStatus.PREPARED, ctx.getTxStatus());

        target.commit(ctx);
        assertEquals(DtxTaskStatus.COMMITTED, ctx.getTxStatus());
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to an invalid
     * context.
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailBadContext() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext badCtx = new DtxResourceManagerContext(UUID.randomUUID(), PREPARED) {
        };
        try {
            target.commit(badCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_INVAL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#PENDING} status of the context.
     * 
     * @throws XAException
     *             if the commit operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailPending() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext pendingCtx = new DtxResourceManagerContext(target.getId(), PENDING) {
        };

        try {
            target.commit(pendingCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#STARTED} status of the context.
     * 
     * @throws XAException
     *             if the commit operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailStarted() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext startedCtx = new DtxResourceManagerContext(target.getId(), STARTED) {
        };

        try {
            target.commit(startedCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#STARTED} status of the context.
     * 
     * @throws XAException
     *             if the commit operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailCommitted() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext committedCtx = new DtxResourceManagerContext(target.getId(), COMMITTED) {
        };

        try {
            target.commit(committedCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to the
     * {@link DtxTaskStatus#ROLLED_BACK} status of the context.
     * 
     * @throws XAException
     *             if the commit operation fails, expected for this test
     */
    @Test(expected = XAException.class)
    public final void testCommitFailRolledBack() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        target.rollback(ctx);
        assertEquals(DtxTaskStatus.ROLLED_BACK, ctx.getTxStatus());
        try {
            target.commit(ctx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method on a started context.
     * 
     * @throws XAException
     *             if the rollback operation fails, not part of this test
     */
    @Test
    public final void testRollbackStarted() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext ctx = target.start(getPayload());

        target.rollback(ctx);
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method on a prepared context.
     * 
     * @throws XAException
     *             if the roll-back operation fails, not part of this test
     */
    @Test
    public final void testRollbackPrepared() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();

        final DtxResourceManagerContext ctx = target.start(getPayload());
        assertNotNull(ctx);
        assertEquals(DtxTaskStatus.STARTED, ctx.getTxStatus());

        assertEquals(Boolean.TRUE, target.prepare(ctx));
        assertEquals(DtxTaskStatus.PREPARED, ctx.getTxStatus());

        target.rollback(ctx);
        assertEquals(DtxTaskStatus.ROLLED_BACK, ctx.getTxStatus());
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to a context in
     * {@link DtxTaskStatus#PENDING} state.
     * 
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testRollbackFailPending() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext pendingCtx = new DtxResourceManagerContext(target.getId(), PENDING) {
        };
        try {
            target.rollback(pendingCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to an invalid
     * context.
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testRollbackFailBadContext() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext badCtx = new DtxResourceManagerContext(UUID.randomUUID(), PREPARED) {
        };
        try {
            target.rollback(badCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_INVAL, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to a context in
     * {@link DtxTaskStatus#COMMITTED} state.
     * 
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testRollbackFailCommitted() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext committedCtx = new DtxResourceManagerContext(target.getId(), COMMITTED) {
        };
        try {
            target.rollback(committedCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

    /**
     * Tests the {@link DtxResourceManager#commit(DtxResourceManagerContext)} method's failure due to a context in
     * {@link DtxTaskStatus#ROLLED_BACK} state.
     * 
     * 
     * @throws XAException
     *             expected for this test
     */
    @Test(expected = XAException.class)
    public final void testRollbackFailRolledBack() throws XAException {
        final DtxResourceManager target = getResourceManagerInstance();
        final DtxResourceManagerContext rolledbackCtx = new DtxResourceManagerContext(target.getId(), ROLLED_BACK) {
        };
        try {
            target.rollback(rolledbackCtx);
        }
        catch (final XAException xe) {
            assertEquals(XAException.XAER_PROTO, xe.errorCode);
            throw xe;
        }
    }

}
