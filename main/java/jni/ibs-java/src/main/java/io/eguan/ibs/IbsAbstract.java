package io.eguan.ibs;

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

import java.nio.ByteBuffer;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

/**
 * Common code between different implementations of {@link Ibs}.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
abstract class IbsAbstract implements Ibs {
    /** Path of the IBS. For {@link #toString()} and for lock purpose (lock out of the caller scope) */
    protected final String ibsPath;
    /** true when the IBS have been closed */
    @GuardedBy(value = "ibsPath")
    protected volatile boolean closed;
    /** true when the IBS is started */
    @GuardedBy(value = "ibsPath")
    protected volatile boolean started;
    /** true when the destruction of the IBS is in progress */
    @GuardedBy(value = "ibsPath")
    protected volatile boolean destroying;

    protected IbsAbstract(final String ibsPath) {
        super();
        this.ibsPath = ibsPath;
        this.started = false;
        this.closed = false;
    }

    @Override
    public final void start() throws IllegalStateException, IbsException {
        synchronized (ibsPath) {
            if (started || closed || destroying) {
                // Already started, closed or destroy in progress
                throw new IllegalStateException(toString());
            }
            final int retval;
            if ((retval = doStart()) != 0) {
                throw new IbsException(toString(), IbsErrorCode.valueOf(retval));
            }
            started = true;
        }
    }

    @Override
    public final void stop() throws IbsException {
        synchronized (ibsPath) {
            if (!started || closed) {
                // Already started or closed: ignore silently
                return;
            }
            final int retval;
            if ((retval = doStop()) != 0) {
                throw new IbsException(toString(), IbsErrorCode.valueOf(retval));
            }
            started = false;
        }
    }

    @Override
    public final boolean isStarted() {
        // No need to sync here (volatile)
        return started;
    }

    @Override
    public final void close() {
        synchronized (ibsPath) {
            if (!closed) {
                // Make sure the IBS is closed
                try {
                    stop();
                }
                catch (final Throwable t) {
                    // Force started to false (will be closed soon)
                    logger.warn("Failed to stop " + this, t);
                    started = false;
                }

                // Release native resources
                try {
                    doClose();
                }
                catch (final Throwable t) {
                    logger.warn("Failed to close " + this, t);
                }
                closed = true;
            }
        }
    }

    @Override
    public final boolean isClosed() {
        // No need to sync here (volatile)
        return closed;
    }

    @Override
    public final void destroy() throws IbsIOException {
        synchronized (ibsPath) {
            if (destroying) {
                return;
            }
            // Try to stop the IBS before destroy
            try {
                stop();
            }
            catch (final Throwable t) {
                // Force started to false (will be destroyed soon)
                logger.warn("Failed to stop " + this, t);
                started = false;
            }
            // Forbid new start
            destroying = true;
        }
        // Release native resources, not under lock (long operation)
        final int retval;
        if ((retval = doDestroy()) != 0) {
            throw new IbsIOException(toString(), IbsErrorCode.valueOf(retval));
        }
        // Closed by doDestroy()
        synchronized (ibsPath) {
            closed = true;
        }
    }

    @Override
    public final ByteBuffer get(@Nonnull final byte[] key, @Nonnegative final int length, final boolean allocateDirect)
            throws IbsException, IbsIOException, IbsBufferTooSmallException, NullPointerException {
        // Allocate buffer
        final ByteBuffer buffer;
        if (allocateDirect) {
            buffer = ByteBuffer.allocateDirect(length);
        }
        else {
            buffer = ByteBuffer.allocate(length);
        }

        // Get data
        get(key, buffer);
        return buffer;
    }

    @Override
    public final void get(@Nonnull final byte[] key, @Nonnull final ByteBuffer data) throws IbsException,
            IbsIOException, IbsBufferTooSmallException, NullPointerException {
        final int readLength = get(key, data, data.position(), data.remaining());
        data.position(data.position() + readLength);
    }

    /**
     * Real start.
     * 
     * @return 0 or an IbsErrorCode
     */
    protected abstract int doStart();

    /**
     * Real stop.
     * 
     * @return 0 or an IbsErrorCode
     */
    protected abstract int doStop();

    /**
     * Real close.
     * 
     * @return 0 or an IbsErrorCode
     */
    protected abstract int doClose();

    /**
     * Real destroy.
     * 
     * @return 0 or an IbsErrorCode
     */
    protected abstract int doDestroy();

    /**
     * Utility method to check parameters.
     * 
     * @param key
     * @param data
     * @param offset
     * @param length
     */
    protected final void checkArgs(final byte[] key, final ByteBuffer data, final int offset, final int length) {
        // Check references: key is enough, data is accessed
        Objects.requireNonNull(key);

        if (offset < 0 || length < 1) {
            throw new IllegalArgumentException();
        }
        if (data.capacity() < (offset + length)) {
            throw new IndexOutOfBoundsException();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#finalize()
     */
    @Override
    protected final void finalize() throws Throwable {
        // Try to close the IBS
        try {
            close();
        }
        finally {
            super.finalize();
        }
    }

}
