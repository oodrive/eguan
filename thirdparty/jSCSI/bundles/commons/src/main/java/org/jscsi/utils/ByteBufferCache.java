package org.jscsi.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.KTypeArrayDeque;
import com.carrotsearch.hppc.predicates.KTypePredicate;

/**
 * Handles allocation and reuse of {@link ByteBuffer}. Allocates direct or non-direct {@link ByteBuffer}s depending on
 * the size of the buffer.
 * 
 * @author llambert
 * 
 */
public final class ByteBufferCache {

    // private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferCache.class);

    /**
     * Associate a queue of {@link ByteBuffer} and an exclusive lock.
     * 
     * @author llambert
     * 
     */
    private static final class ByteBufferStack {
        private final int capacity;
        private final KTypeArrayDeque<ByteBuffer> stack;
        private final Lock lock;

        /**
         * New instance.
         * 
         * @param capacity
         *            capacity of the {@link ByteBuffer}s.
         */
        ByteBufferStack(final int capacity) {
            super();
            this.capacity = capacity;
            this.stack = new KTypeArrayDeque<>();
            this.lock = new ReentrantLock();
        }

        /**
         * Gets an old buffer or create a new one.
         * 
         * @return a usage {@link ByteBuffer}
         */
        final ByteBuffer pop() {
            lock.lock();
            try {
                if (!stack.isEmpty()) {
                    // LOGGER.warn("Reuse    capacity=" + capacity);
                    final ByteBuffer result = (ByteBuffer) stack.removeFirst().clear();
                    return result;
                }
            }
            finally {
                lock.unlock();
            }
            // LOGGER.warn("Allocate capacity=" + capacity);
            return capacity >= DIRECT_BUFFER_MIN_CAPACITY ? ByteBuffer.allocateDirect(capacity) : ByteBuffer
                    .allocate(capacity);
        }

        final void push(final ByteBuffer buffer) {
            lock.lock();
            try {
                assert !containsObject(buffer);

                stack.addLast(buffer);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Check if the stack contains the buffer. <code>Do NOT</code> call <code>stack.contains()</code> which compares
         * the contents of the buffers.
         * 
         * @param buffer
         * @return true if stack contains the buffer
         */
        private final boolean containsObject(final ByteBuffer buffer) {
            final AtomicBoolean result = new AtomicBoolean();
            stack.forEach(new KTypePredicate<ByteBuffer>() {

                @Override
                public final boolean apply(final ByteBuffer value) {
                    if (value == buffer) {
                        result.set(true);
                        return false;
                    }
                    return true;
                }
            });
            return result.get();
        }
    }

    // Allocate byte arrays for small buffers
    private static final int DIRECT_BUFFER_MIN_CAPACITY = 4 * 1024;

    private static ReadWriteLock buffersLock = new ReentrantReadWriteLock();
    @GuardedBy(value = "buffersLock")
    private static IntObjectOpenHashMap<ByteBufferStack> buffers = new IntObjectOpenHashMap<>();
    static {
        // Most requested sizes
        buffers.put(48, new ByteBufferStack(48));
        buffers.put(16, new ByteBufferStack(16));
    }

    /**
     * No instance.
     */
    private ByteBufferCache() {
        throw new Error();
    }

    /** Singleton for an empty ByteBuffer */
    private static final ByteBuffer ZERO = ByteBuffer.allocate(0);

    /**
     * Allocate a new buffer and release the given one.
     * 
     * @param releaseBuffer
     *            buffer to release
     * @param capacity
     *            capacity of the buffer to allocate
     * @return the new allocated or reused buffer. May not be filled with 0.
     */
    public static final ByteBuffer allocate(final ByteBuffer releaseBuffer, final int capacity) {
        release(releaseBuffer);
        return allocate(capacity);
    }

    /**
     * Allocate a new buffer
     * 
     * @param capacity
     * @return the new allocated or reused buffer. May not be filled with 0.
     */
    public static final ByteBuffer allocate(final int capacity) {
        if (capacity == 0) {
            return ZERO;
        }

        // Look for the associated stack
        ByteBufferStack stack;
        buffersLock.readLock().lock();
        try {
            stack = buffers.get(capacity);
        }
        finally {
            buffersLock.readLock().unlock();
        }

        if (stack == null) {
            // Need to allocate a new stack
            buffersLock.writeLock().lock();
            try {
                stack = buffers.get(capacity);
                if (stack == null) {
                    stack = new ByteBufferStack(capacity);
                    buffers.put(capacity, stack);
                }
            }
            finally {
                buffersLock.writeLock().unlock();
            }
        }

        return stack.pop();
    }

    /**
     * Release a buffer and make it available for reuse.
     * 
     * @param buffer
     *            buffer to release
     */
    public static final void release(final ByteBuffer buffer) {
        if (buffer != null && buffer != ZERO) {
            final int capacity = buffer.capacity();
            // LOGGER.warn("Release  capacity=" + capacity);

            final ByteBufferStack stack;
            buffersLock.readLock().lock();
            try {
                stack = buffers.get(capacity);
            }
            finally {
                buffersLock.readLock().unlock();
            }

            assert stack != null;
            stack.push(buffer);
        }
    }
}
