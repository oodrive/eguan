package com.oodrive.nuage.utils;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class ByteBufferCache {

    // private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferCache.class);

    /**
     * Associate a queue of {@link ByteBuffer} and an exclusive lock.
     * 
     * 
     */
    private static final class ByteBufferStack {
        private final int capacity;
        private final KTypeArrayDeque<ByteBuffer> stack;
        private final Lock lock;
        private final int directBufferMinCapacity;
        private final ByteOrder byteOrder;

        /**
         * New instance.
         * 
         * @param capacity
         *            capacity of the {@link ByteBuffer}s.
         */
        ByteBufferStack(final int capacity, final int directBufferMinCapacity, final ByteOrder byteOrder) {
            super();
            this.capacity = capacity;
            this.stack = new KTypeArrayDeque<>();
            this.lock = new ReentrantLock();
            this.directBufferMinCapacity = directBufferMinCapacity;
            this.byteOrder = byteOrder;
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
            return (capacity >= directBufferMinCapacity ? ByteBuffer.allocateDirect(capacity) : ByteBuffer
                    .allocate(capacity)).order(byteOrder);
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
    private final int directBufferMinCapacity;
    private final ByteOrder byteOrder;

    private final ReadWriteLock buffersLock = new ReentrantReadWriteLock();
    @GuardedBy(value = "buffersLock")
    private final IntObjectOpenHashMap<ByteBufferStack> buffers = new IntObjectOpenHashMap<>();

    /** Singleton for an empty ByteBuffer */
    private static final ByteBuffer ZERO = ByteBuffer.allocate(0);

    /**
     * Create a new {@link ByteBuffer} cache. The smallest buffers are not direct.
     * 
     * @param directBufferMinCapacity
     *            minimal capacity of a direct buffer. Set 0 to allocate only direct buffers.
     */
    public ByteBufferCache(final int directBufferMinCapacity) {
        this(directBufferMinCapacity, ByteOrder.nativeOrder());
    }

    /**
     * Create a new {@link ByteBuffer} cache. The smallest buffers are not direct.
     * 
     * @param directBufferMinCapacity
     *            minimal capacity of a direct buffer. Set 0 to allocate only direct buffers.
     * @param byteOrder
     *            order of the created {@link ByteBuffer}.
     */
    public ByteBufferCache(final int directBufferMinCapacity, final ByteOrder byteOrder) {
        super();
        this.directBufferMinCapacity = directBufferMinCapacity;
        this.byteOrder = byteOrder;
    }

    /**
     * Allocate a new buffer
     * 
     * @param capacity
     * @return the new allocated or reused buffer. May not be filled with 0.
     */
    public final ByteBuffer allocate(final int capacity) {
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
                    stack = new ByteBufferStack(capacity, directBufferMinCapacity, byteOrder);
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
    public final void release(final ByteBuffer buffer) {
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
