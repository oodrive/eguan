package io.eguan.utils;

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

import io.eguan.utils.ByteBufferCache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ByteBufferCache.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class TestByteBufferCache {

    @Test
    public void testSingleton0() {
        final ByteBufferCache cache1 = new ByteBufferCache(11);
        final ByteBufferCache cache2 = new ByteBufferCache(12);
        final ByteBuffer zero11 = cache1.allocate(0);
        final ByteBuffer zero12 = cache1.allocate(0);
        final ByteBuffer zero2 = cache2.allocate(0);
        Assert.assertSame(zero11, zero12);
        Assert.assertSame(zero11, zero2);

        // Can release the singleton in any cache
        cache1.release(zero11);
        cache2.release(zero12);
    }

    @Test
    public void testReuse() {
        final ByteBufferCache cache = new ByteBufferCache(11);

        // Reuse non direct buffers
        {
            final ByteBuffer buf1 = cache.allocate(10);
            Assert.assertFalse(buf1.isDirect());
            final ByteBuffer buf2 = cache.allocate(10);
            Assert.assertFalse(buf2.isDirect());
            Assert.assertNotSame(buf1, buf2);

            cache.release(buf1);
            final ByteBuffer buf3 = cache.allocate(10);
            Assert.assertFalse(buf3.isDirect());
            Assert.assertSame(buf1, buf3);
        }

        // Reuse direct buffers
        {
            final ByteBuffer buf1 = cache.allocate(11);
            Assert.assertTrue(buf1.isDirect());
            final ByteBuffer buf2 = cache.allocate(11);
            Assert.assertTrue(buf2.isDirect());
            Assert.assertNotSame(buf1, buf2);

            cache.release(buf1);
            final ByteBuffer buf3 = cache.allocate(11);
            Assert.assertTrue(buf3.isDirect());
            Assert.assertSame(buf1, buf3);
        }
    }

    /**
     * Release <code>null</code>: does nothing.
     */
    @Test
    public void testReleaseNull() {
        final ByteBufferCache cache = new ByteBufferCache(11);
        cache.release(null);
    }

    @Test
    public void testBigEndianOrder() {
        final ByteBufferCache cache = new ByteBufferCache(11, ByteOrder.BIG_ENDIAN);
        // Read integer from a heap buffer
        {
            final ByteBuffer buffer = cache.allocate(4);
            Assert.assertFalse(buffer.isDirect());
            buffer.put((byte) 1).rewind();
            final int read = buffer.getInt();
            Assert.assertEquals(0x1000000, read);
        }
        // Read integer from a direct buffer
        {
            final ByteBuffer buffer = cache.allocate(40);
            Assert.assertTrue(buffer.isDirect());
            buffer.put((byte) 1).rewind();
            final int read = buffer.getInt();
            Assert.assertEquals(0x1000000, read);
        }
    }

    @Test
    public void testLittleEndianOrder() {
        final ByteBufferCache cache = new ByteBufferCache(11, ByteOrder.LITTLE_ENDIAN);
        // Read integer from a heap buffer
        {
            final ByteBuffer buffer = cache.allocate(4);
            Assert.assertFalse(buffer.isDirect());
            buffer.put((byte) 1).rewind();
            final int read = buffer.getInt();
            Assert.assertEquals(0x01, read);
        }
        // Read integer from a direct buffer
        {
            final ByteBuffer buffer = cache.allocate(40);
            Assert.assertTrue(buffer.isDirect());
            buffer.put((byte) 1).rewind();
            final int read = buffer.getInt();
            Assert.assertEquals(0x01, read);
        }
    }
}
