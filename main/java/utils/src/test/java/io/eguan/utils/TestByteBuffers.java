package io.eguan.utils;

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

import io.eguan.utils.ByteBuffers;

import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests on {@link ByteBuffers}.
 * 
 * @author oodrive
 * @author llambert
 */
public class TestByteBuffers {

    @Test
    public void testEmptyArrayBuffers() {
        testEmptyBuffers(ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testEmptyByteBuffers() {
        testEmptyBuffers(ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    private void testEmptyBuffers(final ByteBuffers.ByteBufferFactory factory) {

        // New buffer empty
        final int capacity = 55;
        final ByteBuffer empty = factory.newByteBuffer(capacity);
        Assert.assertEquals(capacity, empty.capacity());
        Assert.assertEquals(0, empty.position());
        for (int i = 0; i < capacity; i++) {
            Assert.assertEquals(0, empty.get());
        }
        Assert.assertEquals(capacity, empty.position());

    }

    @Test
    public void testFilledArrayBuffers() {
        testFilledBuffers(ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testFilledByteBuffers() {
        testFilledBuffers(ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    private void testFilledBuffers(final ByteBuffers.ByteBufferFactory factory) {

        // New buffer filled with random contents
        final int capacity = 55;
        final byte[] contents = new byte[capacity];
        final Random random = new Random();
        random.nextBytes(contents);
        final ByteBuffer filled = factory.newByteBuffer(contents);
        Assert.assertEquals(capacity, filled.capacity());
        Assert.assertEquals(0, filled.position());
        for (int i = 0; i < capacity; i++) {
            Assert.assertEquals(contents[i], filled.get());
        }
        Assert.assertEquals(capacity, filled.position());
        filled.rewind();

        // Check defensive copy
        if (contents[0] == Byte.MIN_VALUE) {
            contents[0] = Byte.MAX_VALUE;
        }
        else {
            contents[0]--;
        }
        Assert.assertFalse(contents[0] == filled.get());
    }

    @Test
    public void testCompareArrayBuffers() {
        testCompareBuffers(ByteBuffers.FACTORY_BYTE_ARRAY);
    }

    @Test
    public void testCompareByteBuffers() {
        testCompareBuffers(ByteBuffers.FACTORY_BYTE_DIRECT);
    }

    private void testCompareBuffers(final ByteBuffers.ByteBufferFactory factory) {

        // New buffer filled with random contents
        final int capacity = 505;
        final byte[] contents = new byte[capacity];
        final Random random = new Random();
        random.nextBytes(contents);
        final ByteBuffer toCompare1 = factory.newByteBuffer(contents);
        {
            final ByteBuffer toCompare2 = ByteBuffer.wrap(contents);

            // Position is 0
            Assert.assertEquals(0, toCompare1.position());
            Assert.assertEquals(0, toCompare2.position());
            ByteBuffers.assertEqualsByteBuffers(toCompare1, toCompare2);

            // Change one position: should fail
            toCompare1.position(capacity);
            try {
                ByteBuffers.assertEqualsByteBuffers(toCompare1, toCompare2);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final AssertionError e) {
                // OK
            }
            Assert.assertEquals(capacity, toCompare1.position());
            Assert.assertEquals(0, toCompare2.position());

            // Position is capacity: really compares contents
            toCompare2.position(capacity);
            ByteBuffers.assertEqualsByteBuffers(toCompare1, toCompare2);
            Assert.assertEquals(capacity, toCompare1.position());
            Assert.assertEquals(capacity, toCompare2.position());

            // Change contents: comparison should fail
            if (contents[0] == Byte.MIN_VALUE) {
                contents[0] = Byte.MAX_VALUE;
            }
            else {
                contents[0]--;
            }
            try {
                ByteBuffers.assertEqualsByteBuffers(toCompare1, toCompare2);
                throw new AssertionFailedError("Should not be reached");
            }
            catch (final AssertionError e) {
                // OK
            }
            Assert.assertEquals(1, toCompare1.position());
            Assert.assertEquals(1, toCompare2.position());
        }

        // Compares with offset
        {
            final byte[] contents2 = new byte[capacity - 1];
            System.arraycopy(contents, 1, contents2, 0, contents2.length);
            final ByteBuffer toCompare2 = ByteBuffer.wrap(contents2);

            Assert.assertEquals(0, toCompare2.position());

            // Position is capacity: really compares contents
            toCompare1.position(capacity);
            toCompare2.position(toCompare2.capacity());
            ByteBuffers.assertEqualsByteBuffers(toCompare2, toCompare1, 1);
            Assert.assertEquals(capacity, toCompare1.position());
            Assert.assertEquals(capacity - 1, toCompare2.position());
        }
    }
}
