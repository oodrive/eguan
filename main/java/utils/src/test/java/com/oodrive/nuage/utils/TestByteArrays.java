package com.oodrive.nuage.utils;

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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests of {@link ByteArrays}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class TestByteArrays {

    @Test(expected = NullPointerException.class)
    public void testHexNull() {
        ByteArrays.toHex(null);
    }

    @Test
    public void testHexEmpty() {
        Assert.assertEquals("", ByteArrays.toHex(new byte[0]));
    }

    @Test
    public void testHex() {
        Assert.assertEquals(
                "03214FCAB5D698E752",
                ByteArrays.toHex(new byte[] { 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                        (byte) 0xE7, 0x52 }));
    }

    @Test(expected = NullPointerException.class)
    public void testFillByteBufferNullIn() {
        ByteArrays.fillArray(null, new byte[12], 0);
    }

    @Test(expected = NullPointerException.class)
    public void testFillByteBufferNullOut() {
        ByteArrays.fillArray(ByteBuffer.wrap(new byte[12]), null, 0);
    }

    @Test
    public void testFillByteBuffer() {
        final byte[] buffer = new byte[] { 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                (byte) 0xE7, 0x52 };
        final byte[] out = new byte[9];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        ByteArrays.fillArray(byteBuffer, out, 0);
        ByteArrays.assertEqualsByteArrays(buffer, out);
        Assert.assertEquals(9, byteBuffer.position());
    }

    @Test
    public void testFillByteBufferOffset() {
        final byte[] buffer = new byte[] { 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                (byte) 0xE7, 0x52 };
        final byte[] res = new byte[] { 0, 0, 0, 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                (byte) 0xE7, 0x52, 0 };
        final byte[] out = new byte[13];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        ByteArrays.fillArray(byteBuffer, out, 3);
        ByteArrays.assertEqualsByteArrays(res, out);
        Assert.assertEquals(9, byteBuffer.position());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testFillByteBufferShort() {
        final byte[] buffer = new byte[] { 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                (byte) 0xE7, 0x52 };
        final byte[] out = new byte[7];
        ByteArrays.fillArray(ByteBuffer.wrap(buffer), out, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testFillByteBufferOffsetShort() {
        final byte[] buffer = new byte[] { 3, 0x21, 0x4F, (byte) 0xCA, (byte) 0xB5, (byte) 0xD6, (byte) 0x98,
                (byte) 0xE7, 0x52 };
        final byte[] out = new byte[11];
        ByteArrays.fillArray(ByteBuffer.wrap(buffer), out, 3);
    }

    @Test
    public void testBothNull() {
        ByteArrays.assertEqualsByteArrays(null, null);
    }

    @Test(expected = AssertionError.class)
    public void testNull1() {
        ByteArrays.assertEqualsByteArrays(null, new byte[2]);
    }

    @Test(expected = AssertionError.class)
    public void testNull2() {
        ByteArrays.assertEqualsByteArrays(new byte[2], null);
    }

    @Test(expected = AssertionError.class)
    public void testWrongLength() {
        ByteArrays.assertEqualsByteArrays(new byte[2], new byte[3]);
    }

    @Test(expected = AssertionError.class)
    public void testNotEquals() {
        ByteArrays.assertEqualsByteArrays(new byte[] { 2, 3, 4, 5, 6 }, new byte[] { 2, 3, 45, 5, 6 });
    }

    @Test
    public void testEquals() {
        ByteArrays.assertEqualsByteArrays(new byte[] { 2, 3, 4, 5, 6 }, new byte[] { 2, 3, 4, 5, 6 });
    }
}
