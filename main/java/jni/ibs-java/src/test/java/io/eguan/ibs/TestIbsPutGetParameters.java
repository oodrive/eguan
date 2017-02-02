package io.eguan.ibs;

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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;

/**
 * Check the parameters of the get, del, put and replace calls.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
@RunWith(value = Parameterized.class)
public class TestIbsPutGetParameters extends TestIbsClassStarted {

    private final static Random rand = new Random();

    /**
     * Create a new random key.
     * 
     * @return a new random key.
     */
    private static final byte[] newRandowKey() {
        final byte[] key = new byte[16];
        rand.nextBytes(key);
        return key;
    }

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB }, { IbsType.FS }, { IbsType.FAKE } };
        return Arrays.asList(data);
    }

    public TestIbsPutGetParameters(final IbsType ibsType) {
        super(ibsType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidOffset() throws IbsIOException {
        ibs.get(new byte[2], ByteBuffer.wrap(new byte[3]), -1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidLength0() throws IbsIOException {
        ibs.get(new byte[2], ByteBuffer.wrap(new byte[3]), 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getInvalidLengthNegative() throws IbsIOException {
        ibs.get(new byte[2], ByteBuffer.wrap(new byte[3]), 0, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidOffset() throws IbsIOException {
        ibs.put(new byte[2], ByteBuffer.wrap(new byte[3]), -1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidLength0() throws IbsIOException {
        ibs.put(new byte[2], ByteBuffer.wrap(new byte[3]), 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidLengthNegative() throws IbsIOException {
        ibs.put(new byte[2], ByteBuffer.wrap(new byte[3]), 0, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidOffset() throws IbsIOException {
        ibs.replace(new byte[2], new byte[3], ByteBuffer.wrap(new byte[3]), -1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidLength0() throws IbsIOException {
        ibs.replace(new byte[2], new byte[3], ByteBuffer.wrap(new byte[3]), 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidLengthNegative() throws IbsIOException {
        ibs.replace(new byte[2], new byte[3], ByteBuffer.wrap(new byte[3]), 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void getInvalidData() throws IbsIOException {
        ibs.get(new byte[2], ByteBuffer.wrap(new byte[3]), 2, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putInvalidData() throws IbsIOException {
        ibs.put(new byte[2], ByteBuffer.wrap(new byte[3]), 2, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void replaceInvalidData() throws IbsIOException {
        ibs.replace(new byte[2], new byte[3], ByteBuffer.wrap(new byte[3]), 2, 2);
    }

    @Test(expected = NullPointerException.class)
    public void get1NullKey() throws IbsIOException {
        ibs.get(null, 1, false);
    }

    @Test(expected = NullPointerException.class)
    public void get2NullKey() throws IbsIOException {
        ibs.get(null, ByteBuffer.wrap(new byte[3]));
    }

    @Test(expected = NullPointerException.class)
    public void get3NullKey() throws IbsIOException {
        ibs.get(null, ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void get2NullData() throws IbsIOException {
        ibs.get(new byte[3], null);
    }

    @Test(expected = NullPointerException.class)
    public void get3NullData() throws IbsIOException {
        ibs.get(new byte[3], null, 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void del1NullKey() throws IbsIOException {
        ibs.del(null);
    }

    @Test(expected = NullPointerException.class)
    public void del2NullKey() throws IbsIOException {
        ibs.del(null);
    }

    @Test(expected = NullPointerException.class)
    public void put1NullKey() throws IbsIOException {
        ibs.put(null, ByteBuffer.wrap(new byte[3]));
    }

    @Test(expected = NullPointerException.class)
    public void put2NullKey() throws IbsIOException {
        ibs.put(null, ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test
    public void put1NullDataRefresh() throws IbsIOException {
        final byte[] key = newRandowKey();
        final ByteBuffer value = ByteBuffer.wrap(new byte[3]);
        Assert.assertTrue(ibs.put(key, value));
        Assert.assertFalse(ibs.put(key, (ByteBuffer) null));
    }

    @Test
    public void put1NullData() throws IbsIOException {
        try {
            ibs.put(newRandowKey(), (ByteBuffer) null);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
        }
    }

    @Test(expected = NullPointerException.class)
    public void put2NullData() throws IbsIOException {
        ibs.put(new byte[3], null, 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void putByteStrNullKey() throws IbsIOException {
        ibs.put(null, ByteString.copyFrom(new byte[3]));
    }

    @Test(expected = NullPointerException.class)
    public void putByteStrNullData() throws IbsIOException {
        ibs.put(new byte[3], (ByteString) null);
    }

    @Test(expected = NullPointerException.class)
    public void replace1NullOldKey() throws IbsIOException {
        ibs.replace(null, new byte[3], ByteBuffer.wrap(new byte[3]));
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullOldKey() throws IbsIOException {
        ibs.replace(null, new byte[3], ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void replace1NullNewKey() throws IbsIOException {
        ibs.replace(new byte[3], null, ByteBuffer.wrap(new byte[3]));
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullNewKey() throws IbsIOException {
        ibs.replace(new byte[3], null, ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test
    public void replace1NullDataRefresh() throws IbsIOException {
        final byte[] key = newRandowKey();
        final ByteBuffer value = ByteBuffer.wrap(new byte[3]);
        Assert.assertTrue(ibs.put(key, value));
        Assert.assertFalse(ibs.replace(newRandowKey(), key, null));
    }

    @Test
    public void replace1NullData() throws IbsIOException {
        try {
            ibs.replace(newRandowKey(), newRandowKey(), null);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.NOT_FOUND, e.getErrorCode());
        }
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullData() throws IbsIOException {
        ibs.replace(new byte[3], new byte[3], null, 1, 2);
    }

}
