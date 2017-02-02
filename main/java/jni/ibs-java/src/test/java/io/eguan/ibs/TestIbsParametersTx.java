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

import junit.framework.AssertionFailedError;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Check the parameters of the put, replace and other transaction calls.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 */
@RunWith(value = Parameterized.class)
public class TestIbsParametersTx extends TestIbsClassStarted {

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB }, { IbsType.FS }, { IbsType.FAKE } };
        return Arrays.asList(data);
    }

    public TestIbsParametersTx(final IbsType ibsType) {
        super(ibsType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidTxId1() throws IbsIOException {
        ibs.put(-3, new byte[2], ByteBuffer.wrap(new byte[3]), 0, 2);
    }

    @Test
    public void putInvalidTxId2() {
        try {
            ibs.put(3, new byte[9], ByteBuffer.wrap(new byte[3]), 0, 2);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.INVALID_TRANSACTION_ID, e.getErrorCode());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidOffset() throws IbsIOException {
        ibs.put(3, new byte[9], ByteBuffer.wrap(new byte[3]), -1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidLength0() throws IbsIOException {
        ibs.put(3, new byte[9], ByteBuffer.wrap(new byte[3]), 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putInvalidLengthNegative() throws IbsIOException {
        ibs.put(3, new byte[9], ByteBuffer.wrap(new byte[3]), 0, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidTx1() throws IbsIOException {
        ibs.replace(-3, new byte[9], new byte[3], ByteBuffer.wrap(new byte[3]), 0, 2);
    }

    @Test
    public void replaceInvalidTxId2() {
        try {
            ibs.replace(3, new byte[9], new byte[11], ByteBuffer.wrap(new byte[3]), 0, 2);
            throw new AssertionFailedError("Should not be reached");
        }
        catch (final IbsIOException e) {
            Assert.assertEquals(IbsErrorCode.INVALID_TRANSACTION_ID, e.getErrorCode());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidOffset() throws IbsIOException {
        ibs.replace(3, new byte[9], new byte[3], ByteBuffer.wrap(new byte[3]), -1, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidLength0() throws IbsIOException {
        ibs.replace(3, new byte[9], new byte[3], ByteBuffer.wrap(new byte[3]), 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void replaceInvalidLengthNegative() throws IbsIOException {
        ibs.replace(3, new byte[9], new byte[3], ByteBuffer.wrap(new byte[3]), 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void putInvalidData() throws IbsIOException {
        ibs.put(3, new byte[9], ByteBuffer.wrap(new byte[3]), 2, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void replaceInvalidData() throws IbsIOException {
        ibs.replace(3, new byte[9], new byte[3], ByteBuffer.wrap(new byte[3]), 2, 2);
    }

    @Test(expected = NullPointerException.class)
    public void put2NullKey() throws IbsIOException {
        ibs.put(3, null, ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void put2NullData() throws IbsIOException {
        ibs.put(3, new byte[3], null, 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullOldKey() throws IbsIOException {
        ibs.replace(3, null, new byte[3], ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullNewKey() throws IbsIOException {
        ibs.replace(3, new byte[3], null, ByteBuffer.wrap(new byte[3]), 1, 2);
    }

    @Test(expected = NullPointerException.class)
    public void replace2NullData() throws IbsIOException {
        ibs.replace(3, new byte[3], new byte[3], null, 1, 2);
    }

}
