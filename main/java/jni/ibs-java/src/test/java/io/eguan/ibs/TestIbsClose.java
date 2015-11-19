package io.eguan.ibs;

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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TestIbsClose extends TestIbsTest {

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB }, { IbsType.FS }, { IbsType.FAKE } };
        return Arrays.asList(data);
    }

    public TestIbsClose(final IbsType ibsType) {
        super(ibsType, "no");
    }

    @Test
    public void close() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
    }

    @Test
    public void startClose() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.start();
        Assert.assertTrue(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
    }

    @Test
    public void startStopClose() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.start();
        Assert.assertTrue(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.stop();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
    }

    @Test
    public void reclose() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
    }

    @Test(expected = IllegalStateException.class)
    public void closeStart() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        ibs.start();
    }

    @Test(expected = IbsException.class)
    public void closedErrGet1() throws IbsException, IbsIOException, BufferUnderflowException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.get(key, 10, false);
    }

    @Test(expected = IbsException.class)
    public void closedErrGet2() throws IbsException, IbsIOException, BufferUnderflowException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.get(key, data);
    }

    @Test(expected = IbsException.class)
    public void closedErrGet3() throws IbsException, IbsIOException, BufferUnderflowException,
            IllegalArgumentException, IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.get(key, data, 0, 1);
    }

    @Test(expected = IbsException.class)
    public void closedErrDel1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.del(key);
    }

    @Test(expected = IbsException.class)
    public void closedErrDel2() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.del(key);
    }

    @Test(expected = IbsException.class)
    public void closedErrPut1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.put(key, data);
    }

    @Test(expected = IbsException.class)
    public void closedErrPut2() throws IbsException, IbsIOException, IllegalArgumentException,
            IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.put(key, data, 0, 1);
    }

    @Test(expected = IbsException.class)
    public void closedErrReplace1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] keyOld = "1".getBytes();
        final byte[] keyNew = "A".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.replace(keyOld, keyNew, data);
    }

    @Test(expected = IbsException.class)
    public void closedErrReplace2() throws IbsException, IllegalArgumentException, IbsIOException,
            IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.close();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        final byte[] keyOld = "1".getBytes();
        final byte[] keyNew = "A".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.replace(keyOld, keyNew, data, 0, 1);
    }
}
