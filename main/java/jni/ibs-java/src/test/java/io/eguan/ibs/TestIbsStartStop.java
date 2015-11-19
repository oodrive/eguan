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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test IBS start, stop, close.
 * 
 * @author oodrive
 * @author llambert
 */
@RunWith(value = Parameterized.class)
public final class TestIbsStartStop extends TestIbsClass {

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB }, { IbsType.FS }, { IbsType.FAKE } };
        return Arrays.asList(data);
    }

    public TestIbsStartStop(final IbsType ibsType) {
        super(ibsType);
    }

    /**
     * IBS must be stopped before each test.
     */
    @Before
    public void stopIbs() {
        ibs.stop();
    }

    @Test
    public void simpleStartStop() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.start();
        Assert.assertTrue(ibs.isStarted());
        ibs.stop();
        Assert.assertFalse(ibs.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public void reStart() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.start();
        Assert.assertTrue(ibs.isStarted());
        ibs.start();
    }

    @Test
    public void reStop() {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.start();
        Assert.assertTrue(ibs.isStarted());
        ibs.stop();
        Assert.assertFalse(ibs.isStarted());
        ibs.stop();
        Assert.assertFalse(ibs.isStarted());
    }

    @Test(expected = IbsException.class)
    public void notStartedErrGet1() throws IbsException, IbsIOException, BufferUnderflowException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.get(key, 10, false);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrGet2() throws IbsException, IbsIOException, BufferUnderflowException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.get(key, data);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrGet3() throws IbsException, IbsIOException, BufferUnderflowException,
            IllegalArgumentException, IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.get(key, data, 0, 1);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrDel1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.del(key);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrDel2() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        ibs.del(key);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrPut1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.put(key, data);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrPut2() throws IbsException, IbsIOException, IllegalArgumentException,
            IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] key = "1".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.put(key, data, 0, 1);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrReplace1() throws IbsException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] keyOld = "1".getBytes();
        final byte[] keyNew = "A".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.replace(keyOld, keyNew, data);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrReplace2() throws IbsException, IllegalArgumentException, IbsIOException,
            IndexOutOfBoundsException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        final byte[] keyOld = "1".getBytes();
        final byte[] keyNew = "A".getBytes();
        final ByteBuffer data = ByteBuffer.wrap("2".getBytes());
        ibs.replace(keyOld, keyNew, data, 0, 1);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrCreateTx() throws IbsException, IllegalArgumentException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.createTransaction();
    }

    @Test(expected = IbsException.class)
    public void notStartedErrCommitTx() throws IbsException, IllegalArgumentException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.commit(1);
    }

    @Test(expected = IbsException.class)
    public void notStartedErrRollbackTx() throws IbsException, IllegalArgumentException, IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.rollback(1);
    }

}
