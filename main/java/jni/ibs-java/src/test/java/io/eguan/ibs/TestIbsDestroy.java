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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test IBS destroy.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
@RunWith(value = Parameterized.class)
public class TestIbsDestroy extends TestIbsTest {

    @Parameters
    public static Collection<Object[]> testOps() {
        final Object[][] data = new Object[][] { { IbsType.LEVELDB }, { IbsType.FS }, { IbsType.FAKE } };
        return Arrays.asList(data);
    }

    public TestIbsDestroy(final IbsType ibsType) {
        super(ibsType, "no");
    }

    @Test
    public void simpleDestroy() throws IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.destroy();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        if (ibsType == IbsType.LEVELDB) {
            Assert.assertFalse(tempFileConfig.exists());
            Assert.assertFalse(tempDirIbpgen.toFile().exists());
            Assert.assertFalse(tempDirIbp.toFile().exists());
        }
        else if (ibsType == IbsType.FS) {
            Assert.assertFalse(tempFileConfig.exists());
        }
    }

    @Test
    public void reDestroy() throws IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.destroy();
        ibs.destroy();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        if (ibsType == IbsType.LEVELDB) {
            Assert.assertFalse(tempFileConfig.exists());
            Assert.assertFalse(tempDirIbpgen.toFile().exists());
            Assert.assertFalse(tempDirIbp.toFile().exists());
        }
        else if (ibsType == IbsType.FS) {
            Assert.assertFalse(tempFileConfig.exists());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void destroyStart() throws IbsIOException {
        Assert.assertFalse(ibs.isStarted());
        Assert.assertFalse(ibs.isClosed());
        ibs.destroy();
        Assert.assertFalse(ibs.isStarted());
        Assert.assertTrue(ibs.isClosed());
        ibs.start();
    }

}
