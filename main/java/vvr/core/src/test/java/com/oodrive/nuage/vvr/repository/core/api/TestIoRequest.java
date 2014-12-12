package com.oodrive.nuage.vvr.repository.core.api;

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

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.vvr.repository.core.api.DeviceReadWriteHandleImpl.IoRequest;
import com.oodrive.nuage.vvr.repository.core.api.DeviceReadWriteHandleImpl.IoTaskOpe;

/**
 * Unit test for {@link IoRequest}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestIoRequest {

    @Test
    public void testOverlap() {
        Assert.assertTrue(newIoRequest(0, 1).overlap(newIoRequest(0, 1)));
        Assert.assertTrue(newIoRequest(0, 1023).overlap(newIoRequest(0, 1)));
        Assert.assertTrue(newIoRequest(0, 1023).overlap(newIoRequest(1, 2)));
        Assert.assertTrue(newIoRequest(0, 1023).overlap(newIoRequest(1022, 2)));
        Assert.assertTrue(newIoRequest(0, 1023).overlap(newIoRequest(1022, 1)));
        Assert.assertTrue(newIoRequest(0, 1023).overlap(newIoRequest(10, 22)));

        // Same test, change order
        Assert.assertTrue(newIoRequest(0, 1).overlap(newIoRequest(0, 1)));
        Assert.assertTrue(newIoRequest(0, 1).overlap(newIoRequest(0, 1023)));
        Assert.assertTrue(newIoRequest(1, 2).overlap(newIoRequest(0, 1023)));
        Assert.assertTrue(newIoRequest(1022, 2).overlap(newIoRequest(0, 1023)));
        Assert.assertTrue(newIoRequest(1022, 1).overlap(newIoRequest(0, 1023)));
        Assert.assertTrue(newIoRequest(10, 22).overlap(newIoRequest(0, 1023)));

        Assert.assertFalse(newIoRequest(0, 1).overlap(newIoRequest(1, 1)));
        Assert.assertFalse(newIoRequest(0, 1023).overlap(newIoRequest(1023, 1)));
        Assert.assertFalse(newIoRequest(1024, 10).overlap(newIoRequest(1023, 1)));
    }

    private final IoRequest newIoRequest(final long offset, final int length) {
        return new IoRequest(IoTaskOpe.READ, offset, length, null, null);
    }
}
