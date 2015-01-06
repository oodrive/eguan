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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Strings}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class TestStrings {

    @Test(expected = NullPointerException.class)
    public void hexaStringNull() {
        Strings.toHexString(null);
    }

    @Test
    public void hexaString() {
        final byte[] ref = { 0x00, 0x01, 0x23, 0x45, 0x67, 0x78, -102, -67, -4, 0x3F, -88 };
        Assert.assertEquals("0001234567789abdfc3fa8", Strings.toHexString(ref));
        Assert.assertEquals("0001234567789ABDFC3FA8", Strings.toHexString(ref, true));
    }
}
