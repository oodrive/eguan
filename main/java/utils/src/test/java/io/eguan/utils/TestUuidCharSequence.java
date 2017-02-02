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

import io.eguan.utils.UuidCharSequence;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link UuidCharSequence}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestUuidCharSequence {
    private static final UUID REF1 = new UUID(81985529216486895L, 4999316062091541180L);
    private static final String REF_UPPER = "0123456789ABCDEF4561237890DEFABC";
    private static final String REF_LOWER = "0123456789abcdef4561237890defabc";

    @Test
    public void testLength() {
        Assert.assertEquals(32, new UuidCharSequence(new UUID(0, 0)).length());
    }

    @Test
    public void testToString() {
        Assert.assertEquals(new UuidCharSequence(REF1, true).toString(), REF_UPPER);
        Assert.assertEquals(new UuidCharSequence(REF1, false).toString(), REF_LOWER);
        Assert.assertEquals(new UuidCharSequence(REF1).toString(), REF_LOWER);
    }

    @Test
    public void testSubSequence() {
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(12, 25).toString(), REF_LOWER.subSequence(12, 25));
        Assert.assertEquals(new UuidCharSequence(REF1, true).subSequence(0, 1).toString(), REF_UPPER.subSequence(0, 1));
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(30, 31).toString(), REF_LOWER.subSequence(30, 31));
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(31, 31).toString(), REF_LOWER.subSequence(31, 31));
        Assert.assertEquals(new UuidCharSequence(REF1, false).subSequence(0, 2).toString(), REF_LOWER.subSequence(0, 2));
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(5, 32).toString(), REF_LOWER.subSequence(5, 32));
    }

    @Test
    public void testSubSubSequence() {
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(12, 25).subSequence(2, 5).toString(), REF_LOWER
                .subSequence(12, 25).subSequence(2, 5));
        Assert.assertEquals(new UuidCharSequence(REF1, true).subSequence(0, 1).subSequence(0, 1).toString(), REF_UPPER
                .subSequence(0, 1).subSequence(0, 1));
        Assert.assertEquals(new UuidCharSequence(REF1).subSequence(20, 31).subSequence(0, 3).toString(), REF_LOWER
                .subSequence(20, 31).subSequence(0, 3));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void charAtNeg() {
        new UuidCharSequence(REF1).charAt(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void charAtAfterLength() {
        new UuidCharSequence(REF1).charAt(32);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceStartNeg() {
        new UuidCharSequence(REF1).subSequence(-1, 23);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceEndNeg() {
        new UuidCharSequence(REF1).subSequence(1, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceStartEnd() {
        new UuidCharSequence(REF1).subSequence(10, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceEndLength() {
        new UuidCharSequence(REF1).subSequence(10, 33);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceCharAtNeg() {
        new UuidCharSequence(REF1).subSequence(22, 23).charAt(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceCharAtAfterLength() {
        new UuidCharSequence(REF1).subSequence(22, 23).charAt(1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceSubSequenceStartNeg() {
        new UuidCharSequence(REF1).subSequence(22, 25).subSequence(-1, 23);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceSubSequenceEndNeg() {
        new UuidCharSequence(REF1).subSequence(22, 25).subSequence(1, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceSubSequenceStartEnd() {
        new UuidCharSequence(REF1).subSequence(22, 25).subSequence(10, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void subSequenceSubSequenceEndLength() {
        new UuidCharSequence(REF1).subSequence(22, 25).subSequence(2, 4);
    }

}
