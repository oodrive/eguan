package io.eguan.iscsisrv;

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

import io.eguan.iscsisrv.IscsiTargetAttributes;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the creation of a iSCSI target attributes.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class IscsiTargetAttributesCreateTest {

    @Test(expected = NullPointerException.class)
    public void testNullName() {
        new IscsiTargetAttributes(null, "", 0, 2L, false);
    }

    @Test
    public void testNullAlias() {
        final IscsiTargetAttributes iscsiTargetAttributes = new IscsiTargetAttributes("1", null, 0, 2L, false);
        Assert.assertEquals("1", iscsiTargetAttributes.getName());
        Assert.assertEquals("", iscsiTargetAttributes.getAlias());
        Assert.assertEquals(0, iscsiTargetAttributes.getConnectionCount());
        Assert.assertEquals(2L, iscsiTargetAttributes.getSize());
        Assert.assertEquals(false, iscsiTargetAttributes.isReadOnly());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeConnectionCount() {
        final IscsiTargetAttributes iscsiTargetAttributes = new IscsiTargetAttributes("1", null, -1, 2L, false);
        Assert.assertEquals("1", iscsiTargetAttributes.getName());
        Assert.assertEquals("", iscsiTargetAttributes.getAlias());
        Assert.assertEquals(0, iscsiTargetAttributes.getConnectionCount());
        Assert.assertEquals(2L, iscsiTargetAttributes.getSize());
        Assert.assertEquals(false, iscsiTargetAttributes.isReadOnly());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullSize() {
        new IscsiTargetAttributes("1", null, 0, 0L, false);
    }

    @Test
    public void testNormal() {
        final IscsiTargetAttributes iscsiTargetAttributes = new IscsiTargetAttributes("myName", "myAlias", 654,
                213233334444841L, true);
        Assert.assertEquals("myName", iscsiTargetAttributes.getName());
        Assert.assertEquals("myAlias", iscsiTargetAttributes.getAlias());
        Assert.assertEquals(654, iscsiTargetAttributes.getConnectionCount());
        Assert.assertEquals(213233334444841L, iscsiTargetAttributes.getSize());
        Assert.assertEquals(true, iscsiTargetAttributes.isReadOnly());
    }

}
