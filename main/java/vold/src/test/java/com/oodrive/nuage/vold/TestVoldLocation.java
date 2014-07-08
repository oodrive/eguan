package com.oodrive.nuage.vold;

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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the class {@link VoldLocation}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestVoldLocation {

    @Test(expected = NullPointerException.class)
    public void testFactoryNull() {
        VoldLocation.fromString(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryEmpty() {
        VoldLocation.fromString("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryUUIDEmpty() {
        VoldLocation.fromString("@host:1112");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryUUIDInvalid() {
        VoldLocation.fromString("61d1f358-4511-11e@host:1112");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryAddrNoSeparator() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308host:1112");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryAddrEmpty() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308@:1112");
    }

    /**
     * Accept any string as IP address. Resolve or connect may fail later.
     */
    @Test
    public void testFactoryAddrAnyValue() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308@ho.st:1112");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryPortNoSeparator() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308@host1112");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryPortEmpty() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308@host:");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactoryPortInvalid() {
        VoldLocation.fromString("61d1f358-4511-11e2-a28d-180373e17308@host:ja1112");
    }

    @Test
    public void testFactoryOk() {
        final UUID uuid = UUID.randomUUID();
        final VoldLocation voldLocation = VoldLocation.fromString(uuid + "@127.0.0.1:56541");
        Assert.assertEquals(uuid, voldLocation.getNode());
        Assert.assertEquals("127.0.0.1", voldLocation.getSockAddr().getHostString());
        Assert.assertEquals(56541, voldLocation.getSockAddr().getPort());
    }
}
