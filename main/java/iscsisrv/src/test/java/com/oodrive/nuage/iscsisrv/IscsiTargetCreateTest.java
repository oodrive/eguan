package com.oodrive.nuage.iscsisrv;

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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the creation of a iSCSI target.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
public class IscsiTargetCreateTest {

    /** Dummy IQN name */
    private static final String DUMMY_NAME_IQN = "iqn.2000-06.com.oodrive:dummy target";
    /** Dummy EUI name */
    private static final String DUMMY_NAME_EUI = "eui.0123456789ABCDEF";
    /** Dummy non null device */
    static final IscsiDevice DUMMY_DEVICE = new IscsiDevice() {
        @Override
        public final long getSize() {
            return 4096;
        }

        @Override
        public final int getBlockSize() {
            return 4096;
        }

        @Override
        public final boolean isReadOnly() {
            return false;
        }

        @Override
        public final void write(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            // No op
        }

        @Override
        public final void read(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            // No op
        }

        @Override
        public final void close() throws IOException {
            // No op
        }
    };

    @Test(expected = NullPointerException.class)
    public void testNullName() {
        IscsiTarget.newIscsiTarget(null, "", DUMMY_DEVICE);
    }

    @Test(expected = NullPointerException.class)
    public void testNullDevice() {
        IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, "", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameIqnTooLong() {
        final char[] name = new char[234 - 4];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        name[name.length - 1] = (byte) 0;
        IscsiTarget.newIscsiTarget("iqn." + String.copyValueOf(name), null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameIqnLengthLimitNonNullTerminated() {
        final char[] name = new char[233 - 4];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        IscsiTarget.newIscsiTarget("iqn." + String.copyValueOf(name), null, DUMMY_DEVICE);
    }

    @Test
    public void testNameIqnLengthLimit() {
        final char[] name = new char[233 - 4];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        name[name.length - 1] = (byte) 0;
        IscsiTarget.newIscsiTarget("iqn." + String.copyValueOf(name), null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameEuiTooLong() {
        final char[] name = new char[21];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        IscsiTarget.newIscsiTarget("eui." + String.copyValueOf(name), null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameEuiTooShort() {
        final char[] name = new char[19];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        IscsiTarget.newIscsiTarget("eui." + String.copyValueOf(name), null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameEuiInvalid() {
        IscsiTarget.newIscsiTarget("eui.0123456789zBCDEF", null, DUMMY_DEVICE);
    }

    @Test
    public void testNameEuiValid() {
        IscsiTarget.newIscsiTarget("eui.0123456789ABCDEF", null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameInvalid() {
        IscsiTarget.newIscsiTarget("invalid", null, DUMMY_DEVICE);
    }

    /**
     * Suppose that the name is valid for lower case hexa digits.
     */
    @Test
    public void testNameEuiValidLowercase() {
        IscsiTarget.newIscsiTarget("eui.0123456789abcdef", null, DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAliasTooLong() {
        final char[] name = new char[256];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        name[name.length - 1] = (byte) 0;
        IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, String.copyValueOf(name), DUMMY_DEVICE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAliasLengthLimitNonNullTerminated() {
        final char[] name = new char[255];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, String.copyValueOf(name), DUMMY_DEVICE);
    }

    @Test
    public void testAliasLengthLimit() {
        final char[] name = new char[255];
        for (int i = 0; i < name.length; i++) {
            name[i] = 'a';
        }
        name[name.length - 1] = (byte) 0;
        IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, String.copyValueOf(name), DUMMY_DEVICE);
    }

    @Test
    public void testTargetNameIqn() {
        final IscsiTarget targetNoAlias = IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, null, DUMMY_DEVICE);
        Assert.assertEquals(DUMMY_NAME_IQN, targetNoAlias.getTargetName());
        Assert.assertNull(targetNoAlias.getTargetAlias());

        final IscsiTarget targetAlias = IscsiTarget.newIscsiTarget(DUMMY_NAME_IQN, "My device alias \u20AC",
                DUMMY_DEVICE);
        Assert.assertEquals(DUMMY_NAME_IQN, targetAlias.getTargetName());
        Assert.assertEquals("My device alias \u20AC", targetAlias.getTargetAlias());

        Assert.assertTrue(targetAlias.equals(targetNoAlias));
        Assert.assertTrue(targetAlias.equals(targetAlias));
        Assert.assertFalse(targetAlias.equals("targetNoAlias"));
        Assert.assertEquals(targetAlias.hashCode(), targetNoAlias.hashCode());
        Assert.assertEquals(targetAlias.toString(), targetNoAlias.toString());
    }

    @Test
    public void testTargetNameEui() {
        final IscsiTarget targetNoAlias = IscsiTarget.newIscsiTarget(DUMMY_NAME_EUI, null, DUMMY_DEVICE);
        Assert.assertEquals(DUMMY_NAME_EUI, targetNoAlias.getTargetName());
        Assert.assertNull(targetNoAlias.getTargetAlias());

        final IscsiTarget targetAlias = IscsiTarget.newIscsiTarget(DUMMY_NAME_EUI, "My device alias \u20AC",
                DUMMY_DEVICE);
        Assert.assertEquals(DUMMY_NAME_EUI, targetAlias.getTargetName());
        Assert.assertEquals("My device alias \u20AC", targetAlias.getTargetAlias());

        Assert.assertTrue(targetAlias.equals(targetNoAlias));
        Assert.assertTrue(targetAlias.equals(targetAlias));
        Assert.assertFalse(targetAlias.equals("targetNoAlias"));
        Assert.assertEquals(targetAlias.hashCode(), targetNoAlias.hashCode());
        Assert.assertEquals(targetAlias.toString(), targetNoAlias.toString());
    }
}
