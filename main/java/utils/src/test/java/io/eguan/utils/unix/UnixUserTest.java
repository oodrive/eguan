package io.eguan.utils.unix;

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

import io.eguan.utils.unix.UnixUser;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests of {@link UnixUser}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class UnixUserTest {

    public UnixUserTest() {
        super();
    }

    @Test
    public void testCurrent() {
        final UnixUser currentUser = UnixUser.getCurrentUser();
        Assert.assertEquals(System.getProperty("user.name"), currentUser.getName());
        // Can not tell the uid or gid
    }

    /**
     * Check name, uid and gid of a well known user.
     */
    @Test
    public void testRoot() {
        final UnixUser root = UnixUser.getUser("root");
        Assert.assertEquals("root", root.getName());
        Assert.assertEquals(0, root.getUid());
        Assert.assertEquals(0, root.getGid());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserUnknown() {
        UnixUser.getUser("_can not exists__");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserNull() {
        UnixUser.getUser(null);
    }
}
