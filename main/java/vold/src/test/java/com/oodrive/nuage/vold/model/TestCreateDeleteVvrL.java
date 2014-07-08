package com.oodrive.nuage.vold.model;

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

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests the creation and deletion of {@link Vvr}s.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class TestCreateDeleteVvrL extends TestVvrManagerAbstract {

    public TestCreateDeleteVvrL(final Boolean vvrStarted) {
        super(vvrStarted);
    }

    @Test
    public void testCreateNameNull() {
        Assert.assertNotNull(vvrManager.createVvr(null, "description"));
    }

    @Test
    public void testCreateDescNull() {
        Assert.assertNotNull(vvrManager.createVvr("name", null));
    }

    @Test
    public void testCreateUuidNameNull() {
        vvrManager.createVvr(null, "description", UUID.randomUUID().toString());
    }

    @Test
    public void testCreateUuidDescNull() {
        vvrManager.createVvr("name", null, UUID.randomUUID().toString());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateUuidUuidNull() {
        vvrManager.createVvr("name", "description", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUuidUuidInvalid() {
        vvrManager.createVvr("name", "description", "not-a-uuid");
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateUuidUuidDuplicate() {
        final String uuid = vvrManager.createVvr("name", "description");
        vvrManager.createVvr("name2", "description2", uuid);
    }

    @Test
    public void testCreateNoWaitNameNull() {
        final String taskUuid = vvrManager.createVvrNoWait(null, "description");
        Assert.assertNotNull(voldTestHelper.waitVvrManagerTaskEnd(taskUuid, dummyMbeanServer));
    }

    @Test
    public void testCreateNoWaitDescNull() {
        final String taskUuid = vvrManager.createVvrNoWait("name", null);
        Assert.assertNotNull(voldTestHelper.waitVvrManagerTaskEnd(taskUuid, dummyMbeanServer));
    }

    @Test
    public void testCreateNoWaitUuidNameNull() {
        final String uuidStr = UUID.randomUUID().toString();
        final String taskUuid = vvrManager.createVvrNoWait(null, "description", uuidStr);
        Assert.assertEquals(uuidStr, voldTestHelper.waitVvrManagerTaskEnd(taskUuid, dummyMbeanServer));
    }

    @Test
    public void testCreateNoWaitUuidDescNull() {
        final String uuidStr = UUID.randomUUID().toString();
        final String taskUuid = vvrManager.createVvrNoWait("name", null, uuidStr);
        Assert.assertEquals(uuidStr, voldTestHelper.waitVvrManagerTaskEnd(taskUuid, dummyMbeanServer));
    }

    @Test(expected = NullPointerException.class)
    public void testCreateNoWaitUuidUuidNull() {
        vvrManager.createVvrNoWait("name", "description", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateNoWaitUuidUuidInvalid() {
        vvrManager.createVvrNoWait("name", "description", "not-a-uuid");
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateNoWaitUuidUuidDuplicate() {
        final String uuid = vvrManager.createVvr("name", "description");
        final String taskUuid = vvrManager.createVvrNoWait("name2", "description2", uuid);
        voldTestHelper.waitVvrManagerTaskEnd(taskUuid, dummyMbeanServer);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteUuidNull() {
        vvrManager.delete(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteUuidInvalid() {
        vvrManager.delete("not-a-uuid");
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteUuidUnknown() {
        vvrManager.delete(UUID.randomUUID().toString());
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteNoWaitUuidNull() {
        vvrManager.deleteNoWait(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteNoWaitUuidInvalid() {
        vvrManager.deleteNoWait("not-a-uuid");
    }

    @Test(expected = IllegalStateException.class)
    public void testDeleteNoWaitUuidUnknown() {
        final String uuidDelete = vvrManager.deleteNoWait(UUID.randomUUID().toString());
        voldTestHelper.waitVvrManagerTaskEnd(uuidDelete, dummyMbeanServer);
    }

    @Test
    public void testCreateDelete() {
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        vvrManager.createVvr("name", "description", uuidStr);

        // Need to stop the VVR before deleting it
        if (voldTestHelper.isVvrStarted()) {
            voldTestHelper.stopVvr(dummyMbeanServer, uuid);
        }
        vvrManager.delete(uuidStr);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateDeleteStarted() {
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        vvrManager.createVvr("name", "description", uuidStr);

        // Start the VVR before deleting it
        if (!voldTestHelper.isVvrStarted()) {
            voldTestHelper.startVvr(dummyMbeanServer, uuid);
        }
        vvrManager.delete(uuidStr);
    }

    @Test
    public void testCreateDeleteNoWait() {
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        final String uuidCreate = vvrManager.createVvrNoWait("name", "description", uuidStr);
        Assert.assertEquals(uuidStr, voldTestHelper.waitVvrManagerTaskEnd(uuidCreate, dummyMbeanServer));

        // Need to stop the VVR before deleting it
        if (voldTestHelper.isVvrStarted()) {
            voldTestHelper.stopVvr(dummyMbeanServer, uuid);
        }

        final String uuidDelete = vvrManager.deleteNoWait(uuidStr);
        voldTestHelper.waitVvrManagerTaskEnd(uuidDelete, dummyMbeanServer);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateDeleteNoWaitStarted() {
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        vvrManager.createVvr("name", "description", uuidStr);

        // Start the VVR before deleting it
        if (!voldTestHelper.isVvrStarted()) {
            voldTestHelper.startVvr(dummyMbeanServer, uuid);
        }
        final String uuidDelete = vvrManager.deleteNoWait(uuidStr);
        voldTestHelper.waitVvrManagerTaskEnd(uuidDelete, dummyMbeanServer);
    }
}
