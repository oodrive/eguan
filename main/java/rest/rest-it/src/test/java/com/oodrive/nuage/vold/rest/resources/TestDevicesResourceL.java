package com.oodrive.nuage.vold.rest.resources;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.oodrive.nuage.vold.rest.generated.model.Device;
import com.oodrive.nuage.vold.rest.generated.model.DeviceList;
import com.oodrive.nuage.vold.rest.generated.model.Task;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public final class TestDevicesResourceL extends AbstractVvrResourceTest {

    private static final int NUMBER_OF_DEVICES_TO_CREATE = 10;

    public TestDevicesResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetDevices() throws TimeoutException {

        final WebResource vvrResource = getVvrResource();

        final WebResource target = vvrResource.path("devices");

        final WebResource rootSnapRes = vvrResource.path("root");

        // gets initial list
        final DeviceList deviceList = prebuildRequest(target, null).get(DeviceList.class);
        assertNotNull(deviceList);

        final List<Device> devices = deviceList.getDevices();
        assertNotNull(devices);
        assertTrue(devices.isEmpty());

        for (int i = 1; i <= NUMBER_OF_DEVICES_TO_CREATE; i++) {

            final MultivaluedMapImpl createQueryParams = new MultivaluedMapImpl();
            createQueryParams.add("name", TestDevicesResourceL.class.getSimpleName() + " " + i);
            createQueryParams.add("size", DEFAULT_DEVICE_SIZE); // MUST define a size

            assertNotNull(createDevice(rootSnapRes, createQueryParams));

            final DeviceList modifiedResult = prebuildRequest(target, null).get(DeviceList.class);
            assertNotNull(modifiedResult);

            final List<Device> modDevsList = modifiedResult.getDevices();
            assertNotNull(modDevsList);

            assertEquals(i, modDevsList.size());

        }

    }

    @Test
    public final void testGetTasksList() throws TimeoutException, InterruptedException {

        final WebResource vvrResource = getVvrResource();
        final List<Task> localTasks = new ArrayList<Task>();

        for (int i = 1; i <= NUMBER_OF_DEVICES_TO_CREATE; i++) {

            final ClientResponse createResponse = createDevice(vvrResource);
            assertNotNull(createResponse);

            // get task and add it locally
            final WebResource taskResource = getTaskResourceFromAcceptedResponse(createResponse);
            final Task task = prebuildRequest(taskResource, null).get(Task.class);
            localTasks.add(task);

            // now get tasks list returned by the server for vvr
            final WebResource vvrTasksResource = vvrResource.path("tasks");
            final List<Task> vvrModTaskList = getServerTasks(vvrTasksResource);
            assertTrue(checkTasks(localTasks, vvrModTaskList));

            // now get tasks list returned by the server for vvrs
            final WebResource vvrsTasksResource = getVvrsResource().path("tasks");
            final List<Task> vvrsModTaskList = getServerTasks(vvrsTasksResource);
            assertTrue(checkTasks(localTasks, vvrsModTaskList));

            // wait the task is ended (otherwise all the devices will not be necessarily deleted at the end of the test)
            getResultFromTask(createResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
        }

    }

    @Test
    public final void testGetTaskNotInList() throws TimeoutException, InterruptedException {

        // create a new vvr vvrResource2
        final WebResource vvrResource2 = startVvr(createVvr(getVvrsResource(), null), null);

        // create device on vvrResource2
        final ClientResponse createDevResponse = createDevice(vvrResource2);
        assertNotNull(createDevResponse);

        // get task
        final WebResource taskResource = getTaskResourceFromAcceptedResponse(createDevResponse);
        final Task localTask = prebuildRequest(taskResource, null).get(Task.class);

        // now get tasks list returned by the server for the current vvr
        final WebResource vvrTasksResource = getVvrResource().path("tasks");
        final List<Task> vvrModTaskList = getServerTasks(vvrTasksResource);

        // check that this task can not be found in the current vvr task list
        for (final Task serverTask : vvrModTaskList) {
            assertFalse(localTask.getUuid().equals(serverTask.getUuid()));
        }
    }
}
