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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.oodrive.nuage.vold.rest.generated.model.Task;
import com.oodrive.nuage.vold.rest.generated.model.VersionedVolumeRepository;
import com.oodrive.nuage.vold.rest.generated.model.VersionedVolumeRepositoryList;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Test for the methods of class {@link VvrsResourceImpl}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class TestVvrsResourceL extends AbstractVvrsResourceTest {

    private static final int NUMBER_OF_VVRS_TO_CREATE = 10;

    public TestVvrsResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetVvrs() throws TimeoutException {

        final WebResource target = getVvrsResource();

        final VersionedVolumeRepositoryList result = prebuildRequest(target, null).get(
                VersionedVolumeRepositoryList.class);
        assertNotNull(result);

        final List<VersionedVolumeRepository> vvrList = result.getVersionedVolumeRepositories();
        assertNotNull(vvrList);
        assertTrue(vvrList.isEmpty());

        for (int i = 1; i <= NUMBER_OF_VVRS_TO_CREATE; i++) {

            final VersionedVolumeRepository newVvr = prebuildRequest(createVvr(target, null), null).get(
                    VersionedVolumeRepository.class);
            assertNotNull(newVvr);

            final VersionedVolumeRepositoryList modifiedResult = prebuildRequest(target, null).get(
                    VersionedVolumeRepositoryList.class);
            assertNotNull(modifiedResult);

            final List<VersionedVolumeRepository> modVvrList = modifiedResult.getVersionedVolumeRepositories();
            assertNotNull(modVvrList);

            assertEquals(i, modVvrList.size());
        }

    }

    // TODO: add GetVvrs cases with filters by ownerId, machineId, dcId

    @Test
    public final void testCreateVvr() throws TimeoutException {

        final WebResource target = getVvrsResource();

        final WebResource resultRes = createVvr(target, null);
        assertNotNull(resultRes);

        final VersionedVolumeRepository result = prebuildRequest(resultRes, null).get(VersionedVolumeRepository.class);
        assertNotNull(result);
        assertNotNull(result.getUuid());
        assertEquals(getOwnerUuid(), result.getOwnerid());
    }

    @Test
    public final void testCreateVvrWithUuid() throws TimeoutException {

        final WebResource target = getVvrsResource();

        final String newUuid = UUID.randomUUID().toString();

        final MultivaluedMapImpl createVvrParam = new MultivaluedMapImpl();
        createVvrParam.add("uuid", newUuid);

        final WebResource resultRes = createVvr(target, createVvrParam);
        assertNotNull(resultRes);

        final VersionedVolumeRepository result = prebuildRequest(resultRes, null).get(VersionedVolumeRepository.class);
        assertNotNull(result);
        assertNotNull(result.getUuid());
        assertEquals(getOwnerUuid(), result.getOwnerid());
        assertEquals(newUuid, result.getUuid());
    }

    @Test
    public final void testCreateVvrWithUuidFailBadUuid() throws TimeoutException {

        final WebResource target = getVvrsResource();

        final String badUuid = "3475-dead-beef";

        final MultivaluedMapImpl createVvrParam = new MultivaluedMapImpl();
        createVvrParam.add("uuid", badUuid);

        final ClientResponse createResponse = prebuildRequest(target.path(CREATE_VVR_PATH), createVvrParam).post(
                ClientResponse.class, null);
        assertNotNull(createResponse);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());
    }

    @Test
    public final void testGetTasksVVr() throws TimeoutException {

        final WebResource vvrsResource = getVvrsResource();
        WebResource taskResource;
        final List<Task> localTasks = new ArrayList<Task>();

        for (int i = 1; i <= NUMBER_OF_VVRS_TO_CREATE; i++) {
            // create a vvr
            final ClientResponse createVvrResponse = prebuildRequest(vvrsResource.path(CREATE_VVR_PATH), null).post(
                    ClientResponse.class, null);
            assertNotNull(createVvrResponse);

            // get task and add it locally
            taskResource = getTaskResourceFromAcceptedResponse(createVvrResponse);
            final Task task = prebuildRequest(taskResource, null).get(Task.class);
            localTasks.add(task);

            // now get tasks list returned by the server
            final List<Task> modTaskList = getServerTasks(getVvrsTasksResource());
            assertNotNull(modTaskList);

            // check that all the task locally saved are present in the server returned list
            assertTrue(checkTasks(localTasks, modTaskList));

            // wait the task is ended (otherwise all the vvrs will not be necessarily deleted at the end of the tests
            getResultFromTask(createVvrResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
        }

    }
    // TODO: add tests for more error cases as soon as they're defined

}
