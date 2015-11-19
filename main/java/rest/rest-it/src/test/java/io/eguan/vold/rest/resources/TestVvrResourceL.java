package io.eguan.vold.rest.resources;

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
import io.eguan.vold.rest.generated.model.ExecState;
import io.eguan.vold.rest.generated.model.Snapshot;
import io.eguan.vold.rest.generated.model.Task;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepository;
import io.eguan.vold.rest.generated.resources.VvrResource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Tests for the methods of {@link VvrResource} implementations.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class TestVvrResourceL extends AbstractVvrsResourceTest {

    public TestVvrResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetVvr() throws TimeoutException {

        final String ownerId = getOwnerUuid();

        final WebResource vvrResource = createVvr(getVvrsResource(), null);
        assertNotNull(vvrResource);

        final VersionedVolumeRepository result = prebuildRequest(vvrResource, null)
                .get(VersionedVolumeRepository.class);
        assertEquals(ownerId, result.getOwnerid());
    }

    @Test
    public final void testPostVvr() throws JAXBException, TimeoutException, InterruptedException {

        final WebResource vvrResource = createVvr(getVvrsResource(), null);
        assertNotNull(vvrResource);

        final VersionedVolumeRepository vvr = prebuildRequest(vvrResource, null).get(VersionedVolumeRepository.class);

        vvr.setName("original name");
        vvr.setDescription("original description");

        final VersionedVolumeRepository originalVvr = prebuildRequest(vvrResource, null).post(
                VersionedVolumeRepository.class, vvr);
        assertFalse(originalVvr == vvr);
        assertEquals(vvr.getName(), originalVvr.getName());
        assertEquals(vvr.getDescription(), originalVvr.getDescription());

        final VersionedVolumeRepository modifiedVvr = VVR_REPLICATOR.replicate(vvr);
        assertFalse(modifiedVvr == vvr);

        modifiedVvr.setName("modified name");
        modifiedVvr.setDescription("modified description");

        final VersionedVolumeRepository result = prebuildRequest(vvrResource, null).post(
                VersionedVolumeRepository.class, modifiedVvr);
        assertEquals(modifiedVvr.getUuid(), result.getUuid());
        assertEquals(modifiedVvr.isInitialized(), result.isInitialized());
        assertEquals(modifiedVvr.isStarted(), result.isStarted());

        assertEquals(modifiedVvr.getOwnerid(), result.getOwnerid());
        // Wait some time for the new description to be set
        int i = 0;
        while (originalVvr.getDescription().equals(
                prebuildRequest(vvrResource, null).get(VersionedVolumeRepository.class).getDescription())
                && i < 30) {
            Thread.sleep(1000);
            i++;
        }
        assertEquals(modifiedVvr.getName(), result.getName());
        assertEquals(modifiedVvr.getDescription(), result.getDescription());
    }

    @Test
    public final void testPostVvrFailNull() throws TimeoutException {

        final WebResource vvrResource = createVvr(getVvrsResource(), null);
        assertNotNull(vvrResource);

        final ClientResponse response = prebuildRequest(vvrResource, null).post(ClientResponse.class, null);
        assertEquals(400, response.getStatus());
    }

    @Test
    public final void testPostVvrFailIdChanged() throws JAXBException, TimeoutException {

        final WebResource vvrResource = createVvr(getVvrsResource(), null);

        final VersionedVolumeRepository vvr = prebuildRequest(vvrResource, null).get(VersionedVolumeRepository.class);
        final String name = "TestVVR";
        final String desc = "Test instance of a VVR";
        vvr.setName(name);
        vvr.setDescription(desc);

        final VersionedVolumeRepository originalVvr = prebuildRequest(vvrResource, null).post(
                VersionedVolumeRepository.class, vvr);
        assertFalse(originalVvr == vvr);
        assertEquals(vvr.getName(), originalVvr.getName());
        assertEquals(vvr.getDescription(), originalVvr.getDescription());

        final VersionedVolumeRepository modifiedVvr = VVR_REPLICATOR.replicate(vvr);
        assertFalse(modifiedVvr == vvr);

        modifiedVvr.setName("modified name");
        modifiedVvr.setDescription("modified description");

        // modifies the id
        modifiedVvr.setUuid(UUID.randomUUID().toString());
        assertFalse(vvr.getUuid().equals(modifiedVvr.getUuid()));

        final ClientResponse response = prebuildRequest(vvrResource, null).post(ClientResponse.class, modifiedVvr);
        assertEquals(403, response.getStatus());

        final VersionedVolumeRepository readBackVvr = prebuildRequest(vvrResource, null).get(
                VersionedVolumeRepository.class);
        assertEquals(originalVvr.getName(), readBackVvr.getName());
        assertEquals(originalVvr.getDescription(), readBackVvr.getDescription());
    }

    @Test
    public final void testDeleteVvr() throws TimeoutException {

        final WebResource vvrResource = createVvr(getVvrsResource(), null);

        final ClientResponse deleteResponse = prebuildRequest(vvrResource, null).delete(ClientResponse.class);
        assertEquals(202, deleteResponse.getStatus());

        getResultFromTask(deleteResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final ClientResponse getResponse = prebuildRequest(vvrResource, null).get(ClientResponse.class);
        assertEquals(404, getResponse.getStatus());
    }

    @Test(expected = IllegalStateException.class)
    public final void testDeleteVvrFailStarted() throws TimeoutException, InterruptedException {

        final WebResource vvrResource = startVvr(createVvr(getVvrsResource(), null), null);

        final ClientResponse deleteResponse = prebuildRequest(vvrResource, null).delete(ClientResponse.class);
        assertEquals(202, deleteResponse.getStatus());

        final WebResource taskRes = getTaskResourceFromAcceptedResponse(deleteResponse);

        try {
            getResultFromTask(deleteResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
        }
        catch (final TimeoutException te) {
            final Task task = prebuildRequest(taskRes, null).get(Task.class);
            assertEquals(ExecState.FAILED, task.getState());
            throw te;
        }
    }

    @Test
    public final void testStartStopVvr() throws TimeoutException, InterruptedException {

        final WebResource startedVvrRes = startVvr(createVvr(getVvrsResource(), null), null);

        final ClientResponse stopVvrReponse = prebuildRequest(startedVvrRes.path(STOP_VVR_PATH), null).post(
                ClientResponse.class, null);

        final WebResource stoppedVvrRes = getResultFromTask(stopVvrReponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final VersionedVolumeRepository stoppedVvr = prebuildRequest(stoppedVvrRes, null).get(
                VersionedVolumeRepository.class);

        assertFalse(stoppedVvr.isStarted());

        final WebResource reStoppedVvrRes = getResultFromTask(stopVvrReponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final VersionedVolumeRepository reStoppedVvr = prebuildRequest(reStoppedVvrRes, null).get(
                VersionedVolumeRepository.class);

        assertFalse(reStoppedVvr.isStarted());
    }

    @Test(expected = IllegalStateException.class)
    public final void testStartVvrFailStarted() throws TimeoutException, InterruptedException {

        final WebResource startedVvrRes = startVvr(createVvr(getVvrsResource(), null), null);

        // call start twice
        startVvr(startedVvrRes, null);
    }

    @Test
    public final void testGetRootSnapshot() throws TimeoutException, InterruptedException {

        final WebResource startedVvrRes = startVvr(createVvr(getVvrsResource(), null), null);

        final WebResource rootSnapRes = startedVvrRes.path("root");

        final Snapshot rootSnap = prebuildRequest(rootSnapRes, null).get(Snapshot.class);
        assertNotNull(rootSnap);
        assertEquals(rootSnap.getUuid(), rootSnap.getParent());
    }

}
