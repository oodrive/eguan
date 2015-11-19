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
import static org.junit.Assert.assertTrue;
import io.eguan.vold.rest.generated.model.Device;
import io.eguan.vold.rest.generated.model.DeviceList;
import io.eguan.vold.rest.generated.model.Snapshot;
import io.eguan.vold.rest.generated.model.SnapshotList;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public final class TestSnapshotResourceL extends AbstractVvrResourceTest {

    private static final String classNamePrefix = TestSnapshotResourceL.class.getSimpleName();

    public TestSnapshotResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetSnapshot() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();
        assertNotNull(snapshotResource);

        final Snapshot result = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(result);
        assertNotNull(result.getUuid());

        final long oldSnapSize = result.getSize();
        final long newSnapSize = oldSnapSize + DEFAULT_DEVICE_SIZE;
        final String newSnapName = classNamePrefix + "-newSnapshot";

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", classNamePrefix + "-getSnapshot");
        createDevParams.add("size", newSnapSize);

        final WebResource devResource = createDevice(snapshotResource, createDevParams);

        final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
        createSnapParams.add("name", newSnapName);

        final WebResource newSnapRes = createSnapshot(devResource, createSnapParams);

        final Snapshot readSnap = prebuildRequest(newSnapRes, null).get(Snapshot.class);
        assertNotNull(readSnap);
        assertEquals(newSnapSize, readSnap.getSize());
        assertEquals(newSnapName, readSnap.getName());
    }

    @Test
    public final void testPostSnapshot() throws TimeoutException, JAXBException {

        final WebResource snapshotResource = getSnapshotResource();
        assertNotNull(snapshotResource);

        final Snapshot originalSnap = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(originalSnap);
        assertNotNull(originalSnap.getUuid());

        final Snapshot modifiedSnap = snapshotReplicator.replicate(originalSnap);
        assertFalse(originalSnap == modifiedSnap);
        modifiedSnap.setName(classNamePrefix + " - modified snapshot");
        modifiedSnap.setDescription(classNamePrefix + " is now modified ");

        final ClientResponse postResponse = prebuildRequest(snapshotResource, null).post(ClientResponse.class,
                modifiedSnap);
        assertEquals(Status.OK.getStatusCode(), postResponse.getStatus());

        final Snapshot updatedSnap = postResponse.getEntity(Snapshot.class);
        assertNotNull(updatedSnap);
        assertEquals(originalSnap.getUuid(), updatedSnap.getUuid());
        assertEquals(originalSnap.getSize(), updatedSnap.getSize());
        assertEquals(modifiedSnap.getName(), updatedSnap.getName());
        assertEquals(modifiedSnap.getDescription(), updatedSnap.getDescription());
    }

    @Test
    public final void testPostSnapshotFailNull() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();
        assertNotNull(snapshotResource);

        final ClientResponse postResponse = prebuildRequest(snapshotResource, null).post(ClientResponse.class, null);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), postResponse.getStatus());

    }

    @Test
    public final void testPostSnapshotFailIdChanged() throws TimeoutException, JAXBException {

        final WebResource snapshotResource = getSnapshotResource();
        assertNotNull(snapshotResource);

        final Snapshot originalSnap = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(originalSnap);
        assertNotNull(originalSnap.getUuid());

        final Snapshot modifiedSnap = snapshotReplicator.replicate(originalSnap);
        assertFalse(originalSnap == modifiedSnap);
        modifiedSnap.setName(classNamePrefix + " - modified snapshot");
        modifiedSnap.setDescription(classNamePrefix + " is now modified ");

        // modifies the ID
        modifiedSnap.setUuid(UUID.randomUUID().toString());

        final ClientResponse postResponse = prebuildRequest(snapshotResource, null).post(ClientResponse.class,
                modifiedSnap);
        assertEquals(Status.FORBIDDEN.getStatusCode(), postResponse.getStatus());

        final Snapshot readSnap = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(readSnap);
        assertEquals(originalSnap.getUuid(), readSnap.getUuid());
        assertEquals(originalSnap.getSize(), readSnap.getSize());
        assertEquals(originalSnap.getName(), readSnap.getName());
        assertEquals(originalSnap.getDescription(), readSnap.getDescription());
    }

    @Test
    public final void testDeleteSnapshot() throws IllegalStateException, TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();
        assertNotNull(snapshotResource);

        final Snapshot targetSnap = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(targetSnap);
        assertNotNull(targetSnap.getUuid());
        // checks the provided snapshot is not the root snapshot
        assertFalse(targetSnap.getUuid().equals(targetSnap.getParent()));

        final ClientResponse deleteResponse = prebuildRequest(snapshotResource, null).delete(ClientResponse.class);

        final WebResource deletedSnapshotRes = getResultFromTask(deleteResponse, TimeUnit.SECONDS,
                DEFAULT_TASK_TIMEOUT_S);

        final ClientResponse getResponse = prebuildRequest(deletedSnapshotRes, null).get(ClientResponse.class);
        assertEquals(Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());
    }

    @Test
    public final void testDeleteSnapshotFailRoot() throws IllegalStateException, TimeoutException {

        final WebResource rootSnapResource = getVvrResource().path("root");
        assertNotNull(rootSnapResource);

        final Snapshot rootSnap = prebuildRequest(rootSnapResource, null).get(Snapshot.class);
        assertNotNull(rootSnap);
        assertNotNull(rootSnap.getUuid());
        // checks the provided snapshot is really the root snapshot
        assertTrue(rootSnap.getUuid().equals(rootSnap.getParent()));

        final ClientResponse deleteResponse = prebuildRequest(rootSnapResource, null).delete(ClientResponse.class);

        assertEquals(Status.FORBIDDEN.getStatusCode(), deleteResponse.getStatus());
    }

    // @Test
    public final void testDeleteSnapshotFailDeviceActive() {
        // TODO
    }

    @Test
    public final void testNewDevice() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);
        // Name only
        {
            final String devName = "testDevice1";
            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertNotNull(createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }
        // Name + description
        {
            final String devName = "testDevice2";
            final String devDescription = "testDescr2";
            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("description", devDescription);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertNotNull(createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(devDescription, createdDev.getDescription());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }
    }

    @Test
    public final void testNewDeviceWithSize() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        // Name + size
        {
            final String devName = "testDevice1";
            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("size", 4096);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertNotNull(createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }
        // Name + size + description
        {
            final String devName = "testDevice2";
            final String devDescription = "testDescr2";
            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("description", devDescription);
            createDevParams.add("size", 4096);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertNotNull(createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(devDescription, createdDev.getDescription());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }

    }

    @Test
    public final void testNewDeviceWithUuid() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        // Name + uuid
        {
            final String devName = "testDevice1";
            final UUID devUuid = UUID.randomUUID();

            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("uuid", devUuid);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertEquals(devUuid.toString(), createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }
        // Name + description + uuid
        {
            final String devName = "testDevice2";
            final String devDescription = "testDescr2";

            final UUID devUuid = UUID.randomUUID();

            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("description", devDescription);
            createDevParams.add("uuid", devUuid);

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertEquals(devUuid.toString(), createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(devDescription, createdDev.getDescription());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }

    }

    @Test
    public final void testNewDeviceWithUuidWithSize() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        // Name + size + uuid
        {
            final String devName = "testDevice1";
            final UUID devUuid = UUID.randomUUID();

            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("size", 4096);
            createDevParams.add("uuid", devUuid.toString());

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertEquals(devUuid.toString(), createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }

        // Name + description + size + uuid
        {
            final String devName = "testDevice2";
            final String devDescription = "testDescr2";
            final UUID devUuid = UUID.randomUUID();

            final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
            createDevParams.add("name", devName);
            createDevParams.add("description", devDescription);
            createDevParams.add("size", 4096);
            createDevParams.add("uuid", devUuid.toString());

            final WebResource createdDevRes = createDevice(snapshotResource, createDevParams);

            assertNotNull(createdDevRes);

            final Device createdDev = prebuildRequest(createdDevRes, null).get(Device.class);
            assertNotNull(createdDev);
            assertEquals(devUuid.toString(), createdDev.getUuid());
            assertEquals(devName, createdDev.getName());
            assertEquals(devDescription, createdDev.getDescription());
            assertEquals(snapshot.getUuid(), createdDev.getParent());
        }
    }

    @Test
    public final void testNewDeviceFailSize() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        final String devName = "testDevice";

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", devName);
        createDevParams.add("size", -1234);

        final WebResource createDevResource = snapshotResource.path(CREATE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(createDevResource, createDevParams).post(
                ClientResponse.class);
        assertEquals(Status.FORBIDDEN.getStatusCode(), createResponse.getStatus());

    }

    @Test
    public final void testNewDeviceFailUuid() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        final String devName = "testDevice";

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", devName);
        createDevParams.add("uuid", "uuid");

        final WebResource createDevResource = snapshotResource.path(CREATE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(createDevResource, createDevParams).post(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());

    }

    @Test
    public final void testNewDeviceFailName() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final Snapshot snapshot = prebuildRequest(snapshotResource, null).get(Snapshot.class);
        assertNotNull(snapshot);

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.remove("name");

        final WebResource createDevResource = snapshotResource.path(CREATE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(createDevResource, createDevParams).post(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());

    }

    @Test
    public final void testGetDescendantDevicesNonRec() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final WebResource descDevicesRes = snapshotResource.path("devices");

        final MultivaluedMapImpl searchParams = new MultivaluedMapImpl();
        searchParams.add("recursive", "false");

        final DeviceList descDevList = prebuildRequest(descDevicesRes, searchParams).get(DeviceList.class);
        assertNotNull(descDevList);
        final List<Device> descDevices = descDevList.getDevices();
        assertNotNull(descDevices);
        assertEquals(1, descDevices.size());

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        for (int i = 1; i <= 10; i++) {
            createDevParams.putSingle("name", classNamePrefix + " - child device " + i);
            final Device newDev = prebuildRequest(createDevice(snapshotResource, createDevParams), null).get(
                    Device.class);

            final List<Device> modifiedList = prebuildRequest(descDevicesRes, searchParams).get(DeviceList.class)
                    .getDevices();
            assertEquals(i + 1, modifiedList.size());
            boolean listContainsDev = false;
            for (final Device currDev : modifiedList) {
                listContainsDev |= newDev.getUuid().equals(currDev.getUuid());
            }
            assertTrue(listContainsDev);
        }

        List<Device> completeList = prebuildRequest(descDevicesRes, searchParams).get(DeviceList.class).getDevices();
        final WebResource vvrDevResource = getVvrResource().path("devices");
        for (int j = completeList.size() - 1; j >= 0; j--) {
            final Device target = completeList.get(j);
            final WebResource devRes = vvrDevResource.path(target.getUuid());
            assertNotNull(getResultFromTask(prebuildRequest(devRes, null).delete(ClientResponse.class),
                    TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S));

            completeList = prebuildRequest(descDevicesRes, searchParams).get(DeviceList.class).getDevices();
            assertEquals(j, completeList.size());
        }
    }

    @Test
    public final void testGetChildSnapshots() throws TimeoutException {

        final WebResource snapshotResource = getSnapshotResource();

        final WebResource childSnapshotsRes = snapshotResource.path("snapshots");

        final SnapshotList childSnapList = prebuildRequest(childSnapshotsRes, null).get(SnapshotList.class);
        assertNotNull(childSnapList);
        final List<Snapshot> childSnapshots = childSnapList.getSnapshots();
        assertNotNull(childSnapshots);
        assertTrue(childSnapshots.isEmpty());

        final MultivaluedMapImpl createParams = new MultivaluedMapImpl();
        for (int i = 1; i <= 10; i++) {
            createParams.putSingle("name", classNamePrefix + " - device for child snapshot " + i);
            final WebResource newDevRes = createDevice(snapshotResource, createParams);

            createParams.putSingle("name", classNamePrefix + " - child snapshot " + i);
            final Snapshot newSnap = prebuildRequest(createSnapshot(newDevRes, createParams), null).get(Snapshot.class);

            final List<Snapshot> modifiedList = prebuildRequest(childSnapshotsRes, null).get(SnapshotList.class)
                    .getSnapshots();
            assertEquals(i, modifiedList.size());
            boolean listContainsSnap = false;
            for (final Snapshot currSnap : modifiedList) {
                listContainsSnap |= newSnap.getUuid().equals(currSnap.getUuid());
            }
            assertTrue(listContainsSnap);
        }

        List<Snapshot> completeList = prebuildRequest(childSnapshotsRes, null).get(SnapshotList.class).getSnapshots();
        final WebResource vvrSnapResource = getVvrResource().path("snapshots");
        for (int j = completeList.size() - 1; j >= 0; j--) {
            final Snapshot target = completeList.get(j);
            final WebResource devRes = vvrSnapResource.path(target.getUuid());
            assertNotNull(getResultFromTask(prebuildRequest(devRes, null).delete(ClientResponse.class),
                    TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S));

            completeList = prebuildRequest(childSnapshotsRes, null).get(SnapshotList.class).getSnapshots();
            assertEquals(j, completeList.size());
        }
    }

}
