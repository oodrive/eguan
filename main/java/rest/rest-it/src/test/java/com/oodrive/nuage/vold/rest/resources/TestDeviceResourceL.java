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

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.google.common.base.Strings;
import com.oodrive.nuage.vold.rest.generated.model.ConnectionInfo;
import com.oodrive.nuage.vold.rest.generated.model.Device;
import com.oodrive.nuage.vold.rest.generated.model.Snapshot;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public final class TestDeviceResourceL extends AbstractVvrResourceTest {

    private static final int DEFAULT_ISCSI_SERVER_PORT = 3260;
    private static final int DEFAULT_NBD_SERVER_PORT = 10809;

    private static final String classNamePrefix = TestDeviceResourceL.class.getSimpleName();

    public TestDeviceResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetDevice() throws TimeoutException {

        final WebResource rootSnapResource = getVvrResource().path("root");
        assertNotNull(rootSnapResource);

        final String devName = classNamePrefix + "-getDevice";
        final long devSize = DEFAULT_DEVICE_SIZE;

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", devName);
        createDevParams.add("size", devSize);

        final WebResource deviceResource = createDevice(rootSnapResource, createDevParams);
        assertNotNull(deviceResource);

        final Device result = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(result);
        assertNotNull(result.getUuid());
        assertEquals(devSize, result.getSize());
        assertEquals(devName, result.getName());
    }

    @Test
    public final void testPostDevice() throws TimeoutException, JAXBException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device originalDev = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(originalDev);
        assertNotNull(originalDev.getUuid());

        final Device modifiedDev = deviceReplicator.replicate(originalDev);
        assertFalse(originalDev == modifiedDev);
        modifiedDev.setName(classNamePrefix + " - modified device");
        modifiedDev.setDescription(classNamePrefix + " is now modified ");

        final ClientResponse postResponse = prebuildRequest(deviceResource, null).post(ClientResponse.class,
                modifiedDev);
        assertEquals(Status.OK.getStatusCode(), postResponse.getStatus());

        final Device updatedDev = postResponse.getEntity(Device.class);
        assertNotNull(updatedDev);
        assertEquals(originalDev.getUuid(), updatedDev.getUuid());
        assertEquals(originalDev.getSize(), updatedDev.getSize());
        assertEquals(modifiedDev.getName(), updatedDev.getName());
        assertEquals(modifiedDev.getDescription(), updatedDev.getDescription());

    }

    @Test
    public final void testPostDeviceFailNull() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final ClientResponse postResponse = prebuildRequest(deviceResource, null).post(ClientResponse.class, null);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), postResponse.getStatus());

    }

    @Test
    public final void testPostDeviceFailIdChanged() throws TimeoutException, JAXBException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device originalDev = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(originalDev);
        assertNotNull(originalDev.getUuid());

        final Device modifiedDev = deviceReplicator.replicate(originalDev);
        assertFalse(originalDev == modifiedDev);
        modifiedDev.setName(classNamePrefix + " - modified device");
        modifiedDev.setDescription(classNamePrefix + " is now modified ");

        // modifies the ID
        modifiedDev.setUuid(UUID.randomUUID().toString());

        final ClientResponse postResponse = prebuildRequest(deviceResource, null).post(ClientResponse.class,
                modifiedDev);
        assertEquals(Status.FORBIDDEN.getStatusCode(), postResponse.getStatus());

        final Device readDev = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(readDev);
        assertEquals(originalDev.getUuid(), readDev.getUuid());
        assertEquals(originalDev.getSize(), readDev.getSize());
        assertEquals(originalDev.getName(), readDev.getName());
        assertEquals(originalDev.getDescription(), readDev.getDescription());
    }

    @Test
    public final void testDeleteDevice() throws IllegalStateException, TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device targetDev = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(targetDev);
        assertNotNull(targetDev.getUuid());

        final ClientResponse deleteResponse = prebuildRequest(deviceResource, null).delete(ClientResponse.class);

        final WebResource deletedDeviceRes = getResultFromTask(deleteResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final ClientResponse getResponse = prebuildRequest(deletedDeviceRes, null).get(ClientResponse.class);
        assertEquals(Status.NOT_FOUND.getStatusCode(), getResponse.getStatus());
    }

    @Test
    public final void testDeleteDeviceFailDeviceActive() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final WebResource activatedDevRes = activateDevice(deviceResource, false);

        final ClientResponse deleteResponse = prebuildRequest(activatedDevRes, null).delete(ClientResponse.class);

        assertEquals(Status.FORBIDDEN.getStatusCode(), deleteResponse.getStatus());
    }

    @Test
    public final void testActivateDeactivateDevice() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device inactiveDevice = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(inactiveDevice);
        assertFalse(inactiveDevice.isActive());
        assertFalse(inactiveDevice.isReadOnly());

        final WebResource activatedDevRes = activateDevice(deviceResource, false);

        final Device activatedDevice = prebuildRequest(activatedDevRes, null).get(Device.class);
        assertNotNull(activatedDevice);
        assertEquals(inactiveDevice.getUuid(), activatedDevice.getUuid());
        assertTrue(activatedDevice.isActive());
        assertFalse(activatedDevice.isReadOnly());

        final WebResource deactivatedDevRes = deactivateDevice(activatedDevRes);

        final Device deactivatedDevice = prebuildRequest(deactivatedDevRes, null).get(Device.class);
        assertNotNull(deactivatedDevice);
        assertEquals(deactivatedDevice.getUuid(), activatedDevice.getUuid());
        assertFalse(deactivatedDevice.isActive());
        assertFalse(activatedDevice.isReadOnly());

        final WebResource twiceDeactivatedDevRes = deactivateDevice(activatedDevRes);

        final Device twiceDeactivatedDevice = prebuildRequest(twiceDeactivatedDevRes, null).get(Device.class);
        assertNotNull(twiceDeactivatedDevice);
        assertEquals(twiceDeactivatedDevice.getUuid(), activatedDevice.getUuid());
        assertFalse(twiceDeactivatedDevice.isActive());
        assertFalse(activatedDevice.isReadOnly());

    }

    @Test
    public final void testActivateDeviceFailActive() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final WebResource activatedDevRes = activateDevice(deviceResource, false);
        final Device activatedDevice = prebuildRequest(activatedDevRes, null).get(Device.class);
        assertNotNull(activatedDevice);
        assertTrue(activatedDevice.isActive());
        assertFalse(activatedDevice.isReadOnly());

        final WebResource activateActionRes = deviceResource.path("action/activate");

        final MultivaluedMapImpl activateParams = new MultivaluedMapImpl();
        activateParams.add("readOnly", false);

        final ClientResponse activateResponse = prebuildRequest(activateActionRes, activateParams).post(
                ClientResponse.class);

        assertEquals(Status.FORBIDDEN.getStatusCode(), activateResponse.getStatus());
    }

    @Test
    public final void testResizeDevice() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device originalDevice = prebuildRequest(deviceResource, null).get(Device.class);
        assertNotNull(originalDevice);
        assertFalse(originalDevice.isActive());
        final long origSize = originalDevice.getSize();
        assertTrue(origSize >= 0);

        final long inactiveAugmentedSize = origSize + DEFAULT_DEVICE_SIZE;
        final long inactiveReducedSize = Math.max(origSize, DEFAULT_DEVICE_SIZE / 2);

        final WebResource resizeActionRes = deviceResource.path("action/resize");

        final MultivaluedMapImpl resizeParams = new MultivaluedMapImpl();
        resizeParams.add("size", inactiveAugmentedSize);

        final WebResource inactiveAugmentedDeviceRes = getResultFromTask(prebuildRequest(resizeActionRes, resizeParams)
                .post(ClientResponse.class), TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
        assertNotNull(inactiveAugmentedDeviceRes);

        final Device inactiveAugmentedDevice = prebuildRequest(inactiveAugmentedDeviceRes, null).get(Device.class);
        assertEquals(originalDevice.getUuid(), inactiveAugmentedDevice.getUuid());
        assertEquals(inactiveAugmentedSize, inactiveAugmentedDevice.getSize());

        resizeParams.putSingle("size", inactiveReducedSize);
        final WebResource inactiveReducedDeviceRes = getResultFromTask(prebuildRequest(resizeActionRes, resizeParams)
                .post(ClientResponse.class), TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final Device inactiveReducedDevice = prebuildRequest(inactiveReducedDeviceRes, null).get(Device.class);
        assertEquals(originalDevice.getUuid(), inactiveReducedDevice.getUuid());
        assertEquals(inactiveReducedSize, inactiveReducedDevice.getSize());

        final WebResource activeDeviceRes = activateDevice(deviceResource, false);

        final Device activeDevice = prebuildRequest(activeDeviceRes, null).get(Device.class);
        assertNotNull(activeDevice);
        assertTrue(activeDevice.isActive());

        final long activeNewSize = inactiveReducedSize + DEFAULT_DEVICE_SIZE;

        resizeParams.putSingle("size", activeNewSize);

        final WebResource activeResizedDeviceRes = getResultFromTask(prebuildRequest(resizeActionRes, resizeParams)
                .post(ClientResponse.class), TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
        assertNotNull(activeResizedDeviceRes);

        final Device activeResizedDevice = prebuildRequest(inactiveAugmentedDeviceRes, null).get(Device.class);
        assertEquals(originalDevice.getUuid(), activeResizedDevice.getUuid());
        assertEquals(activeNewSize, activeResizedDevice.getSize());
    }

    @Test
    public final void testResizeDeviceFailMissingSize() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final WebResource resizeActionRes = deviceResource.path("action/resize");

        final ClientResponse resizeResponse = prebuildRequest(resizeActionRes, null).post(ClientResponse.class);
        assertEquals(Status.FORBIDDEN.getStatusCode(), resizeResponse.getStatus());
    }

    @Test
    public final void testResizeDeviceFailNegativeSize() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final MultivaluedMapImpl resizeParams = new MultivaluedMapImpl();
        resizeParams.add("size", (-1) * DEFAULT_DEVICE_SIZE);

        final WebResource resizeActionRes = deviceResource.path("action/resize");

        final ClientResponse resizeResponse = prebuildRequest(resizeActionRes, resizeParams).post(ClientResponse.class);
        assertEquals(Status.FORBIDDEN.getStatusCode(), resizeResponse.getStatus());
    }

    @Test
    public final void testResizeDeviceFailSizeReducedAndActive() throws TimeoutException {

        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final long startSize = 2 * DEFAULT_DEVICE_SIZE;

        final WebResource resizeActionRes = deviceResource.path("action/resize");

        final MultivaluedMapImpl resizeParams = new MultivaluedMapImpl();
        resizeParams.add("size", startSize);

        final WebResource startDeviceRes = getResultFromTask(
                prebuildRequest(resizeActionRes, resizeParams).post(ClientResponse.class), TimeUnit.SECONDS,
                DEFAULT_TASK_TIMEOUT_S);
        assertNotNull(startDeviceRes);

        final Device startDevice = prebuildRequest(startDeviceRes, null).get(Device.class);
        assertFalse(startDevice.isActive());
        assertEquals(startSize, startDevice.getSize());

        activateDevice(startDeviceRes, false);

        resizeParams.putSingle("size", startSize / 2);

        final ClientResponse resizeResponse = prebuildRequest(resizeActionRes, resizeParams).post(ClientResponse.class);
        assertEquals(Status.FORBIDDEN.getStatusCode(), resizeResponse.getStatus());
    }

    @Test
    public final void testCreateSnapshotNoUuid() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        // Name only
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "defSnap1");
            final WebResource newSnapRes = createSnapshot(deviceResource, createSnapParams);
            final Snapshot readSnap = prebuildRequest(newSnapRes, null).get(Snapshot.class);
            assertEquals("defSnap1", readSnap.getName());
        }
        // Name + description
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "defSnap2");
            createSnapParams.add("description", "descriptionSnap2");

            final WebResource newSnapRes = createSnapshot(deviceResource, createSnapParams);
            final Snapshot readSnap = prebuildRequest(newSnapRes, null).get(Snapshot.class);
            assertEquals("defSnap2", readSnap.getName());
            assertEquals("descriptionSnap2", readSnap.getDescription());
        }
    }

    @Test
    public final void testCreateSnapshotWithUuid() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);
        // Name + uuid
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "defSnap1");
            final UUID snapUuid = UUID.randomUUID();
            createSnapParams.add("uuid", snapUuid.toString());
            final WebResource newSnapRes = createSnapshot(deviceResource, createSnapParams);
            final Snapshot readSnap = prebuildRequest(newSnapRes, null).get(Snapshot.class);
            assertEquals("defSnap1", readSnap.getName());
            assertEquals(snapUuid.toString(), readSnap.getUuid());
        }
        // Name + uuid + description
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "defSnap2");
            createSnapParams.add("description", "descriptionSnap2");
            final UUID snapUuid = UUID.randomUUID();
            createSnapParams.add("uuid", snapUuid.toString());
            final WebResource newSnapRes = createSnapshot(deviceResource, createSnapParams);
            final Snapshot readSnap = prebuildRequest(newSnapRes, null).get(Snapshot.class);
            assertEquals("defSnap2", readSnap.getName());
            assertEquals("descriptionSnap2", readSnap.getDescription());
            assertEquals(snapUuid.toString(), readSnap.getUuid());
        }

    }

    @Test
    public final void testCreateSnapshotNoName() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
        createSnapParams.remove("name");

        final WebResource createSnapResource = deviceResource.path(CREATE_SNAPSHOT_PATH);

        ClientResponse createResponse = prebuildRequest(createSnapResource, createSnapParams)
                .post(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());

        final UUID devUuid = UUID.randomUUID();
        createSnapParams.add("uuid", devUuid.toString());

        createResponse = prebuildRequest(createSnapResource, createSnapParams).post(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());
    }

    @Test
    public final void testCreateSnapshotBadUuid() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
        createSnapParams.add("name", "defSnap");
        createSnapParams.add("uuid", "uuid");

        final WebResource createSnapResource = deviceResource.path(CREATE_SNAPSHOT_PATH);

        final ClientResponse createResponse = prebuildRequest(createSnapResource, createSnapParams).post(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());
    }

    /**
     * Test successful iSCSI connection information retrieval.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionIscsi() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getIscsiConnectParams = new MultivaluedMapImpl();
        getIscsiConnectParams.add("ip", "198.51.100.24");
        getIscsiConnectParams.add("clientProtocol", "iscsi");

        final ClientResponse iscsiConnectResponse = prebuildRequest(connectionResource, getIscsiConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), iscsiConnectResponse.getStatus());
        assertTrue(iscsiConnectResponse.hasEntity());
        final ConnectionInfo iscsiConnectInfo = iscsiConnectResponse.getEntity(ConnectionInfo.class);
        assertEquals("iscsi", iscsiConnectInfo.getDriverVolumeType());
        assertFalse(Strings.isNullOrEmpty(iscsiConnectInfo.getServerAddress()));
        assertEquals(Integer.valueOf(DEFAULT_ISCSI_SERVER_PORT), iscsiConnectInfo.getServerPort());
        assertFalse(Strings.isNullOrEmpty(iscsiConnectInfo.getIqn()));
        assertFalse(Strings.isNullOrEmpty(iscsiConnectInfo.getIscsiAlias()));
    }

    /**
     * Test successful NBD connection information retrieval.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionNbd() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getNbdConnectParams = new MultivaluedMapImpl();
        getNbdConnectParams.add("ip", "203.0.113.130");
        getNbdConnectParams.add("clientProtocol", "nbd");

        final ClientResponse nbdConnectResponse = prebuildRequest(connectionResource, getNbdConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), nbdConnectResponse.getStatus());
        assertTrue(nbdConnectResponse.hasEntity());
        final ConnectionInfo nbdConnectInfo = nbdConnectResponse.getEntity(ConnectionInfo.class);
        assertEquals("nbd", nbdConnectInfo.getDriverVolumeType());
        assertEquals(device.getName(), nbdConnectInfo.getDevName());
        assertFalse(Strings.isNullOrEmpty(nbdConnectInfo.getServerAddress()));
        assertEquals(Integer.valueOf(DEFAULT_NBD_SERVER_PORT), nbdConnectInfo.getServerPort());
        assertFalse(Strings.isNullOrEmpty(nbdConnectInfo.getDevName()));
    }

    /**
     * Test failed connection information retrieval due to an inactive device.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionFailNotActive() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertFalse(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getConnectParams = new MultivaluedMapImpl();
        getConnectParams.add("ip", "203.0.113.130");
        getConnectParams.add("clientProtocol", "iscsi");

        final ClientResponse connectResponse = prebuildRequest(connectionResource, getConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.FORBIDDEN.getStatusCode(), connectResponse.getStatus());
    }

    /**
     * Test failed connection information retrieval due to a missing client IP address.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionFailNoIp() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getConnectParams = new MultivaluedMapImpl();
        getConnectParams.add("clientProtocol", "iscsi");

        final ClientResponse connectResponse = prebuildRequest(connectionResource, getConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), connectResponse.getStatus());
    }

    /**
     * Test failed connection information retrieval due to an invalid client IP address.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionFailBadIp() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getConnectParams = new MultivaluedMapImpl();
        getConnectParams.add("ip", "to.the.unknown.h0$t");
        getConnectParams.add("clientProtocol", "iscsi");

        final ClientResponse connectResponse = prebuildRequest(connectionResource, getConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), connectResponse.getStatus());
    }

    /**
     * Test failed connection information retrieval due to a missing client protocol.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionFailNoProtocol() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getConnectParams = new MultivaluedMapImpl();
        getConnectParams.add("ip", "203.0.113.130");

        final ClientResponse connectResponse = prebuildRequest(connectionResource, getConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), connectResponse.getStatus());
    }

    /**
     * Test failed connection information retrieval due to an unsupported client protocol.
     * 
     * @throws TimeoutException
     *             if device setup fails, not part of this test
     */
    @Test
    public final void testGetConnectionFailBadProtocol() throws TimeoutException {
        final WebResource deviceResource = activateDevice(getDeviceResource(), false);
        assertNotNull(deviceResource);

        final Device device = prebuildRequest(deviceResource, null).get(Device.class);
        assertTrue(device.isActive());

        final WebResource connectionResource = deviceResource.path(CONNECTION_PATH);

        final MultivaluedMapImpl getConnectParams = new MultivaluedMapImpl();
        getConnectParams.add("ip", "203.0.113.130");
        getConnectParams.add("clientProtocol", "snailmail");

        final ClientResponse connectResponse = prebuildRequest(connectionResource, getConnectParams).get(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), connectResponse.getStatus());
    }

    @Test
    public final void testCloneDevice() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        // Name only
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "clone1");
            final WebResource newDeviceRes = cloneDevice(deviceResource, createSnapParams);
            final Device readClone = prebuildRequest(newDeviceRes, null).get(Device.class);
            assertEquals("clone1", readClone.getName());
        }
        // Name + description
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            createSnapParams.add("name", "clone2");
            createSnapParams.add("description", "desc2");
            final WebResource newDeviceRes = cloneDevice(deviceResource, createSnapParams);
            final Device readClone = prebuildRequest(newDeviceRes, null).get(Device.class);
            assertEquals("clone2", readClone.getName());
            assertEquals("desc2", readClone.getDescription());
        }
        // Name + uuid
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            final UUID cloneUuid = UUID.randomUUID();
            createSnapParams.add("name", "clone3");
            createSnapParams.add("uuid", cloneUuid);
            final WebResource newDeviceRes = cloneDevice(deviceResource, createSnapParams);
            final Device readClone = prebuildRequest(newDeviceRes, null).get(Device.class);
            assertEquals("clone3", readClone.getName());
            assertEquals(cloneUuid.toString(), readClone.getUuid());
        }
        // Name + uuid + description
        {
            final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
            final UUID cloneUuid = UUID.randomUUID();
            createSnapParams.add("name", "clone4");
            createSnapParams.add("description", "desc4");
            createSnapParams.add("uuid", cloneUuid);
            final WebResource newDeviceRes = cloneDevice(deviceResource, createSnapParams);
            final Device readClone = prebuildRequest(newDeviceRes, null).get(Device.class);
            assertEquals("clone4", readClone.getName());
            assertEquals("desc4", readClone.getDescription());
            assertEquals(cloneUuid.toString(), readClone.getUuid());
        }
    }

    @Test
    public final void testCloneDeviceNoName() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final MultivaluedMapImpl cloneDeviceParams = new MultivaluedMapImpl();
        cloneDeviceParams.remove("name");

        final WebResource cloneResource = deviceResource.path(CLONE_DEVICE_PATH);

        // Clone with no param
        ClientResponse cloneResponse = prebuildRequest(cloneResource, cloneDeviceParams).post(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), cloneResponse.getStatus());

        // try to clone only with uuid
        final UUID devUuid = UUID.randomUUID();
        cloneDeviceParams.add("uuid", devUuid);

        cloneResponse = prebuildRequest(cloneResource, cloneDeviceParams).post(ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), cloneResponse.getStatus());
    }

    @Test
    public final void testCloneDeviceBadUuid() throws TimeoutException {
        final WebResource deviceResource = getDeviceResource();
        assertNotNull(deviceResource);

        final MultivaluedMapImpl cloneDeviceParams = new MultivaluedMapImpl();
        cloneDeviceParams.add("name", "clone");
        cloneDeviceParams.add("uuid", "uuid");

        final WebResource cloneResource = deviceResource.path(CLONE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(cloneResource, cloneDeviceParams).post(
                ClientResponse.class);
        assertEquals(Status.BAD_REQUEST.getStatusCode(), createResponse.getStatus());
    }

}
