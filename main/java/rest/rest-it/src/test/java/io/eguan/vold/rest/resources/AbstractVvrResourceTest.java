package io.eguan.vold.rest.resources;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.vold.rest.generated.model.Device;
import io.eguan.vold.rest.generated.model.Snapshot;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.runners.model.InitializationError;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Abstract superclass for all tests whose activity is limited to one VVR.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public abstract class AbstractVvrResourceTest extends AbstractVvrsResourceTest {

    protected static final long DEFAULT_DEVICE_SIZE = 10737418240l;

    static final String CREATE_SNAPSHOT_PATH = "action/newSnapshot";

    static final String CREATE_DEVICE_PATH = "action/newDevice";

    static final String CLONE_DEVICE_PATH = "action/clone";

    static final String CONNECTION_PATH = "connection";

    protected static final JaxbPojoReplicator<Snapshot> snapshotReplicator = new JaxbPojoReplicator<>(Snapshot.class);

    protected static final JaxbPojoReplicator<Device> deviceReplicator = new JaxbPojoReplicator<>(Device.class);

    private WebResource testVvrRes;

    public AbstractVvrResourceTest(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Before
    public final void setUp() throws InitializationError, InterruptedException {
        try {
            this.testVvrRes = startVvr(createVvr(getVvrsResource(), null), null);
        }
        catch (final TimeoutException e) {
            throw new InitializationError(e);
        }
    }

    protected final WebResource getVvrResource() {
        return testVvrRes;
    }

    protected static final WebResource createDevice(final WebResource snapshotResource,
            final MultivaluedMap<String, String> queryParams) throws TimeoutException {

        final WebResource createDevResource = snapshotResource.path(CREATE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(createDevResource, queryParams)
                .post(ClientResponse.class);

        return getResultFromTask(createResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
    }

    protected static final ClientResponse createDevice(final WebResource vvrResource) {

        final WebResource rootSnapRes = vvrResource.path("root");
        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", "defDevice");
        createDevParams.add("size", DEFAULT_DEVICE_SIZE);
        final WebResource createDevResource = rootSnapRes.path(CREATE_DEVICE_PATH);
        final ClientResponse createResponse = prebuildRequest(createDevResource, createDevParams).post(
                ClientResponse.class);
        return createResponse;
    }

    protected final WebResource getSnapshotResource() throws TimeoutException {
        final WebResource vvrResource = getVvrResource();

        final WebResource rootSnapRes = vvrResource.path("root");

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", "defSnapCreate");
        createDevParams.add("size", DEFAULT_DEVICE_SIZE);

        final WebResource devResource = createDevice(rootSnapRes, createDevParams);

        final MultivaluedMapImpl createSnapParams = new MultivaluedMapImpl();
        createSnapParams.add("name", "defSnap");

        return createSnapshot(devResource, createSnapParams);
    }

    protected final WebResource getDeviceResource() throws TimeoutException {
        final WebResource vvrResource = getVvrResource();

        final WebResource rootSnapRes = vvrResource.path("root");

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", "defDevice");
        createDevParams.add("size", DEFAULT_DEVICE_SIZE);

        return createDevice(rootSnapRes, createDevParams);
    }

    protected static final WebResource createSnapshot(final WebResource deviceResource,
            final MultivaluedMap<String, String> queryParams) throws TimeoutException {
        final WebResource createSnapResource = deviceResource.path(CREATE_SNAPSHOT_PATH);

        final ClientResponse createResponse = prebuildRequest(createSnapResource, queryParams).post(
                ClientResponse.class);
        return getResultFromTask(createResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
    }

    protected static final WebResource cloneDevice(final WebResource deviceResource,
            final MultivaluedMap<String, String> queryParams) throws TimeoutException {
        final WebResource createDeviceResource = deviceResource.path(CLONE_DEVICE_PATH);

        final ClientResponse createResponse = prebuildRequest(createDeviceResource, queryParams).post(
                ClientResponse.class);
        return getResultFromTask(createResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
    }

}
