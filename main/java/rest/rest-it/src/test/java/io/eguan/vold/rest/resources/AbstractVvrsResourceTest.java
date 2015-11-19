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

import static com.sun.jersey.api.client.ClientResponse.Status.ACCEPTED;
import static com.sun.jersey.api.client.ClientResponse.Status.NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.vold.rest.generated.model.Device;
import io.eguan.vold.rest.generated.model.DeviceList;
import io.eguan.vold.rest.generated.model.ExecState;
import io.eguan.vold.rest.generated.model.Task;
import io.eguan.vold.rest.generated.model.TaskList;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepository;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepositoryList;
import io.eguan.vold.rest.generated.resources.VvrsResource;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Abstract class providing a common REST client setup.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
@RunWith(Parameterized.class)
public abstract class AbstractVvrsResourceTest extends AbstractResourceTest {

    private static final String VVRS_RESOURCE_URI = SERVER_BASE_URI + "/"
            + VvrsResource.class.getAnnotation(Path.class).value();

    static final String CREATE_VVR_PATH = "action/createVvr";

    static final String START_VVR_PATH = "action/start";

    static final String STOP_VVR_PATH = "action/stop";

    static final String TASK_VVRS_PATH = VVRS_RESOURCE_URI + "/tasks";

    /**
     * The default task timeout in seconds.
     */
    protected static final int DEFAULT_TASK_TIMEOUT_S = 25;

    /**
     * Common POJO replicator.
     * 
     * @see JaxbPojoReplicator
     */
    protected static final JaxbPojoReplicator<VersionedVolumeRepository> VVR_REPLICATOR = new JaxbPojoReplicator<>(
            VersionedVolumeRepository.class);

    private static Client client;

    private static MultivaluedMapImpl mandatoryQueryParams;

    private static MediaType contentType = MediaType.APPLICATION_XML_TYPE;

    private static MediaType acceptType = MediaType.APPLICATION_XML_TYPE;

    private static final int TASK_WAIT_MS = 100;

    private static final int NOT_FOUND_RETRY_COUNT = 100;

    private static final int NOT_FOUND_WAIT_DELAY_MS = 200;

    /**
     * Internal constructor with content and response type arguments.
     * 
     * @param runContentType
     *            the content type to send in requests
     * @param runAcceptType
     *            the accepted response type
     */
    protected AbstractVvrsResourceTest(final MediaType runContentType, final MediaType runAcceptType) {
        contentType = runContentType;
        acceptType = runAcceptType;
    }

    /**
     * Test parameter generator method.
     * 
     * @return all test parameter combinations to test
     */
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE },
                { MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_XML_TYPE } });
    }

    /**
     * Gets the root 'vvrs' resource.
     * 
     * @return a valid {@link WebResource}
     */
    protected final WebResource getVvrsResource() {
        return client.resource(VVRS_RESOURCE_URI);
    }

    /**
     * Gets the common vvrs task resource.
     * 
     * @return a valid {@link WebResource}
     */
    protected final WebResource getVvrsTasksResource() {
        return client.resource(TASK_VVRS_PATH);
    }

    /**
     * Sets up the Jersey client.
     */
    @BeforeClass
    public static final void setUpJerseyClient() {
        final ClientConfig cfg = new DefaultClientConfig();
        cfg.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

        client = Client.create(cfg);
        mandatoryQueryParams = new MultivaluedMapImpl();
        mandatoryQueryParams.add("ownerId", Objects.requireNonNull(getOwnerUuid()));
    }

    /**
     * Sets up and cleans the root vvrs resource.
     */
    @Before
    public final void setUpVvrsResource() {
        clearVvrsResource(getOwnerUuid(), getVvrsResource());
    }

    /**
     * Tears down leftover sub-resources of the root vvrs resource.
     */
    @After
    public final void tearDownVvrsResource() {
        clearVvrsResource(getOwnerUuid(), getVvrsResource());
    }

    /**
     * Prepares a request with common mandatory parameters and optional query parameters.
     * 
     * @param targetResource
     *            the target {@link WebResource}
     * @param additionalQueryParams
     *            optional query parameters
     * @return a preconfigured
     */
    protected static final Builder prebuildRequest(final WebResource targetResource,
            final MultivaluedMap<String, String> additionalQueryParams) {
        WebResource parameterizedResource = targetResource.queryParams(mandatoryQueryParams);
        if (additionalQueryParams != null) {
            parameterizedResource = parameterizedResource.queryParams(additionalQueryParams);
        }
        return parameterizedResource.accept(acceptType).type(contentType);
    }

    /**
     * Extracts the task resource from the given response to a task-creating request.
     * 
     * @param taskCreateResponse
     *            a {@link ClientResponse} received as response to a task-creating (POST) request
     * @return a valid {@link WebResource} pointing to the created task
     * @throws IllegalArgumentException
     *             if the provided response does not have the proper return code or its location header is empty
     */
    protected static final WebResource getTaskResourceFromAcceptedResponse(final ClientResponse taskCreateResponse)
            throws IllegalArgumentException {
        final int responseStatus = taskCreateResponse.getStatus();
        if (ACCEPTED.getStatusCode() != responseStatus) {
            throw new IllegalArgumentException("Response status is not 'accepted'; responseStatus="
                    + Status.fromStatusCode(responseStatus));
        }

        final List<String> locationHeaders = taskCreateResponse.getHeaders().get("location");
        if (locationHeaders.isEmpty()) {
            throw new IllegalArgumentException("No location headers found");
        }

        return client.resource(locationHeaders.get(0));
    }

    /**
     * Utility method to wait for the result produced by a task.
     * 
     * @param taskCreateResponse
     *            the {@link ClientResponse} pointing to the newly created task
     * @param timeUnit
     *            a {@link TimeUnit} to measure the timeout in
     * @param timeout
     *            a timeout after which to stop waiting for the result
     * @return a {@link WebResource} pointing to the result
     * @throws TimeoutException
     *             if the specified timeout is reached before the result could be obtained
     * @throws IllegalStateException
     *             if the task fails or does not produce a result
     */
    protected static final WebResource getResultFromTask(final ClientResponse taskCreateResponse,
            final TimeUnit timeUnit, final long timeout) throws IllegalStateException, TimeoutException {
        return getResultFromTask(taskCreateResponse, timeUnit, timeout, false);
    }

    /**
     * Utility method to wait for the result produced by a task.
     * 
     * @param taskCreateResponse
     *            the {@link ClientResponse} pointing to the newly created task
     * @param timeUnit
     *            a {@link TimeUnit} to measure the timeout in
     * @param timeout
     *            a timeout after which to stop waiting for the result
     * @param waitForResult
     *            whether to wait with the same timeout until the resulting resource appears before returning
     * @return a {@link WebResource} pointing to the result
     * @throws TimeoutException
     *             if the specified timeout is reached before the result could be obtained
     * @throws IllegalStateException
     *             if the task fails or does not produce a result
     */
    protected static final WebResource getResultFromTask(final ClientResponse taskCreateResponse,
            final TimeUnit timeUnit, final long timeout, final boolean waitForResult) throws IllegalStateException,
            TimeoutException {

        final WebResource taskRes = getTaskResourceFromAcceptedResponse(taskCreateResponse);

        final long millisTimeout = timeUnit.toMillis(timeout) + System.currentTimeMillis();

        String resultRef;
        boolean resultReferenced = false;
        ExecState state;
        boolean keepWaiting;
        do {

            if (System.currentTimeMillis() > millisTimeout) {
                throw new TimeoutException();
            }

            final Task task = prebuildRequest(taskRes, null).get(Task.class);

            state = task.getState();

            resultRef = task.getResultRef();

            resultReferenced = (resultRef != null);

            if (state == ExecState.FAILED) {
                throw new IllegalStateException("Task failed; uuid=" + task.getUuid());
            }

            if ((state == ExecState.DONE) && !resultReferenced) {
                throw new IllegalStateException("Task done without producing a result; uuid=" + task.getUuid());
            }

            keepWaiting = !resultReferenced || state == ExecState.IN_PROGRESS || state == ExecState.PENDING;
            if (keepWaiting) {
                try {
                    Thread.sleep(TASK_WAIT_MS);
                }
                catch (final InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }

        } while (keepWaiting);

        final WebResource result = client.resource(resultRef);
        if (!waitForResult) {
            return result;
        }

        ClientResponse checkResponse = prebuildRequest(result, null).get(ClientResponse.class);
        final int statusOk = Status.OK.getStatusCode();
        while (checkResponse.getStatus() != statusOk) {

            if (System.currentTimeMillis() > millisTimeout) {
                throw new TimeoutException();
            }

            try {
                Thread.sleep(TASK_WAIT_MS);
            }
            catch (final InterruptedException e) {
                throw new IllegalStateException(e);
            }
            checkResponse = prebuildRequest(result, null).get(ClientResponse.class);
        }

        return result;

    }

    /**
     * Creates a VVR resource.
     * 
     * @param vvrsResource
     *            the parent vvrs resource
     * @param additionalQueryParams
     *            query parameters to include in the creation request
     * @return a {@link WebResource} pointing to the result
     * @throws TimeoutException
     *             if the creation task does not complete within {@value #DEFAULT_TASK_TIMEOUT_S} seconds
     */
    protected static final WebResource createVvr(final WebResource vvrsResource,
            final MultivaluedMap<String, String> additionalQueryParams) throws TimeoutException {
        final ClientResponse createResponse = prebuildRequest(vvrsResource.path(CREATE_VVR_PATH), additionalQueryParams)
                .post(ClientResponse.class, null);
        return getResultFromTask(createResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S, true);
    }

    /**
     * Starts the given VVR.
     * 
     * @param vvrResource
     *            the target VVR resource
     * @param additionalQueryParams
     *            query parameters to include in the start request
     * @return a {@link WebResource} pointing to the started VVR
     * @throws TimeoutException
     *             if the start task does not complete within {@value #DEFAULT_TASK_TIMEOUT_S} seconds
     * @throws InterruptedException
     *             if interrupted while waiting for the VVR resource to be available
     */
    protected static final WebResource startVvr(final WebResource vvrResource,
            final MultivaluedMap<String, String> additionalQueryParams) throws TimeoutException, InterruptedException {

        ClientResponse startResponse = prebuildRequest(vvrResource.path(START_VVR_PATH), additionalQueryParams).post(
                ClientResponse.class, null);

        int count = 0;
        while (NOT_FOUND.getStatusCode() == startResponse.getStatus() && count < NOT_FOUND_RETRY_COUNT) {
            Thread.sleep(NOT_FOUND_WAIT_DELAY_MS);
            count++;
            startResponse = prebuildRequest(vvrResource.path(START_VVR_PATH), additionalQueryParams).post(
                    ClientResponse.class, null);
        }
        final WebResource startedVvrRes = getResultFromTask(startResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);

        final VersionedVolumeRepository startedVvr = prebuildRequest(startedVvrRes, additionalQueryParams).get(
                VersionedVolumeRepository.class);

        assertTrue(startedVvr.isStarted());
        return startedVvrRes;
    }

    /**
     * Activates a given device.
     * 
     * @param deviceResource
     *            the {@link WebResource} representing the target device
     * @param readOnly
     *            whether to activate read-only
     * @return a {@link WebResource} representing the active device
     * @throws TimeoutException
     *             if the activation task does not complete within {@value #DEFAULT_TASK_TIMEOUT_S} seconds
     */
    protected static final WebResource activateDevice(final WebResource deviceResource, final boolean readOnly)
            throws TimeoutException {

        final WebResource activateActionRes = deviceResource.path("action/activate");

        final MultivaluedMapImpl activateParams = new MultivaluedMapImpl();
        activateParams.add("readOnly", readOnly);

        final ClientResponse activateResponse = prebuildRequest(activateActionRes, activateParams).post(
                ClientResponse.class);

        return getResultFromTask(activateResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
    }

    /**
     * Deactivates a given device.
     * 
     * @param deviceResource
     *            the {@link WebResource} representing the target device
     * @return a {@link WebResource} representing the inactive device
     * @throws TimeoutException
     *             if the deactivation task does not complete within {@value #DEFAULT_TASK_TIMEOUT_S} seconds
     */
    protected static final WebResource deactivateDevice(final WebResource deviceResource) throws TimeoutException {

        final WebResource deactivateActionRes = deviceResource.path("action/deactivate");

        final ClientResponse deactivateResponse = prebuildRequest(deactivateActionRes, null).post(ClientResponse.class);

        return getResultFromTask(deactivateResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
    }

    /**
     * Check all the tasks locally saved are present in the server returned list.
     * 
     * @param localTasks
     *            a {@link List} of local tasks
     * @param serverTasks
     *            a {@link List} of server-side tasks
     * @return <code>true</code> if all local tasks have been found in the server-side tasks
     */
    protected static final boolean checkTasks(final List<Task> localTasks, final List<Task> serverTasks) {
        // check that all the tasks locally saved are present in the server returned list
        boolean localTaskfound = false;
        for (final Task localTask : localTasks) {
            for (final Task serverTask : serverTasks) {
                if (localTask.getUuid().equals(serverTask.getUuid())) {
                    localTaskfound = true;
                    break;
                }
            }
        }
        return localTaskfound;
    }

    /**
     * Gets the list of tasks from a tasks resource.
     * 
     * @param tasksResource
     *            the target {@link WebResource}
     * @return a {@link List} of {@link Task}s
     */
    protected static final List<Task> getServerTasks(final WebResource tasksResource) {
        final TaskList serverTaskList = prebuildRequest(tasksResource, null).get(TaskList.class);
        assertNotNull(serverTaskList);
        final List<Task> newTaskList = serverTaskList.getTasks();
        assertNotNull(newTaskList);
        return newTaskList;
    }

    /**
     * Delete all sub-resources of the given vvrs resource.
     * 
     * @param ownerId
     *            the owner ID associated to the target resource
     * @param rootResource
     *            the target vvrs resource
     */
    private static final void clearVvrsResource(final String ownerId, final WebResource rootResource) {

        // clear all remaining vvr resources contained in the root vvrs resource
        final VersionedVolumeRepositoryList vvrsToDelete = prebuildRequest(rootResource, null).get(
                VersionedVolumeRepositoryList.class);
        try {
            for (final VersionedVolumeRepository currVvr : vvrsToDelete.getVersionedVolumeRepositories()) {

                final WebResource resource = rootResource.path(currVvr.getUuid());

                if (currVvr.isStarted()) {
                    deactivateAllDevices(resource);
                    final WebResource stoppedVvrRes = getResultFromTask(
                            prebuildRequest(resource.path("action/stop"), null).post(ClientResponse.class, null),
                            TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
                    final VersionedVolumeRepository stoppedVvr = prebuildRequest(stoppedVvrRes, null).get(
                            VersionedVolumeRepository.class);
                    assert !stoppedVvr.isStarted();
                }
                final ClientResponse deleteResponse = prebuildRequest(resource, null).delete(ClientResponse.class);
                assertEquals(Status.ACCEPTED.getStatusCode(), deleteResponse.getStatus());

                getResultFromTask(deleteResponse, TimeUnit.SECONDS, DEFAULT_TASK_TIMEOUT_S);
            }
        }
        catch (final TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Deactivates all devices in a given VVR resource.
     * 
     * @param vvrResource
     *            the target resource
     * @throws TimeoutException
     *             if any of the deactivation tasks does not complete within {@value #DEFAULT_TASK_TIMEOUT_S} seconds
     */
    private static final void deactivateAllDevices(final WebResource vvrResource) throws TimeoutException {
        final WebResource devicesRes = vvrResource.path("devices");
        for (final Device currDev : prebuildRequest(devicesRes, null).get(DeviceList.class).getDevices()) {
            if (!currDev.isActive()) {
                continue;
            }
            final WebResource deviceRes = devicesRes.path(currDev.getUuid());
            deactivateDevice(deviceRes);
        }
    }

}
