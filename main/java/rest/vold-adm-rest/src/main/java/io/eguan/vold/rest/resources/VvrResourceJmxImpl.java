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

import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.errors.ClientErrorFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepository;
import io.eguan.vold.rest.generated.resources.DevicesResource;
import io.eguan.vold.rest.generated.resources.RootSnapshotResource;
import io.eguan.vold.rest.generated.resources.SnapshotsResource;
import io.eguan.vold.rest.generated.resources.VvrResource;
import io.eguan.vold.rest.generated.resources.VvrStartResource;
import io.eguan.vold.rest.generated.resources.VvrStopResource;
import io.eguan.vold.rest.generated.resources.VvrTasksResource;
import io.eguan.vold.rest.util.InputValidation;
import io.eguan.vold.rest.util.ResourcePath;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link VvrResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class VvrResourceJmxImpl extends AbstractResource implements VvrResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(VvrsResourceJmxImpl.class);

    private final VvrMXBean vvrInstance;

    private VvrsResourceJmxImpl parentResource;

    private final URI resourceUri;

    private final String tasksResourcePath;

    VvrResourceJmxImpl(final VvrMXBean vvrProxy, final URI resourceUri) {
        this.vvrInstance = vvrProxy;
        this.resourceUri = resourceUri;
        this.tasksResourcePath = ResourcePath.extractPathForSubResourceLocator(this.getClass(), VvrTasksResource.class);
    }

    public final class VvrStartResourceJmxImpl implements VvrStartResource {

        @Override
        public final Response start(final String ownerId) {
            final VvrsTasksResourceJmxImpl tasksResource = getParentResource().getVvrsTasksResource();

            final String taskId;
            try {
                taskId = vvrInstance.startNoWait();
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("failed to start the vvr", "Exception start failed",
                        e);
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }
    }

    public final class VvrStopResourceJmxImpl implements VvrStopResource {

        @Override
        public final Response stop(final String ownerId) {
            final VvrsTasksResourceJmxImpl tasksResource = getParentResource().getVvrsTasksResource();

            final String taskId;
            try {
                taskId = vvrInstance.stopNoWait();
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException(e.getMessage(), "Illegal state to stop vvr");
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to stop vvr", "Exception stop vvr", e);
            }

            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }
    }

    @Override
    public final VersionedVolumeRepository getVvr(final String ownerId) {
        return VvrsResourceJmxImpl.getVvrPojoFromMbeanProxy(vvrInstance);
    }

    @Override
    public final VersionedVolumeRepository postVvr(final String ownerId, final VersionedVolumeRepository vvr)
            throws CustomResourceException {
        Objects.requireNonNull(vvr);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("POST on VVR " + vvrInstance.getUuid() + "; argument=" + vvr.toString());
        }

        boolean readOnlyChanged = false;
        readOnlyChanged |= !vvrInstance.getUuid().equals(vvr.getUuid());
        readOnlyChanged |= !vvrInstance.getOwnerUuid().equals(ownerId);

        if (readOnlyChanged) {
            LOGGER.warn("Detected read-only attribute change on VVR " + vvrInstance.getUuid());
            throw ClientErrorFactory.newForbiddenException("Read-only attribute(s) changed",
                    "Read-only attribute(s) change request on VVR " + vvrInstance.getUuid());
        }

        vvrInstance.setName(vvr.getName());
        vvrInstance.setDescription(vvr.getDescription());
        // FIXME: persist quota and instanceCount or remove them
        // vvrInstance.setQuota(vvr.getQuota());
        // vvrInstance.setInstanceCount(vvr.getInstanceCount());

        return VvrsResourceJmxImpl.getVvrPojoFromMbeanProxy(vvrInstance);
    }

    @Override
    public final Response deleteVvr(final String ownerId) {

        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);

        final VvrsTasksResourceJmxImpl taskResource = getParentResource().getVvrsTasksResource();

        final String vvrUuid = vvrInstance.getUuid();
        final String taskId;
        try {
            taskId = getParentResource().newVvrManagerProxy(ownerUuid).deleteNoWait(vvrUuid);
        }
        catch (final IllegalArgumentException e) {
            throw ClientErrorFactory.newBadRequestException(e.getMessage(), "Illegal argument to delete vvr", e);
        }
        catch (final Exception e) {
            throw ServerErrorFactory.newInternalErrorException("Failed to delete vvr", "Exception delete vvr", e);
        }
        final URI taskUri = taskResource.constructTaskUri(taskId);

        return Response.status(Status.ACCEPTED).location(taskUri).build();
    }

    @Override
    public final RootSnapshotResource getRootSnapshotResource(final String ownerId) throws CustomResourceException {

        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID vvrUuid = UUID.fromString(vvrInstance.getUuid());

        final MBeanServerConnection jmxConnection = getParentResource().getConnection();

        final ObjectName rootSnapName;
        try {
            final Set<ObjectName> foundNames = jmxConnection.queryNames(
                    VvrObjectNameFactory.newSnapshotQueryListObjectName(ownerUuid, vvrUuid),
                    Query.eq(Query.attr("Uuid"), Query.attr("Parent")));
            if (foundNames.isEmpty()) {
                return null;
            }
            rootSnapName = foundNames.iterator().next();
        }
        catch (final IOException e) {
            LOGGER.error("Failed to query server for root snapshot", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying root snapshot", e);
        }

        final URI rootSnapshotResUri = UriBuilder.fromUri(resourceUri)
                .path(ResourcePath.extractPathForNewSubResource(uriInfo)).build();
        return new RootSnapshotResourceJmxImpl(JMX.newMBeanProxy(jmxConnection, rootSnapName, SnapshotMXBean.class),
                this, rootSnapshotResUri);
    }

    @Override
    public final SnapshotsResource getSnapshotsResource() {
        final URI snapshotsResUri = UriBuilder.fromUri(getResourceUri())
                .path(ResourcePath.extractPathForNewSubResource(uriInfo)).build();
        final SnapshotsResourceJmxImpl result = new SnapshotsResourceJmxImpl(getParentResource(), snapshotsResUri);
        ResourcePath.injectUriInfoContext(uriInfo, result);
        return result;
    }

    @Override
    public final DevicesResource getDevicesResource() {
        final URI devicesResUri = UriBuilder.fromUri(getResourceUri())
                .path(ResourcePath.extractPathForNewSubResource(uriInfo)).build();
        final DevicesResourceJmxImpl result = new DevicesResourceJmxImpl(getParentResource(), devicesResUri);
        ResourcePath.injectUriInfoContext(uriInfo, result);
        return result;
    }

    @Override
    public final VvrStartResource getVvrStartResource(final String ownerId) {
        return new VvrStartResourceJmxImpl();
    }

    @Override
    public final VvrStopResource getVvrStopResource(final String ownerId) {
        return new VvrStopResourceJmxImpl();
    }

    @Override
    public final VvrTasksResourceJmxImpl getVvrTasksResource() {

        final URI taskResUri = UriBuilder.fromUri(resourceUri).path(tasksResourcePath).build();
        final VvrTasksResourceJmxImpl vvrTasksRes = new VvrTasksResourceJmxImpl(taskResUri, getParentResource(),
                resourceUri, vvrInstance);

        return vvrTasksRes;
    }

    @Override
    protected final URI getResourceUri() {
        return resourceUri;
    }

    /**
     * @return the parentResource
     */
    final VvrsResourceJmxImpl getParentResource() {
        if (parentResource == null) {
            parentResource = ResourcePath.extractAncestorFromMatchedResources(uriInfo, VvrsResourceJmxImpl.class);
        }
        return parentResource;
    }

    final String getVvrUuid() {
        return vvrInstance.getUuid();
    }

}
