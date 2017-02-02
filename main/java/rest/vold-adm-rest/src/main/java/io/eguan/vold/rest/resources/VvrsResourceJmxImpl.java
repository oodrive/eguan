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

import io.eguan.vold.jmx.client.JmxClientConnectionFactory;
import io.eguan.vold.model.VvrMXBean;
import io.eguan.vold.model.VvrManagementException;
import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepository;
import io.eguan.vold.rest.generated.model.VersionedVolumeRepositoryList;
import io.eguan.vold.rest.generated.resources.CreateVvrResource;
import io.eguan.vold.rest.generated.resources.VvrResource;
import io.eguan.vold.rest.generated.resources.VvrsResource;
import io.eguan.vold.rest.generated.resources.VvrsTasksResource;
import io.eguan.vold.rest.util.InputValidation;
import io.eguan.vold.rest.util.ResourcePath;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * {@link VvrsResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class VvrsResourceJmxImpl extends AbstractResource implements VvrsResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(VvrsResourceJmxImpl.class);

    private final String tasksResourcePath;

    private final String serverUrl;

    private MBeanServerConnection connection;

    private volatile Boolean initialized = false;

    /**
     * Constructs a {@link VvrsResource} implementation using a {@link javax.management.MBeanServer} and
     * {@link JMX#newMXBeanProxy(MBeanServerConnection, ObjectName, Class) MXBeanProxies} as backend.
     * 
     * @param serverUrl
     *            the URL of the {@link javax.management.MBeanServer}
     */
    public VvrsResourceJmxImpl(final String serverUrl) {
        this.serverUrl = serverUrl.trim();
        this.tasksResourcePath = ResourcePath
                .extractPathForSubResourceLocator(this.getClass(), VvrsTasksResource.class);
    }

    /**
     * Gets the {@link UriBuilder} configured with this resource's absolute path.
     * 
     * @param uriInfo
     *            the request's {@link UriInfo}
     * @return a functional {@link UriBuilder}
     */
    static final UriBuilder getVvrsUriBuilder(final UriInfo uriInfo) {
        // TODO: make this relative in case we're not at the root
        return uriInfo.getBaseUriBuilder().path(VvrsResource.class);
    }

    /**
     * This {@link VvrsResource}'s {@link CreateVvrResource} implementation.
     * 
     * 
     */
    public final class CreateVvrResourceJmxImpl implements CreateVvrResource {

        @Override
        public Response createVvr(final String ownerId, final String uuid) {

            final VvrsTasksResourceJmxImpl taskResource = getVvrsTasksResource();

            final VvrManagerMXBean vvrMgr = newVvrManagerProxy(InputValidation.getUuidFromString(ownerId));

            // TODO: add name and description as query parameters
            final String taskId;
            try {
                if (Strings.isNullOrEmpty(uuid)) {
                    taskId = vvrMgr.createVvrNoWait(null, null);
                }
                else {
                    InputValidation.getUuidFromString(uuid);
                    taskId = vvrMgr.createVvrNoWait(null, null, uuid);
                }
                final URI taskUri = taskResource.constructTaskUri(taskId);

                return Response.status(Status.ACCEPTED).location(taskUri).build();
            }
            catch (final VvrManagementException e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to create vvr", "Exception create vvr", e);
            }

        }

    }

    /**
     * Gets the currently active JMX connection.
     * 
     * @return an open {@link MBeanServerConnection}
     * @throws CustomResourceException
     *             if opening the connection fails
     */
    final MBeanServerConnection getConnection() throws CustomResourceException {
        if (!initialized) {
            init();
        }
        return connection;
    }

    /**
     * Creates a new proxy for the {@link VvrManagerMXBean} matching the provided owner UUID.
     * 
     * @param ownerUuid
     *            the owner's {@link UUID}
     * @return the vvrManagerProxy
     */
    final VvrManagerMXBean newVvrManagerProxy(final UUID ownerUuid) {

        return JMX.newMXBeanProxy(getConnection(), VvrObjectNameFactory.newVvrManagerObjectName(ownerUuid),
                VvrManagerMXBean.class);
    }

    /**
     * Gets the set of all contained VVR instances.
     * 
     * @param ownerId
     *            the associated owner ID
     * @return a (possibly empty) set of {@link ObjectName}s representing the target VVRs
     * @throws CustomResourceException
     *             if querying the remote JMX server fails
     */
    final Set<ObjectName> getAllVvrInstances(final String ownerId) throws CustomResourceException {

        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        // TODO: include filters in query
        Set<ObjectName> vvrInstances;
        try {
            vvrInstances = getConnection().queryNames(VvrObjectNameFactory.newVvrQueryListObjectName(ownerUuid),
                    Query.eq(Query.attr("OwnerUuid"), Query.value(ownerId)));
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying VVRs", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying VVR instances", e);
        }
        return vvrInstances;

    }

    @Override
    public final VersionedVolumeRepositoryList getAllVvr(final String ownerId, final String ownerIdFilter,
            final String machineIdFilter, final String dcIdFilter) throws CustomResourceException {

        final MBeanServerConnection jmxConnection = getConnection();
        final Set<ObjectName> vvrInstances = getAllVvrInstances(ownerId);
        final VersionedVolumeRepositoryList result = getObjectFactory().createVersionedVolumeRepositoryList();
        final List<VersionedVolumeRepository> vvrList = result.getVersionedVolumeRepositories();

        for (final ObjectName currObjName : vvrInstances) {
            vvrList.add(getVvrPojoFromMbeanProxy(JMX.newMXBeanProxy(jmxConnection, currObjName, VvrMXBean.class)));
        }
        return result;
    }

    @Override
    public final VvrResource getVvrResource(final String ownerId, final String vvrId) throws CustomResourceException {

        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID vvrUuid = InputValidation.getUuidFromString(vvrId);

        final MBeanServerConnection jmxConn = getConnection();

        final ObjectName wantedVvrObjName = VvrObjectNameFactory.newVvrObjectName(ownerUuid, vvrUuid);

        try {
            final Set<ObjectName> foundBeans = jmxConn.queryNames(wantedVvrObjName, null);
            if (foundBeans.isEmpty()) {
                return null;
            }
        }
        catch (final IOException e) {
            LOGGER.error("Failed to query server for VVR", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying VVR instance", e);
        }

        VvrResourceJmxImpl result;
        final URI vvrResUri = uriInfo.getBaseUriBuilder().path(uriInfo.getMatchedURIs().get(0)).build();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting VVR resource for URI " + vvrResUri.toString());
        }

        result = new VvrResourceJmxImpl(JMX.newMXBeanProxy(jmxConn, wantedVvrObjName, VvrMXBean.class), vvrResUri);
        ResourcePath.injectUriInfoContext(uriInfo, result);

        return result;

    }

    /**
     * Gets the POJO representation of the given JMX proxy.
     * 
     * @param vvrProxy
     *            a non-<code>null</code> {@link VvrMXBean} proxy
     * @return a newly created {@link VersionedVolumeRepository} representing the input argument
     */
    static final VersionedVolumeRepository getVvrPojoFromMbeanProxy(final VvrMXBean vvrProxy) {
        final VersionedVolumeRepository result = getObjectFactory().createVersionedVolumeRepository();

        // FIXME: quota, instanceCount are not present with VvrMXBean, so neither are persisted
        result.setDescription(vvrProxy.getDescription());
        result.setInitialized(vvrProxy.isInitialized());
        result.setInstanceCount(1);
        result.setName(vvrProxy.getName());
        result.setOwnerid(vvrProxy.getOwnerUuid());
        result.setQuota(Long.MAX_VALUE);
        result.setStarted(vvrProxy.isStarted());
        result.setUuid(vvrProxy.getUuid());

        return result;
    }

    private final void init() throws CustomResourceException {
        synchronized (initialized) {
            try {
                connection = JmxClientConnectionFactory.newConnection(serverUrl);
            }
            catch (NullPointerException | SecurityException | IOException e) {
                LOGGER.error("Exception getting connection", e);
                throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                        "Exception getting connection", e);
            }
            initialized = true;
        }
    }

    @Override
    public final CreateVvrResource getCreateVvrResource(final String ownerId) {
        return new CreateVvrResourceJmxImpl();
    }

    @Override
    public final VvrsTasksResourceJmxImpl getVvrsTasksResource() {
        final URI tasksUri;
        final URI resourceUri;
        if (uriInfo == null) {
            LOGGER.warn("No injected URI information found, constructing tasks resource with default relative URI");
            tasksUri = UriBuilder.fromPath(tasksResourcePath).build();
            resourceUri = UriBuilder.fromPath("/").build(); // empty
        }
        else {
            tasksUri = getVvrsUriBuilder(uriInfo).path(tasksResourcePath).build();
            resourceUri = getVvrsUriBuilder(uriInfo).build();
        }
        final VvrsTasksResourceJmxImpl vvrsTasksRes = new VvrsTasksResourceJmxImpl(tasksUri, this, resourceUri);
        return vvrsTasksRes;
    }

    @Override
    protected final URI getResourceUri() {
        return getVvrsUriBuilder(uriInfo).build();
    }

}
