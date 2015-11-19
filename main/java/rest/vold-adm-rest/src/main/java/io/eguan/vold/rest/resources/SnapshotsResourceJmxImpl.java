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

import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.generated.model.Snapshot;
import io.eguan.vold.rest.generated.model.SnapshotList;
import io.eguan.vold.rest.generated.resources.SnapshotResource;
import io.eguan.vold.rest.generated.resources.SnapshotsResource;
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
import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SnapshotsResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class SnapshotsResourceJmxImpl extends AbstractResource implements SnapshotsResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotsResourceJmxImpl.class);

    private final VvrsResourceJmxImpl rootResource;
    private VvrResourceJmxImpl parentResource;
    private final URI resourceUri;

    SnapshotsResourceJmxImpl(final VvrsResourceJmxImpl rootResource, final URI resourceUri) {
        this.rootResource = rootResource;
        this.resourceUri = resourceUri;
    }

    static final Snapshot getSnapshotPojoFromMbeanProxy(final SnapshotMXBean snapshotProxy) {
        final Snapshot result = getObjectFactory().createSnapshot();

        // FIXME: dataSize, partial, vvr are not present with SnapshotMXBean, so none of them are persisted
        result.setDataSize(0);
        result.setDescription(snapshotProxy.getDescription());
        result.setName(snapshotProxy.getName());
        result.setParent(snapshotProxy.getParent());
        result.setPartial(true);
        result.setSize(snapshotProxy.getSize());
        result.setUuid(snapshotProxy.getUuid());
        result.setVvr(null);

        return result;
    }

    @Override
    public final SnapshotList getAllSnapshots(final String ownerId) throws CustomResourceException {
        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID vvrUuid = UUID.fromString(getParentResource().getVvr(ownerId).getUuid());

        final MBeanServerConnection jmxConnection = rootResource.getConnection();

        final SnapshotList result = getObjectFactory().createSnapshotList();
        final List<Snapshot> snapshotList = result.getSnapshots();

        Set<ObjectName> vvrInstances;
        try {
            vvrInstances = jmxConnection.queryNames(
                    VvrObjectNameFactory.newSnapshotQueryListObjectName(ownerUuid, vvrUuid), null);
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying Snapshots", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying snapshots", e);
        }

        for (final ObjectName currObjName : vvrInstances) {
            snapshotList.add(getSnapshotPojoFromMbeanProxy(JMX.newMXBeanProxy(jmxConnection, currObjName,
                    SnapshotMXBean.class)));
        }
        return result;
    }

    @Override
    public final SnapshotResource getSnapshotResource(final String ownerId, final String snapshotId)
            throws CustomResourceException {

        final VvrResourceJmxImpl parentRes = getParentResource();
        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID snapshotUuid = InputValidation.getUuidFromString(snapshotId);
        final UUID vvrUuid = UUID.fromString(parentRes.getVvrUuid());

        final MBeanServerConnection jmxConnection = parentRes.getParentResource().getConnection();

        final ObjectName wantedObjName = VvrObjectNameFactory.newSnapshotObjectName(ownerUuid, vvrUuid, snapshotUuid);

        try {
            final Set<ObjectName> foundObjNames = jmxConnection.queryNames(wantedObjName, null);
            if (foundObjNames.isEmpty()) {
                return null;
            }
        }
        catch (final IOException e) {
            LOGGER.error("Failed to query server for snapshot", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying snapshot", e);
        }

        final URI resUri = UriBuilder.fromUri(resourceUri).path(snapshotId).build();

        return new SnapshotResourceJmxImpl(JMX.newMBeanProxy(jmxConnection, wantedObjName, SnapshotMXBean.class),
                parentRes, resUri);
    }

    @Override
    protected final URI getResourceUri() {
        return resourceUri;
    }

    /**
     * @return the parentResource
     */
    final VvrResourceJmxImpl getParentResource() {
        if (parentResource == null) {
            parentResource = ResourcePath.extractAncestorFromMatchedResources(uriInfo, VvrResourceJmxImpl.class);
        }
        return parentResource;
    }

}
