package io.eguan.vold.rest.resources;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.vold.model.DeviceMXBean;
import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.errors.ClientErrorFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.generated.model.Device;
import io.eguan.vold.rest.generated.model.DeviceList;
import io.eguan.vold.rest.generated.model.Snapshot;
import io.eguan.vold.rest.generated.model.SnapshotList;
import io.eguan.vold.rest.generated.resources.BinarySnapshotResource;
import io.eguan.vold.rest.generated.resources.ChildSnapshotsResource;
import io.eguan.vold.rest.generated.resources.DescendantDevicesResource;
import io.eguan.vold.rest.generated.resources.NewDeviceResource;
import io.eguan.vold.rest.generated.resources.SnapshotResource;
import io.eguan.vold.rest.generated.resources.UploadSnapshotResource;
import io.eguan.vold.rest.util.InputValidation;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SnapshotResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public class SnapshotResourceJmxImpl extends AbstractResource implements SnapshotResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotResourceJmxImpl.class);

    private final SnapshotMXBean snapshotInstance;
    private final VvrResourceJmxImpl vvrResource;
    private final URI resourceUri;

    public SnapshotResourceJmxImpl(final SnapshotMXBean snapshotProxy, final VvrResourceJmxImpl vvrResource,
            final URI resourceUri) {
        this.snapshotInstance = snapshotProxy;
        this.vvrResource = vvrResource;
        this.resourceUri = resourceUri;
    }

    public final class NewDeviceResourceJmxImpl implements NewDeviceResource {

        @Override
        public final Response newDevice(final String ownerId, final String name, final String description,
                final String uuid, final Long size) {

            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();

            final String taskId;
            try {
                if (size == null) {
                    if (uuid == null) {
                        if (description == null) {
                            taskId = snapshotInstance.createDevice(name);
                        }
                        else {
                            taskId = snapshotInstance.createDevice(name, description);
                        }
                    }
                    else {
                        if (description == null) {
                            taskId = snapshotInstance.createDeviceUuid(name, uuid);
                        }
                        else {
                            taskId = snapshotInstance.createDeviceUuid(name, description, uuid);
                        }
                    }
                }
                else {
                    if (uuid == null) {
                        if (description == null) {
                            taskId = snapshotInstance.createDevice(name, size);
                        }
                        else {
                            taskId = snapshotInstance.createDevice(name, description, size);
                        }
                    }
                    else {
                        if (description == null) {
                            taskId = snapshotInstance.createDeviceUuid(name, uuid, size);
                        }
                        else {
                            taskId = snapshotInstance.createDeviceUuid(name, description, uuid, size);
                        }
                    }
                }
            }
            catch (final IllegalArgumentException e) {
                throw ClientErrorFactory.newBadRequestException(e.getMessage(), "Illegal argument for name or uuid", e);
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException("Invalid size", "Exception create device");
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();

        }

    }

    public final class UploadSnapshotResourceJmxImpl implements UploadSnapshotResource {

        @Override
        public Response uploadSnapshot(final String ownerId, final long size) throws CustomResourceException {
            // TODO: implement
            LOGGER.warn("Not implemented");
            throw ServerErrorFactory.newNotImplementedException("Not supported by this server",
                    "Snapshot upload not yet implemented");
        }

    }

    public final class ChildSnapshotsResourceJmxImpl implements ChildSnapshotsResource {

        @Override
        public SnapshotList getChildSnapshots(final String ownerId) throws CustomResourceException {

            final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
            final UUID vvrUuid = UUID.fromString(vvrResource.getVvrUuid());

            final MBeanServerConnection jmxConnection = vvrResource.getParentResource().getConnection();

            final SnapshotList result = getObjectFactory().createSnapshotList();
            final List<Snapshot> snapList = result.getSnapshots();

            final Set<ObjectName> foundSnapObjNames;
            try {
                foundSnapObjNames = jmxConnection.queryNames(
                        VvrObjectNameFactory.newSnapshotQueryListObjectName(ownerUuid, vvrUuid),
                        Query.eq(Query.attr("Parent"), Query.value(snapshotInstance.getUuid())));
            }
            catch (final IOException e) {
                LOGGER.error("Failed to query server for snapshots", e);
                throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                        "Exception querying snapshots", e);
            }
            for (final ObjectName currObjName : foundSnapObjNames) {
                snapList.add(SnapshotsResourceJmxImpl.getSnapshotPojoFromMbeanProxy(JMX.newMBeanProxy(jmxConnection,
                        currObjName, SnapshotMXBean.class)));
            }

            return result;
        }
    }

    public final class DescendantDevicesResourceJmxImpl implements DescendantDevicesResource {

        @Override
        public DeviceList getDescendantDevices(final String ownerId, final Boolean recursive)
                throws CustomResourceException {

            final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
            final UUID vvrUuid = UUID.fromString(vvrResource.getVvrUuid());

            final MBeanServerConnection jmxConnection = vvrResource.getParentResource().getConnection();

            final DeviceList result = getObjectFactory().createDeviceList();
            final List<Device> devList = result.getDevices();

            final Set<ObjectName> foundDevObjNames;
            try {
                if (recursive) {
                    LOGGER.warn("Recursive search for device not implemented");
                    throw ServerErrorFactory.newNotImplementedException("Not supported by this server",
                            "Recursive search for device not implemented");
                }
                else {
                    foundDevObjNames = jmxConnection.queryNames(
                            VvrObjectNameFactory.newDeviceQueryListObjectName(ownerUuid, vvrUuid),
                            Query.eq(Query.attr("Parent"), Query.value(snapshotInstance.getUuid())));
                }
            }
            catch (final IOException e) {
                LOGGER.error("Failed to query server for devices", e);
                throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                        "Exception querying Devices", e);
            }

            for (final ObjectName currObjName : foundDevObjNames) {
                devList.add(DevicesResourceJmxImpl.getDevicePojoFromMbeanProxy(JMX.newMBeanProxy(jmxConnection,
                        currObjName, DeviceMXBean.class)));
            }

            return result;
        }

    }

    public final class BinarySnapshotResourceJmxImpl implements BinarySnapshotResource {

        @Override
        public Response getBinarySnapshot(final String ownerId) throws CustomResourceException {
            // TODO: implement
            LOGGER.warn("Not implemented");
            throw ServerErrorFactory.newNotImplementedException("Not supported by this server",
                    "Binary snapshot access not yet implemented");
        }

    }

    @Override
    public final Snapshot getSnapshot(final String ownerId) {
        return SnapshotsResourceJmxImpl.getSnapshotPojoFromMbeanProxy(snapshotInstance);
    }

    @Override
    public final Snapshot postSnapshot(final String ownerId, final Snapshot snapshot) throws CustomResourceException {
        Objects.requireNonNull(snapshot);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("POST on Snapshot " + snapshotInstance.getUuid() + "; argument=" + snapshot.toString());
        }

        boolean readOnlyChanged = false;
        readOnlyChanged |= !snapshotInstance.getUuid().equals(snapshot.getUuid());
        readOnlyChanged |= !snapshotInstance.getParent().equals(snapshot.getParent());
        readOnlyChanged |= (snapshotInstance.getSize() != snapshot.getSize());

        if (readOnlyChanged) {
            LOGGER.warn("Detected read-only attribute change on Snapshot " + snapshotInstance.getUuid());
            throw ClientErrorFactory.newForbiddenException("Write on read-only attributes forbidden",
                    "Tried to modify read-only attributes for snapshot " + snapshotInstance.getUuid());
        }

        snapshotInstance.setName(snapshot.getName());
        snapshotInstance.setDescription(snapshot.getDescription());

        return SnapshotsResourceJmxImpl.getSnapshotPojoFromMbeanProxy(snapshotInstance);
    }

    @Override
    public Response deleteSnapshot(final String ownerId) {

        final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();

        final String taskId;
        try {

            taskId = snapshotInstance.delete();
        }
        catch (final Exception e) {
            throw ServerErrorFactory.newInternalErrorException("Failed to delete Snapshot",
                    "Exception delete snapshot", e);
        }
        final URI taskUri = tasksResource.constructTaskUri(taskId);
        // FIXME delete root snapshoot (with its ID not with /root) does not return 403

        return Response.status(Status.ACCEPTED).location(taskUri).build();

    }

    @Override
    public final NewDeviceResource getNewDeviceResource(final String ownerId) {
        return new NewDeviceResourceJmxImpl();
    }

    @Override
    public final UploadSnapshotResource getUploadSnapshotResource(final String ownerId) {
        return new UploadSnapshotResourceJmxImpl();
    }

    @Override
    public final ChildSnapshotsResource getChildSnapshotsResource(final String ownerId) {
        return new ChildSnapshotsResourceJmxImpl();
    }

    @Override
    public final DescendantDevicesResource getDescendantDevicesResource(final String ownerId) {
        return new DescendantDevicesResourceJmxImpl();
    }

    @Override
    public final BinarySnapshotResource getBinarySnapshotResource(final String ownerId) {
        return new BinarySnapshotResourceJmxImpl();
    }

    @Override
    protected URI getResourceUri() {
        return resourceUri;
    }

}
