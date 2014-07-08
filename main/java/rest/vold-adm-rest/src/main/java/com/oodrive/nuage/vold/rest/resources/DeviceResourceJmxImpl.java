package com.oodrive.nuage.vold.rest.resources;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Objects;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.oodrive.nuage.iscsisrv.IscsiServerMXBean;
import com.oodrive.nuage.nbdsrv.NbdServerMXBean;
import com.oodrive.nuage.srv.AbstractServerMXBean;
import com.oodrive.nuage.vold.model.DeviceMXBean;
import com.oodrive.nuage.vold.rest.errors.ClientErrorFactory;
import com.oodrive.nuage.vold.rest.errors.CustomResourceException;
import com.oodrive.nuage.vold.rest.errors.ServerErrorFactory;
import com.oodrive.nuage.vold.rest.generated.model.ConnectionInfo;
import com.oodrive.nuage.vold.rest.generated.model.Device;
import com.oodrive.nuage.vold.rest.generated.resources.ActivateDeviceResource;
import com.oodrive.nuage.vold.rest.generated.resources.CloneDeviceResource;
import com.oodrive.nuage.vold.rest.generated.resources.ConnectionResource;
import com.oodrive.nuage.vold.rest.generated.resources.DeactivateDeviceResource;
import com.oodrive.nuage.vold.rest.generated.resources.DeviceResource;
import com.oodrive.nuage.vold.rest.generated.resources.NewSnapshotResource;
import com.oodrive.nuage.vold.rest.generated.resources.ResizeDeviceResource;

/**
 * {@link DeviceResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class DeviceResourceJmxImpl extends AbstractResource implements DeviceResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceResourceJmxImpl.class);

    /**
     * Enumeration of all supported client protocols.
     * 
     * 
     */
    private enum ClientProtocol {
        ISCSI, NBD;
    }

    private final DeviceMXBean deviceInstance;
    private final VvrResourceJmxImpl vvrResource;
    private final URI resourceUri;

    public DeviceResourceJmxImpl(final DeviceMXBean deviceProxy, final VvrResourceJmxImpl vvrResource,
            final URI resourceUri) {
        this.deviceInstance = deviceProxy;
        this.vvrResource = vvrResource;
        this.resourceUri = resourceUri;
    }

    public final class ActivateDeviceResourceJmxImpl implements ActivateDeviceResource {

        @Override
        public Response activateDevice(final String ownerId, final boolean readOnly) {

            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();
            String taskId;
            try {
                if (readOnly) {
                    taskId = deviceInstance.activateRO();
                }
                else {
                    taskId = deviceInstance.activateRW();
                }
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException(
                        e.getMessage() == null ? "Failed to activate device" : e.getMessage(),
                        "Illegal state to activate device");
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to activate the device",
                        "Exception activate device", e);
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }
    }

    public final class DeactivateDeviceResourceJmxImpl implements DeactivateDeviceResource {

        @Override
        public Response deactivateDevice(final String ownerId) {
            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();
            final String taskId;
            try {
                taskId = deviceInstance.deActivate();
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException(e.getMessage() == null ? "Failed to de-activate device"
                        : e.getMessage(), "Illegal state to de-activate device");
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to de-activate device",
                        "Exception to activate device", e);
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }

    }

    public final class ResizeDeviceResourceJmxImpl implements ResizeDeviceResource {

        @Override
        public Response resizeDevice(final String ownerId, final long size) throws CustomResourceException {
            // input validation
            if (size <= 0) {
                throw ClientErrorFactory.newForbiddenException("Invalid size", "Size " + size
                        + " requested for device " + deviceInstance.getUuid());
            }

            final long currentSize = deviceInstance.getSize();

            if ((size < currentSize) && deviceInstance.isActive()) {
                throw ClientErrorFactory.newForbiddenException("Device is active",
                        "Resize requested for active device " + deviceInstance.getUuid());
            }

            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();
            final String taskId;
            try {
                taskId = deviceInstance.setSizeNoWait(size);
            }
            catch (final Exception e) {
                throw ServerErrorFactory
                        .newInternalErrorException("Failed to set new size", "Exception to set size", e);
            }

            final URI taskUri = tasksResource.constructTaskUri(taskId);

            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }
    }

    public final class ConnectionResourceJmxImpl implements ConnectionResource {

        @Override
        public ConnectionInfo getConnection(final String ownerId, final String ip, final String clientProtocol) {
            if (Strings.isNullOrEmpty(ip)) {
                throw ClientErrorFactory
                        .newBadRequestException("No client IP provided", "No client IP; ip=" + ip, null);
            }

            try {
                InetAddress.getByName(ip);
            }
            catch (final UnknownHostException e) {
                throw ClientErrorFactory.newBadRequestException("Invalid client IP provided", "Invalid client IP; ip="
                        + ip, e);
            }

            if (!deviceInstance.isActive()) {
                throw ClientErrorFactory.newForbiddenException("Can't connect to inactive device; ip=" + ip
                        + ", protocol=" + clientProtocol, "Can't connect to device");
            }

            if (Strings.isNullOrEmpty(clientProtocol)) {
                throw ClientErrorFactory.newBadRequestException("No client protocol provided",
                        "No client protocol; protocol=" + clientProtocol, null);
            }

            final ClientProtocol validClientProtocol;
            try {
                validClientProtocol = ClientProtocol.valueOf(clientProtocol.toUpperCase());
            }
            catch (final IllegalArgumentException e) {
                throw ClientErrorFactory.newBadRequestException("Unsupported client protocol",
                        "Bad client protocol; protocol=" + clientProtocol, null);
            }

            final ConnectionInfo result = getObjectFactory().createConnectionInfo();

            final MBeanServerConnection connection = vvrResource.getParentResource().getConnection();

            final ObjectName serverObjName;

            try {
                switch (validClientProtocol) {
                case NBD:
                    result.setDriverVolumeType("nbd");

                    serverObjName = new ObjectName(NbdServerMXBean.class.getPackage().getName() + ":type=Server");

                    result.setDevName(deviceInstance.getName());
                    break;
                case ISCSI:
                default:
                    result.setDriverVolumeType("iscsi");

                    serverObjName = new ObjectName(IscsiServerMXBean.class.getPackage().getName() + ":type=Server");

                    result.setIqn(deviceInstance.getIqn());
                    result.setIscsiAlias(deviceInstance.getIscsiAlias());
                }
            }
            catch (final MalformedObjectNameException e) {
                LOGGER.warn("");
                throw ServerErrorFactory.newInternalErrorException("Server not found",
                        "Exception getting connection data", e);
            }

            final AbstractServerMXBean server = JMX
                    .newMBeanProxy(connection, serverObjName, AbstractServerMXBean.class);
            result.setServerAddress(server.getAddress());
            result.setServerPort(server.getPort());
            return result;
        }

    }

    /**
     * Member {@link NewSnapshotResource} implementation.
     * 
     * 
     */
    public final class NewSnapshotResourceJmxImpl implements NewSnapshotResource {

        @Override
        public final Response newSnapshot(final String ownerId, final String name, final String description,
                final String uuid) {

            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();
            final String taskId;
            try {
                if (uuid == null) {
                    if (description == null) {
                        taskId = deviceInstance.takeSnapshot(name);
                    }
                    else {
                        taskId = deviceInstance.takeSnapshot(name, description);
                    }
                }
                else {
                    if (description == null) {
                        taskId = deviceInstance.takeSnapshotUuid(name, uuid);
                    }
                    else {
                        taskId = deviceInstance.takeSnapshotUuid(name, description, uuid);
                    }
                }
            }
            catch (final IllegalArgumentException e) {
                throw ClientErrorFactory.newBadRequestException(
                        e.getMessage() == null ? "Failed to create a new snapshot" : e.getMessage(),
                        "Illegal argument exception for takeSnaphot", e);
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException(
                        e.getMessage() == null ? "Failed to create a new snapshot" : e.getMessage(),
                        "Illegal State exception for takeSnaphot");
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to create a new snapshot",
                        "Exception create snapshot", e);
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);
            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }

    }

    /**
     * Member {@link CloneDeviceResource} implementation.
     * 
     * 
     */
    public final class CloneDeviceResourceJmxImpl implements CloneDeviceResource {

        @Override
        public Response cloneDevice(final String ownerId, final String name, final String description, final String uuid) {
            final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();
            final String taskId;
            try {
                if (uuid == null) {
                    if (description == null) {
                        taskId = deviceInstance.clone(name);
                    }
                    else {
                        taskId = deviceInstance.clone(name, description);
                    }
                }
                else {
                    if (description == null) {
                        taskId = deviceInstance.cloneUuid(name, uuid);
                    }
                    else {
                        taskId = deviceInstance.cloneUuid(name, description, uuid);
                    }
                }
            }
            catch (final IllegalArgumentException e) {
                throw ClientErrorFactory.newBadRequestException(e.getMessage() == null ? "Failed to clone a new device"
                        : e.getMessage(), "Illegal argument exception for clone", e);
            }
            catch (final IllegalStateException e) {
                throw ClientErrorFactory.newForbiddenException(e.getMessage() == null ? "Failed to clone a new device"
                        : e.getMessage(), "Illegal State exception for clone");
            }
            catch (final Exception e) {
                throw ServerErrorFactory.newInternalErrorException("Failed to clone a new device",
                        "Exception clone device", e);
            }
            final URI taskUri = tasksResource.constructTaskUri(taskId);
            return Response.status(Status.ACCEPTED).location(taskUri).build();
        }

    }

    @Override
    public final Device getDevice(final String ownerId) {
        return DevicesResourceJmxImpl.getDevicePojoFromMbeanProxy(deviceInstance);
    }

    @Override
    public final Device postDevice(final String ownerId, final Device device) throws CustomResourceException {
        Objects.requireNonNull(device);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("POST on Device " + deviceInstance.getUuid() + "; argument=" + device.toString());
        }

        boolean readOnlyChanged = false;
        readOnlyChanged |= !deviceInstance.getUuid().equals(device.getUuid());
        readOnlyChanged |= !deviceInstance.getParent().equals(device.getParent());
        readOnlyChanged |= (deviceInstance.getSize() != device.getSize());

        if (readOnlyChanged) {
            LOGGER.warn("Detected read-only attribute change on Snapshot " + deviceInstance.getUuid());
            throw ClientErrorFactory.newForbiddenException("Write on read-only attributes forbidden",
                    "Tried to modify read-only attributes for device " + deviceInstance.getUuid());
        }

        try {
            deviceInstance.setName(device.getName());
        }
        catch (final IllegalArgumentException e) {
            throw ClientErrorFactory.newBadRequestException(e.getMessage() == null ? "Illegal Name" : e.getMessage(),
                    "Illegal argument for name", e);
        }
        catch (final Exception e) {
            throw ServerErrorFactory.newInternalErrorException("Failed to set name", "Exception set name", e);
        }
        try {
            deviceInstance.setDescription(device.getDescription());
        }
        catch (final Exception e) {
            throw ServerErrorFactory.newInternalErrorException("Failed to set description",
                    "Exception set description", e);
        }

        return DevicesResourceJmxImpl.getDevicePojoFromMbeanProxy(deviceInstance);
    }

    @Override
    public final Response deleteDevice(final String ownerId) {
        final VvrTasksResourceJmxImpl tasksResource = vvrResource.getVvrTasksResource();

        final String taskId;
        try {
            taskId = deviceInstance.delete();
        }
        catch (final IllegalStateException e) {
            throw ClientErrorFactory.newForbiddenException(e.getMessage() == null ? "Illegal state to delete device"
                    : e.getMessage(), "Illegal argument for name");
        }
        catch (final Exception e) {
            throw ServerErrorFactory.newInternalErrorException("Failed to delete device", "Exception delete device", e);
        }
        final URI taskUri = tasksResource.constructTaskUri(taskId);
        return Response.status(Status.ACCEPTED).location(taskUri).build();
    }

    @Override
    public final ActivateDeviceResource getActivateDeviceResource(final String ownerId) {
        return new ActivateDeviceResourceJmxImpl();
    }

    @Override
    public final DeactivateDeviceResource getDeactivateDeviceResource(final String ownerId) {
        return new DeactivateDeviceResourceJmxImpl();
    }

    @Override
    public final ResizeDeviceResource getResizeDeviceResource(final String ownerId) {
        return new ResizeDeviceResourceJmxImpl();
    }

    @Override
    public final NewSnapshotResource getNewSnapshotResource(final String ownerId) {
        return new NewSnapshotResourceJmxImpl();
    }

    @Override
    public final ConnectionResource getConnectionResource(final String ownerId) {
        return new ConnectionResourceJmxImpl();
    }

    @Override
    public CloneDeviceResource getCloneDeviceResource(final String ownerId) {
        return new CloneDeviceResourceJmxImpl();
    }

    @Override
    protected final URI getResourceUri() {
        return resourceUri;
    }

}
