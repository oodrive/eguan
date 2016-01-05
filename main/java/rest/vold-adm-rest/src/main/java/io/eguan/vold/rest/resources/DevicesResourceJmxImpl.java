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
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.generated.model.Device;
import io.eguan.vold.rest.generated.model.DeviceList;
import io.eguan.vold.rest.generated.resources.DeviceResource;
import io.eguan.vold.rest.generated.resources.DevicesResource;
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
 * {@link DevicesResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DevicesResourceJmxImpl extends AbstractResource implements DevicesResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(DevicesResourceJmxImpl.class);

    private final VvrsResourceJmxImpl rootResource;
    private VvrResourceJmxImpl parentResource;

    private final URI resourceUri;

    DevicesResourceJmxImpl(final VvrsResourceJmxImpl rootResource, final URI resourceUri) {
        this.rootResource = rootResource;
        this.resourceUri = resourceUri;
    }

    static final Device getDevicePojoFromMbeanProxy(final DeviceMXBean deviceProxy) {
        final Device result = getObjectFactory().createDevice();

        // FIXME: dataSize, partial, vvr are not present with DeviceMXBean, so none of them are persisted
        result.setActive(deviceProxy.isActive());
        result.setDataSize(0);
        result.setDescription(deviceProxy.getDescription());
        result.setName(deviceProxy.getName());
        result.setParent(deviceProxy.getParent());
        result.setPartial(true);
        result.setReadOnly(deviceProxy.isReadOnly());
        result.setSize(deviceProxy.getSize());
        result.setUuid(deviceProxy.getUuid());
        result.setVvr(null);

        return result;
    }

    @Override
    public final DeviceList getAllDevices(final String ownerId) throws CustomResourceException {
        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID vvrUuid = UUID.fromString(getParentResource().getVvr(ownerId).getUuid());

        final MBeanServerConnection jmxConnection = rootResource.getConnection();

        final DeviceList result = getObjectFactory().createDeviceList();
        final List<Device> deviceList = result.getDevices();

        Set<ObjectName> vvrInstances;
        try {
            vvrInstances = jmxConnection.queryNames(
                    VvrObjectNameFactory.newDeviceQueryListObjectName(ownerUuid, vvrUuid), null);
        }
        catch (final IOException e) {
            LOGGER.error("Exception querying Devices", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Exception querying Devices", e);
        }

        for (final ObjectName currObjName : vvrInstances) {
            deviceList.add(getDevicePojoFromMbeanProxy(JMX.newMXBeanProxy(jmxConnection, currObjName,
                    DeviceMXBean.class)));
        }
        return result;

    }

    @Override
    public final DeviceResource getDeviceResource(final String ownerId, final String deviceId)
            throws CustomResourceException {

        final VvrResourceJmxImpl parentRes = getParentResource();
        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final UUID deviceUuid = InputValidation.getUuidFromString(deviceId);
        final UUID vvrUuid = UUID.fromString(parentRes.getVvrUuid());

        final MBeanServerConnection jmxConnection = parentRes.getParentResource().getConnection();

        final ObjectName wantedObjName = VvrObjectNameFactory.newDeviceObjectName(ownerUuid, vvrUuid, deviceUuid);

        try {
            final Set<ObjectName> foundObjNames = jmxConnection.queryNames(wantedObjName, null);
            if (foundObjNames.isEmpty()) {
                return null;
            }
        }
        catch (final IOException e) {
            LOGGER.error("Failed to query server for device", e);
            throw ServerErrorFactory.newInternalErrorException("Internal communication error",
                    "Failed to query server for device", e);
        }

        final URI resUri = UriBuilder.fromUri(resourceUri).path(deviceId).build();

        final DeviceResourceJmxImpl result = new DeviceResourceJmxImpl(JMX.newMBeanProxy(jmxConnection, wantedObjName,
                DeviceMXBean.class), parentRes, resUri);

        ResourcePath.injectUriInfoContext(uriInfo, result);

        return result;
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
