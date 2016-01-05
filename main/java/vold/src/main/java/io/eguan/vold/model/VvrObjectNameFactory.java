package io.eguan.vold.model;

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

import io.eguan.dtx.DtxLocalNodeMXBean;
import io.eguan.dtx.DtxManagerMXBean;
import io.eguan.vold.VoldMXBean;

import java.util.Objects;
import java.util.UUID;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * {@link ObjectName}s and utility methods for accessing vold objects via JMX.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * @author jmcaba
 * 
 */
public final class VvrObjectNameFactory {

    /**
     * No instance.
     */
    private VvrObjectNameFactory() {
        throw new AssertionError();
    }

    /**
     * Get the {@link ObjectName} of a {@link VoldMXBean}.
     * 
     * @param nodeUuid
     *            node of the VOLD
     * @return the object name of the {@link VoldMXBean}.
     */
    public static final ObjectName newVoldObjectName(final UUID nodeUuid) {
        return newManagerObjectName(nodeUuid, "Vold");
    }

    /**
     * Get the {@link ObjectName} of a {@link VvrManagerMXBean}.
     * 
     * @param ownerUuid
     *            owner of the VVR
     * @return the object name of the {@link VvrManagerMXBean}.
     */
    public static final ObjectName newVvrManagerObjectName(final UUID ownerUuid) {
        return newManagerObjectName(ownerUuid, "VvrManager");
    }

    /**
     * Get the {@link ObjectName} of a {@link DtxManagerMXBean}.
     * 
     * @param ownerUuid
     *            owner of the VVR
     * @return the object name of the {@link DtxManagerMXBean}.
     */
    public static final ObjectName newDtxManagerObjectName(final UUID ownerUuid) {
        return newManagerObjectName(ownerUuid, "DtxManager");
    }

    /**
     * Get the {@link ObjectName} of a {@link DtxLocalNodeMXBean}.
     * 
     * @param ownerUuid
     *            owner of the VVR
     * @return the object name of the {@link DtxLocalNodeMXBean}.
     */
    public static final ObjectName newDtxLocalNodeObjectName(final UUID ownerUuid) {
        return newManagerObjectName(ownerUuid, "DtxLocalNode");
    }

    /**
     * Get the {@link ObjectName} of a {@link VvrManagerMXBean}.
     * 
     * @param ownerUuid
     *            owner of the VVR
     * @return the object name of the {@link VvrManagerMXBean}.
     */
    private static final ObjectName newManagerObjectName(final UUID ownerUuid, final String manager) {
        Objects.requireNonNull(ownerUuid);
        final String managerObjNameStr = Constants.MB_BASENAME + ":type=" + manager;
        try {
            return new ObjectName(managerObjNameStr);
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Get the {@link ObjectName} to query the list of
     * {@link io.eguan.vvr.repository.core.api.VersionedVolumeRepository}.
     * 
     * @param ownerUuid
     * @return the {@link ObjectName} to get the list of VVR of the given owner
     */
    public static final ObjectName newVvrQueryListObjectName(final UUID ownerUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_VVR_TYPE + Constants.MB_VVR_KEY + "*");
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Compute the object name of a VVR.
     * 
     * @param ownerUuid
     * @param vvrUuid
     * @return the {@link ObjectName} of <code>vvr</code>
     */
    public static final ObjectName newVvrObjectName(final UUID ownerUuid, final UUID vvrUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_VVR_TYPE + Constants.MB_VVR_KEY
                    + Objects.requireNonNull(vvrUuid));
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Get the {@link ObjectName} to query the list of the snapshots of a
     * {@link io.eguan.vvr.repository.core.api.VersionedVolumeRepository}.
     * 
     * @param ownerUuid
     * @param vvrUuid
     * @return the {@link ObjectName} to get the list of the snapshots of a VVR of the given owner
     */
    public static final ObjectName newSnapshotQueryListObjectName(final UUID ownerUuid, final UUID vvrUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_SNAPSHOT_TYPE + Constants.MB_VVR_KEY
                    + Objects.requireNonNull(vvrUuid, "vvrUuid") + Constants.MB_SNAPSHOT_KEY + "*");
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Build the {@link ObjectName} of a {@link VvrSnapshot}.
     * 
     * @param ownerUuid
     * @param vvrUuid
     * @param snapshotUuid
     * @return the {@link ObjectName} of <code>snapshot</code>
     */
    public static final ObjectName newSnapshotObjectName(final UUID ownerUuid, final UUID vvrUuid,
            final UUID snapshotUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_SNAPSHOT_TYPE + Constants.MB_VVR_KEY
                    + Objects.requireNonNull(vvrUuid, "vvrUuid") + Constants.MB_SNAPSHOT_KEY
                    + Objects.requireNonNull(snapshotUuid, "snapshotUuid"));
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Get the {@link ObjectName} to query the list of the devices of a
     * {@link io.eguan.vvr.repository.core.api.VersionedVolumeRepository}.
     * 
     * @param ownerUuid
     * @param vvrUuid
     * @return the {@link ObjectName} to get the list of the devices of a VVR of the given owner
     */
    public static final ObjectName newDeviceQueryListObjectName(final UUID ownerUuid, final UUID vvrUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_DEVICE_TYPE + Constants.MB_VVR_KEY
                    + Objects.requireNonNull(vvrUuid, "vvrUuid") + Constants.MB_DEVICE_KEY + "*");
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }

    /**
     * Build the {@link ObjectName} of a {@link VvrDevice}.
     * 
     * @param ownerUuid
     * @param vvrUuid
     * @param deviceUuid
     * @return the {@link ObjectName} of <code>device</code>
     */
    public static final ObjectName newDeviceObjectName(final UUID ownerUuid, final UUID vvrUuid, final UUID deviceUuid) {
        Objects.requireNonNull(ownerUuid);
        try {
            return new ObjectName(Constants.MB_BASENAME + Constants.MB_DEVICE_TYPE + Constants.MB_VVR_KEY
                    + Objects.requireNonNull(vvrUuid, "vvrUuid") + Constants.MB_DEVICE_KEY
                    + Objects.requireNonNull(deviceUuid, "deviceUuid"));
        }
        catch (final MalformedObjectNameException e) {
            // Should not occur
            throw new AssertionError("Failed to create ObjectName", e);
        }
    }
}
