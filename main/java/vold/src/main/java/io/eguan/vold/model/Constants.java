package io.eguan.vold.model;

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

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various constant definition used by both Controller and Main.
 * 
 * @author oodrive
 * @author llambert
 */
public final class Constants {

    /** FQDN base name for all beans. */
    static final String MB_BASENAME = "io.eguan.vold";
    /** Key type for the {@link ObjectName}. */
    private static final String MB_TYPE_HEADER = ":type=";
    /** Value for the {@link ObjectName} type key to represent a Vvr. */
    static final String VVR_TYPE = "Vvr";
    /** Type for the {@link VvrMXBean}. */
    static final String MB_VVR_TYPE = MB_TYPE_HEADER + VVR_TYPE;
    /** Keyword for the {@link VvrMXBean}. */
    static final String MB_VVR_KEY = ",vvr=";
    /** Value for the {@link ObjectName} type key to represent a snapshot. */
    static final String SNAPSHOT_TYPE = "Snapshot";
    /** Type for the {@link SnapshotMXBean}. */
    static final String MB_SNAPSHOT_TYPE = MB_TYPE_HEADER + SNAPSHOT_TYPE;
    /** Keyword for the {@link SnapshotMXBean}. */
    static final String MB_SNAPSHOT_KEY = ",snapshot=";
    /** Value for the {@link ObjectName} type key to represent a device. */
    static final String DEVICE_TYPE = "Device";
    /** Type for the {@link DeviceMXBean}. */
    static final String MB_DEVICE_TYPE = MB_TYPE_HEADER + DEVICE_TYPE;
    /** Keyword for the {@link DeviceMXBean}. */
    static final String MB_DEVICE_KEY = ",device=";

    /** Default iSCSI IQN prefix */
    static final String IQN_PREFIX = "iqn.2014-07.io.eguan:";

    /** VOLD Logger */
    public static final Logger LOGGER = LoggerFactory.getLogger("vold");

    private Constants() {
        throw new Error();
    }
}
