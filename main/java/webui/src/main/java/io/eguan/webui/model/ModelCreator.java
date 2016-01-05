package io.eguan.webui.model;

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

import java.util.UUID;

/**
 * Interface used to create the different model.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface ModelCreator {

    /**
     * Create a new vvr manager representation.
     * 
     * @return a new vvr manager model
     */
    public VvrManagerModel createVvrManagerModel();

    /**
     * Create a new vvr representation.
     * 
     * @param vvrUuid
     *            the vvr unique identifier
     * 
     * @return the new vvr model
     */
    public VvrModel createVvrModel(UUID vvrUuid);

    /**
     * Create a new snapshot representation.
     * 
     * @param model
     *            the vvr model used to create the snapshot
     * 
     * @param snapshotUuid
     *            the snapshot unique identifier
     * 
     * @return the new snapshot model
     */
    public SnapshotModel createSnapshotModel(VvrModel model, UUID snapshotUuid);

    /**
     * Create a new device representation.
     * 
     * @param model
     *            the vvr model used to create device.
     * 
     * @param deviceUuid
     *            the device unique identifier.
     * 
     * @return a new device model
     */
    public DeviceModel createDeviceModel(VvrModel model, UUID deviceUuid);

}
