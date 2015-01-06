package com.oodrive.nuage.webui.model;

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

import java.util.Set;
import java.util.UUID;

/**
 * Interface used to represent a VVR.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface VvrModel extends AbstractItemModel {

    /***
     * Get VVR description
     * 
     * @param vvrUuid
     * @return
     */
    public String getVvrDescription();

    /**
     * Set VVR description.
     * 
     * @param vvrUuid
     * @param desc
     */
    public void setVvrDescription(final String desc);

    /**
     * Gets VVR name
     * 
     * @param vvrUuid
     * @return
     */
    public String getVvrName();

    /**
     * Set VVR name
     * 
     * @param vvrUuid
     * @param name
     */
    public void setVvrName(final String name);

    /**
     * Start VVR
     * 
     * @param vvrUuid
     */
    public void startVvr();

    /**
     * Stop a VVR
     * 
     * @param vvrUuid
     */
    public void stopVvr();

    /**
     * Tells if a VVr is started
     * 
     * @param vvrUuid
     * @return
     */
    public boolean isVvrStarted();

    /**
     * Gets the list of the snapshot for a given VVR.
     * 
     * @param vvrUuid
     * @return
     */
    public Set<UUID> getSnapshotsList();

    /**
     * Gets the list of the device for a given VVR.
     * 
     * @param vvrUuid
     * @return
     */
    public Set<UUID> getDevicesList();

    /**
     * Get the UUID of the root snapshot for a given VVR.
     * 
     * @param vvrUuid
     * @return
     */
    public UUID getRootSnapshot();
}
