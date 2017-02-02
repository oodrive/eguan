package io.eguan.webui.model;

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

import java.util.UUID;

/**
 * Interface used to represent the snapshot.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface SnapshotModel extends AbstractItemModel {

    /**
     * Get the snapshot name.
     * 
     * @return
     */
    public String getSnapshotName();

    /**
     * Set the snapshot name.
     * 
     * @param name
     * @return
     */
    public void setSnapshotName(String name);

    /**
     * Get the snapshot description
     * 
     * @return
     */
    public String getSnapshotDescription();

    /**
     * Set the snapshot description.
     * 
     * @param description
     * @return
     */
    public void setSnapshotDescription(String description);

    /**
     * Get the snapshot size
     * 
     * @return
     */
    public long getSnapshotSize();

    /**
     * Get the list of all the children devices.
     * 
     * @return
     */
    public UUID[] getSnapshotChildrenDevices();

    /**
     * Get the list of all the children snapshots.
     * 
     * @return
     */
    public UUID[] getSnapshotChildrenSnapshots();

    /**
     * Get the snapshot parent
     * 
     * @return
     */
    public UUID getSnapshotParent();

    /**
     * Create a device
     * 
     * @param deviceName
     * @param size
     */
    public void createDevice(final String deviceName, final long size);

    /**
     * Delete a snapshot
     * 
     */
    public void deleteSnapshot();
}
