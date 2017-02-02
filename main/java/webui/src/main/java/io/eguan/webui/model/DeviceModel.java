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
 * Interface used to represent a device.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface DeviceModel extends AbstractItemModel {

    /**
     * Gets device name.
     * 
     * @return
     */
    public String getDeviceName();

    /**
     * Set device name
     * 
     * @param name
     */
    public void setDeviceName(final String name);

    /**
     * Get device description
     * 
     * @return
     */
    public String getDeviceDescription();

    /**
     * Set device description.
     * 
     * @param description
     */
    public void setDeviceDescription(final String description);

    /**
     * Get device iqn.
     * 
     * @return
     */
    public String getDeviceIqn();

    /**
     * Set device iqn.
     * 
     * @param iqn
     */
    public void setDeviceIqn(final String iqn);

    /**
     * Get device iscsi alias.
     * 
     * @return
     */
    public String getDeviceIscsiAlias();

    /**
     * Set device iscsi alias.
     * 
     * @param alias
     */
    public void setDeviceIscsiAlias(final String alias);

    /**
     * Get device iscsi block size.
     * 
     * @return
     */
    public int getDeviceIscsiBlockSize();

    /**
     * Set device Iscsi Block size.
     * 
     * @param blockSize
     */
    public void setDeviceIscsiBlockSize(final int blockSize);

    /**
     * Get device size.
     * 
     * @return
     */
    public long getDeviceSize();

    /**
     * Set device size.
     * 
     * @param size
     */
    public void setDeviceSize(final long size);

    /**
     * Get device parent
     * 
     * @return
     */
    public UUID getDeviceParent();

    /**
     * Tell if the device is active
     * 
     * @return
     */
    public boolean isDeviceActive();

    /**
     * Tell if the device is read only.
     * 
     * @return
     */
    public boolean isDeviceReadOnly();

    /**
     * Activate device read only.
     * 
     * @return
     */
    public void activateDeviceRO();

    /**
     * Activate RW a device
     * 
     * @return
     */
    public void activateDeviceRW();

    /**
     * Deactivate a device.
     * 
     * @return
     */
    public void deActivateDevice();

    /**
     * Take a new snapshot.
     * 
     * @param name
     * @return
     */
    public void takeDeviceSnapshot(final String name);

    /**
     * Delete a device.
     * 
     * @return
     */
    public void deleteDevice();

}
