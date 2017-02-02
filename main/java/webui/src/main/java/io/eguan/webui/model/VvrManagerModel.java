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

import io.eguan.vold.model.VvrManagementException;

import java.util.Set;
import java.util.UUID;

/**
 * Interface used to represent the VVR manager.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface VvrManagerModel extends AbstractItemModel {

    /**
     * Gets all vvrs for a given owner id.
     * 
     * @return the list of vvr uuid
     */
    public Set<UUID> getVvrs();

    /**
     * Create a new VVR.
     * 
     * @param name
     * @param description
     * @return
     * @throws VvrManagementException
     */
    public void createVvr(final String name, final String description) throws Exception;

    /**
     * Delete a VVR.
     * 
     * @param uuid
     * @return
     */
    public void deleteVvr(UUID uuid);

}
