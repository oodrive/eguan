package io.eguan.dtx;

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

import io.eguan.dtx.DtxManager.DtxLocalNode;

/**
 * MXBean definitions for the {@link DtxLocalNode}.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public interface DtxLocalNodeMXBean {

    /**
     * Gets Node id.
     * 
     * @return the read-only uuid.
     */
    String getUuid();

    /**
     * Gets node ip address.
     * 
     * @return the read-only ip address.
     */
    String getIpAddress();

    /**
     * Gets node port.
     * 
     * @return the read-only port.
     */
    int getPort();

    /**
     * Gets next atomic long.
     * 
     * @return the read-only next atomic long.
     */
    long getNextAtomicLong();

    /**
     * Gets current atomic long.
     * 
     * @return the read-only current atomic long.
     */
    long getCurrentAtomicLong();

    /**
     * Gets node status
     * 
     * @return the read-only status.
     */
    DtxNodeState getStatus();

    /**
     * Gets peers
     * 
     * @return the read-only peers list.
     */
    DtxPeerAdm[] getPeers();

}
