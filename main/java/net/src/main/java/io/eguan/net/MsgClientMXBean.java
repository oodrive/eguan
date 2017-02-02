package io.eguan.net;

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

public interface MsgClientMXBean {
    /**
     * Gets Node id.
     * 
     * @return the read-only uuid.
     */
    String getUuid();

    /**
     * Gets Timeout.
     * 
     * @return the timeout to specify the maximum allowed duration of a sending in ms.
     */

    long getTimeout();

    /**
     * Set Timeout.
     * 
     * @param timeout
     *            to specify the maximum allowed duration of a sending in ms.
     */
    void setTimeout(final long timeout);

    /**
     * Gets Peers count.
     * 
     * @return the number of peers which are connected.
     */
    int getConnectedPeersCount();

    /**
     * Tells if the client is started.
     * 
     * @return <code>true</code> if started.
     */
    boolean isStarted();

    /**
     * Gets Peers.
     * 
     * @return the list of {@link MsgClientPeerAdm}
     */
    MsgClientPeerAdm[] getPeers();

    /**
     * Restart the client.
     * 
     */
    void restart();

}
