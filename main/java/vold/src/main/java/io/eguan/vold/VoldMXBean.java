package io.eguan.vold;

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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.management.JMException;

/**
 * MXBean definitions for the {@link Vold}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author jmcaba
 */
public interface VoldMXBean {

    /**
     * Gets Path.
     * 
     * @return the read-only path.
     */
    String getPath();

    /**
     * Gets vold configuration
     * 
     * @return the read-only config.
     */
    Map<String, String> getVoldConfiguration();

    /**
     * Start the {@link Vold}.
     * 
     * @throws IOException
     * @throws JMException
     */
    void start() throws JMException, IOException;

    /**
     * Stop the {@link Vold}.
     * 
     * @throws IOException
     */
    void stop() throws IOException;

    /**
     * Add a new peer to the {@link Vold}.
     * 
     * @param uuid
     * @param address
     * @param port
     * @throws JMException
     */
    void addPeer(@Nonnull final String uuid, @Nonnull final String address, @Nonnegative final int port)
            throws JMException;

    /**
     * Add a new peer to the {@link Vold}.
     * 
     * @param uuid
     * @param address
     * @param port
     * @throws JMException
     * @return The {@link UUID} of the task handling the operation as a String
     */
    String addPeerNoWait(@Nonnull final String uuid, @Nonnull final String address, @Nonnegative final int port)
            throws JMException;

    /**
     * Remove a peer from the {@link Vold}.
     * 
     * @param peer
     * @throws JMException
     */
    void removePeer(@Nonnull final String uuid) throws JMException;

    /**
     * Remove a peer from the {@link Vold}.
     * 
     * @param peer
     * @throws JMException
     * @return The {@link UUID} of the task handling the operation as a String
     */
    String removePeerNoWait(@Nonnull final String uuid) throws JMException;
}
