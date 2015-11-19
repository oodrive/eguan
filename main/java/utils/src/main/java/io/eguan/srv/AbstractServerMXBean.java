package io.eguan.srv;

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

import java.net.UnknownHostException;

/**
 * JMX definitions for a {@link AbstractServer}.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public interface AbstractServerMXBean {

    /**
     * Gets the server bind address.
     * 
     * @return the string representation of the bind address.
     */
    String getAddress();

    /**
     * Sets the server bind address. Will be taken into account during the next server start.
     * 
     * @param address
     *            new server bind address.
     * @throws NullPointerException
     *             if address is <code>null</code>
     * @throws ServerConfigurationException
     *             if address is invalid (not a local address)
     */
    void setAddress(String address) throws UnknownHostException;

    /**
     * Gets the server bind port.
     * 
     * @return the bind port.
     */
    int getPort();

    /**
     * Sets the server port. Will be taken into account during the next server start.
     * 
     * @param port
     *            the new bind port
     * @throws ServerConfigurationException
     *             if port is not a valid bind port
     */
    void setPort(int port) throws ServerConfigurationException;

    /**
     * Starts the server. Takes a copy of the current configuration and starts the server.
     * 
     * @throws IllegalStateException
     *             if the server is already started
     */
    void start() throws IllegalStateException;

    /**
     * Stops the server. Does nothing if the server is not started. The method blocks until the server stops.
     */
    void stop();

    /**
     * Tells if the server is started.
     * 
     * @return <code>true</code> if the server is started.
     */
    boolean isStarted();

    /**
     * Tells if the server must be restarted.
     * 
     * @return <code>true</code> if the server must be restarted
     */
    boolean isRestartRequired();
}
