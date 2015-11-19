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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

import javax.annotation.Nonnull;

/**
 * Abstract class for a server configuration.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public abstract class AbstractServerConfig implements Cloneable {

    /** Server bind address */
    private InetAddress address;
    /** Server bind port */
    private int port;

    public AbstractServerConfig(@Nonnull final InetAddress address, @Nonnull final int port) {
        super();
        setAddress(address);
        setPort(port);
    }

    /**
     * Gets IP address.
     * 
     * @return the address
     */
    public final InetAddress getAddress() {
        return address;
    }

    /**
     * Check and set a new ip address.
     * 
     * @param address
     *            the new address to be set
     */
    public final void setAddress(final InetAddress address) {

        try {
            // This call checks if address is null
            if (!address.isAnyLocalAddress() && NetworkInterface.getByInetAddress(address) == null) {
                throw new ServerConfigurationException("Invalid address: " + address);
            }
        }
        catch (final SocketException e) {
            throw new ServerConfigurationException("Invalid address: " + address, e);
        }
        this.address = address;
    }

    /**
     * Get the port.
     * 
     * @return the current port
     */
    public final int getPort() {
        return port;
    }

    /**
     * Check and set the port.
     * 
     * @param port
     *            the new port to be set
     */
    public final void setPort(final int port) {
        if (port < 1 || port > 0xFFFF) {
            throw new ServerConfigurationException("port=" + port);
        }
        this.port = port;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof AbstractServerConfig))
            return false;
        final AbstractServerConfig other = (AbstractServerConfig) obj;

        if (port != other.port) {
            return false;
        }
        return (address == other.address || (address != null && address.equals(other.address)));
    }

    @Override
    public String toString() {
        return "AbstractServerConfig [address=" + address + ", port=" + port + "]";
    }

    @Override
    public AbstractServerConfig clone() {
        try {
            final AbstractServerConfig result = (AbstractServerConfig) super.clone();
            return result;
        }
        catch (final CloneNotSupportedException e) {
            throw new AssertionError();
        }

    }
}
