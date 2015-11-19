package io.eguan.vold;

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

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Location of an instance of a VOLD. A location is represented by the UUID of the node hosting the VOLD and the IP
 * address and port to connect to that instance.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author jmcaba
 */
@Immutable
public final class VoldLocation {

    /** UUID of the node */
    private final UUID node;
    /** Address to connect to the peer */
    // TODO: add support to other transport?
    private final InetSocketAddress sockAddr;

    VoldLocation(@Nonnull final UUID node, @Nonnull final InetSocketAddress sockAddr) {
        super();
        this.node = Objects.requireNonNull(node, "node");
        this.sockAddr = Objects.requireNonNull(sockAddr, "sockAddr");
        assert sockAddr.isUnresolved() == false;
    }

    /**
     * UUID of the node hosting the VOLD.
     * 
     * @return UUID of the node
     */
    public final UUID getNode() {
        return node;
    }

    /**
     * IP address and port of the VOLD.
     * 
     * @return address and port to connect to the VOLD.
     */
    public final InetSocketAddress getSockAddr() {
        return sockAddr;
    }

    /**
     * Parse a {@link VoldLocation} from a string. Format:
     * <code>&#x3C;UUID of the node&#x3E;&#x40;&#x3C;IP address&#x3E;:&#x3C;port&#x3E;</code>.
     * 
     * @param value
     *            string to parse
     * @return new {@link VoldLocation}
     * @throws IllegalArgumentException
     *             if the string if not valid
     */
    static final VoldLocation fromString(final String value) throws IllegalArgumentException {
        final int uuidEndIndex = value.indexOf('@');
        if (uuidEndIndex < 0) {
            throw new IllegalArgumentException(value);
        }
        final String uuidStr = value.substring(0, uuidEndIndex);
        final UUID uuid = UUID.fromString(uuidStr);
        final String sockAddrStr = value.substring(uuidEndIndex + 1);

        final int addrEndIndex = sockAddrStr.indexOf(':');
        if (addrEndIndex < 0) {
            throw new IllegalArgumentException(value);
        }
        final String host = sockAddrStr.substring(0, addrEndIndex);
        if (host.isEmpty()) {
            throw new IllegalArgumentException(value);
        }
        int port;
        try {
            port = Integer.valueOf(sockAddrStr.substring(addrEndIndex + 1)).intValue();
        }
        catch (final NumberFormatException e) {
            throw new IllegalArgumentException(value, e);
        }
        final InetSocketAddress addr = new InetSocketAddress(host, port);
        return new VoldLocation(uuid, addr);
    }

    @Override
    public final String toString() {
        return node.toString() + "@" + sockAddr.getAddress().getHostAddress() + ":" + sockAddr.getPort();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VoldLocation)) {
            return false;
        }
        final VoldLocation voldLocation = (VoldLocation) obj;
        return sockAddr.equals(voldLocation.getSockAddr()) && node.equals(voldLocation.getNode());
    }

    @Override
    public final int hashCode() {
        int result = 29;
        result = 31 * result + node.hashCode();
        result = 31 * result + sockAddr.hashCode();
        return result;
    }
}
