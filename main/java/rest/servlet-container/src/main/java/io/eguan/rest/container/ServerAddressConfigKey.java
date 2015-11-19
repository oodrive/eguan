package io.eguan.rest.container;

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

import io.eguan.configuration.InetAddressConfigKey;
import io.eguan.configuration.ValidationError;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Key defining the IP address to which the server binds.
 * 
 * <table border='1'>
 * <tr>
 * <th>NAME</th>
 * <th>DESCRIPTION</th>
 * <th>REQUIRED</th>
 * <th>UNIT</th>
 * <th>TYPE</th>
 * <th>DEFAULT</th>
 * <th>MIN</th>
 * <th>MAX</th>
 * </tr>
 * <tr>
 * <td>{@value #NAME}</td>
 * <td>The IP address to which the server binds</td>
 * <td>FALSE</td>
 * <td></td>
 * <td>InetAddress</td>
 * <td>0.0.0.0</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class ServerAddressConfigKey extends InetAddressConfigKey {
    static {
        try {
            DEFAULT_VALUE = InetAddress.getByAddress(new byte[4]);
        }
        catch (final UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    protected static final String NAME = "server.address";

    private static final InetAddress DEFAULT_VALUE;

    private static final ServerAddressConfigKey INSTANCE = new ServerAddressConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #ServerAddressConfigKey()}
     */
    public static final ServerAddressConfigKey getInstance() {
        return INSTANCE;
    }

    private ServerAddressConfigKey() {
        super(NAME);
    }

    @Override
    protected final ValidationError performadditionalChecks(final InetAddress value) {
        return ValidationError.NO_ERROR;
    }

    @Override
    protected final InetAddress getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
