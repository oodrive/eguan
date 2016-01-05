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

import io.eguan.configuration.InetAddressConfigKey;
import io.eguan.configuration.ValidationError;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Key defining the port of the VOLD synchronization server.
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
 * <td>Binding address of the VOLD synchronization server.</td>
 * <td>FALSE</td>
 * <td></td>
 * <td>InetAddress</td>
 * <td>0.0.0.0</td>
 * <td>NA</td>
 * <td>NA</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class ServerEndpointInetAddressConfigKey extends InetAddressConfigKey {

    private static final String DEFAULT_ADDR = "0.0.0.0";
    private static final String NAME = "server.address";

    private static final ServerEndpointInetAddressConfigKey INSTANCE = new ServerEndpointInetAddressConfigKey();

    private static final InetAddress DEFAULT_VALUE;
    static {
        try {
            // Should not fail
            DEFAULT_VALUE = InetAddress.getByName(DEFAULT_ADDR);
        }
        catch (final UnknownHostException e) {
            throw new AssertionError("Failed to parse address " + DEFAULT_ADDR, e);
        }
    }

    public static ServerEndpointInetAddressConfigKey getInstance() {
        return INSTANCE;
    }

    private ServerEndpointInetAddressConfigKey() {
        super(NAME);
    }

    @Override
    protected final InetAddress getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    protected ValidationError performadditionalChecks(final InetAddress value) {
        // No additional check here
        return ValidationError.NO_ERROR;
    }

}
