package com.oodrive.nuage.srv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
import java.net.UnknownHostException;

import com.oodrive.nuage.configuration.InetAddressConfigKey;
import com.oodrive.nuage.configuration.ValidationError;

public abstract class ServerInetAddressConfigKey extends InetAddressConfigKey {

    private static final String DEFAULT_ADDR = "0.0.0.0";
    // public for javadoc
    public static final String NAME = "address";

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

    protected ServerInetAddressConfigKey() {
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
