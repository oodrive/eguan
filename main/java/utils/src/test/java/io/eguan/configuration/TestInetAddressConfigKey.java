package io.eguan.configuration;

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

import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.InetAddressConfigKey;
import io.eguan.configuration.ValidationError;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

/**
 * Implementation of {@link TestAbstractConfigKeys} for testing {@link InetAddressConfigKey}s.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class TestInetAddressConfigKey extends TestAbstractConfigKeys {

    private static final String INVALID_HOST = "256.256.256.256";

    @Override
    protected AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {

        return new InetAddressConfigKey("test.inetaddr.key") {

            @Override
            protected InetAddress getDefaultValue() {
                try {
                    return hasDefault ? InetAddress.getByAddress(new byte[4]) : null;
                }
                catch (final UnknownHostException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public boolean isRequired() {
                return required;
            }

            @Override
            protected ValidationError performadditionalChecks(final InetAddress value) {
                return ValidationError.NO_ERROR;
            }
        };
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseValueFailUnknownHost() {
        final AbstractConfigKey target = getTestKey(false, false);

        target.parseValue(INVALID_HOST);
    }

}
