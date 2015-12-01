package io.eguan.configuration;

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

import io.eguan.configuration.ValidationError.ErrorType;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * {@link AbstractConfigKey} implementation dedicated to handling any form of {@link InetAddress} representation.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public abstract class InetAddressConfigKey extends AbstractConfigKey {

    /**
     * Proxy constructor for subclasses.
     * 
     * @param name
     *            see {@link AbstractConfigKey#AbstractConfigKey(String)}
     */
    public InetAddressConfigKey(final String name) {
        super(name);
    }

    @Override
    public final InetAddress getTypedValue(final MetaConfiguration configuration) throws IllegalStateException,
            ClassCastException, NullPointerException {
        Objects.requireNonNull(configuration);
        checkConfigForKey(configuration);
        return (InetAddress) configuration.getValue(this);
    }

    @Override
    protected final Object parseValue(final String value) throws IllegalArgumentException, NullPointerException {
        if (value.isEmpty()) {
            // Returns the default value or null when there is no default value
            final InetAddress defaultValue = (InetAddress) getDefaultValue();
            return defaultValue;
        }

        try {
            return InetAddress.getByName(value);
        }
        catch (final UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
        if (value == null) {
            return "";
        }
        if (value instanceof InetAddress) {
            return ((InetAddress) value).getHostAddress();
        }
        else {
            throw new IllegalArgumentException("Not an InetAddress");
        }
    }

    @Override
    protected final ValidationError checkValue(final Object value) {
        final ValidationError result = checkForNullAndRequired(value);
        if (result != ValidationError.NO_ERROR) {
            return (result.getType() == ValidationError.ErrorType.VALUE_NULL) ? ValidationError.NO_ERROR : result;
        }
        try {
            final InetAddress inetAddrValue = InetAddress.class.cast(value);
            return performadditionalChecks(inetAddrValue);
        }
        catch (final ClassCastException ce) {
            return new ValidationError(ErrorType.VALUE_INVALID, null, this, value, ce.getMessage());
        }

    }

    /**
     * Method to be override by subclasses for additional checks on the {@link InetAddress} value.
     * 
     * @param value
     *            the value of type {@link InetAddress}
     * @return possible {@link ValidationError}s
     */
    protected abstract ValidationError performadditionalChecks(InetAddress value);

}
