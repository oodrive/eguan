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

import io.eguan.configuration.MultiValuedConfigKey;
import io.eguan.configuration.ValidationError;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Key holding the list of remote peers of this VOLD instance.
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
 * <td>List of remote peers of this VOLD instance.</td>
 * <td>FALSE (temporary)</td>
 * <td>Comma-separated list of vold location:
 * <code>&#x3C;UUID of the node&#x3E;&#x40;&#x3C;IP address&#x3E;:&#x3C;port&#x3E;</code></td>
 * <td>{@link String}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 */
public final class PeersConfigKey extends MultiValuedConfigKey<ArrayList<VoldLocation>, VoldLocation> {

    private static final String NAME = "peers";

    private static final PeersConfigKey INSTANCE = new PeersConfigKey();

    public static PeersConfigKey getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    private PeersConfigKey() {
        /*
         * unchecked cast warning suppression intentional and limited to narrowing the class of
         * ArrayList<VoldLocation>().getClass() to the exact match for the super constructor. TODO: remove as soon as
         * Java allows ArrayList<VoldLocation>.class
         */
        super(NAME, ",", (Class<ArrayList<VoldLocation>>) new ArrayList<VoldLocation>().getClass(), VoldLocation.class);

    }

    @Override
    protected final ArrayList<VoldLocation> getDefaultValue() {
        return null;
    }

    /*
     * TODO: temporary, during remote mode development.
     * 
     * @see io.eguan.configuration.AbstractConfigKey#isRequired()
     */
    @Override
    public final boolean isRequired() {
        return false;
    }

    @Override
    protected final String valueToString(final Object value) throws IllegalArgumentException, NullPointerException {
        if (value == null) {
            return "";
        }
        if (value instanceof ArrayList) {
            final String stringList = String.valueOf(value);
            return stringList.substring(1, stringList.length() - 1).replace(", ", getSeparator());
        }
        else {
            throw new IllegalArgumentException("Not an ArrayList");
        }
    }

    @Override
    protected final ArrayList<VoldLocation> getCollectionFromValueList(final ArrayList<VoldLocation> values) {
        return values;
    }

    @Override
    protected final VoldLocation getItemValueFromString(final String value) {
        final String cleanValue = Objects.requireNonNull(value).trim();
        if (cleanValue.isEmpty()) {
            throw new IllegalArgumentException("Empty file path");
        }
        return VoldLocation.fromString(cleanValue);
    }

    @Override
    protected final ArrayList<VoldLocation> makeDefensiveCopy(final ArrayList<VoldLocation> value) {
        return value == null ? null : new ArrayList<VoldLocation>(value);
    }

    @Override
    protected final ValidationError performAdditionalValueChecks(final ArrayList<VoldLocation> value) {
        return ValidationError.NO_ERROR;
    }

}
