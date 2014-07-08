package com.oodrive.nuage.nbdsrv.packet;

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

import com.carrotsearch.hppc.LongObjectOpenHashMap;

public enum DataPushingError {

    NBD_NO_ERROR(0x00),
    /** permission denied */
    NBD_EPERM_ERROR(0x01),
    /** io error */
    NBD_IO_ERROR(0x05),
    /** Illegal argument */
    NBD_EINVAL_ERROR(0x16);

    private final long value;

    private static LongObjectOpenHashMap<DataPushingError> mapping;

    static {
        DataPushingError.mapping = new LongObjectOpenHashMap<DataPushingError>(values().length);
        for (final DataPushingError s : values()) {
            DataPushingError.mapping.put(s.value, s);
        }
    }

    private DataPushingError(final long newValue) {

        value = newValue;
    }

    /**
     * Return the long value of the Data Pushing error.
     * 
     * @return the value
     * 
     */
    public final long value() {
        return value;
    }

    /**
     * Return the Data Pushing error corresponding to a long.
     * 
     * @param value
     *            the value to translate in a {@link DataPushingError}
     * 
     */
    public static final DataPushingError valueOf(final long value) {
        return DataPushingError.mapping.get(value);
    }
}
