package com.oodrive.nuage.nbdsrv.packet;

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

import com.carrotsearch.hppc.LongObjectOpenHashMap;

public enum DataPushingCmd {

    /** Read command */
    NBD_CMD_READ(0x00),
    /** Write command */
    NBD_CMD_WRITE(0x01),
    /** Disconnection */
    NBD_CMD_DISC(0x02),
    /** Flush */
    NBD_CMD_FLUSH(0x03),
    /** Trim */
    NBD_CMD_TRIM(0x04);

    private final long value;

    private static LongObjectOpenHashMap<DataPushingCmd> mapping;

    static {
        DataPushingCmd.mapping = new LongObjectOpenHashMap<DataPushingCmd>(values().length);
        for (final DataPushingCmd s : values()) {
            DataPushingCmd.mapping.put(s.value, s);
        }
    }

    private DataPushingCmd(final long newValue) {

        value = newValue;
    }

    /**
     * Return the long value of the Data Pushing command.
     * 
     * @return the value
     * 
     */
    public final long value() {
        return value;
    }

    /**
     * Return the Data Pushing command corresponding to a long.
     * 
     * @param value
     *            the value to translate in a {@link DataPushingCmd}
     * 
     */
    public static final DataPushingCmd valueOf(final long value) {
        return DataPushingCmd.mapping.get(value);
    }
}
