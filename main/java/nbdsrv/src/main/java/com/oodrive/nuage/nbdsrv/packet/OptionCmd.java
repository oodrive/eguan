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

// unsigned 32 bits
public enum OptionCmd {
    NBD_OPT_EXPORT_NAME(0x01), NBD_OPT_ABORT(0x02), NBD_OPT_LIST(0x03);

    private final long value;

    private static LongObjectOpenHashMap<OptionCmd> mapping;

    static {
        OptionCmd.mapping = new LongObjectOpenHashMap<OptionCmd>(values().length);
        for (final OptionCmd s : values()) {
            OptionCmd.mapping.put(s.value, s);
        }
    }

    private OptionCmd(final long newValue) {
        value = newValue;
    }

    public final long value() {
        return value;
    }

    public static final OptionCmd valueOf(final long value) {
        return OptionCmd.mapping.get(value);
    }
}
