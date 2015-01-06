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
public enum OptionReplyCmd {
    /*
     * Will be sent by the server when it accepts the option, or when sending data related to the option (in the case of
     * NBD_OPT_LIST) has finished. No data.
     */
    NBD_REP_ACK(0x01),

    /* A description of an export */
    NBD_REP_SERVER(0x02),
    /*
     * The option sent by the client is unknown by this server implementation (e.g., because the server is too old, or
     * from another source).
     */
    NBD_REP_ERR_UNSUP(2 ^ 31 + 1),
    /*
     * The option sent by the client is known by this server and syntactically valid, but server-side policy forbids the
     * server to allow the option (e.g., the client sent NBD_OPT_LIST but server configuration has that disabled)
     */
    NBD_REP_ERR_POLICY(2 ^ 31 + 2),
    /*
     * The option sent by the client is know by this server, but was determined by the server to be syntactically
     * invalid. For instance, the client sent an NBD_OPT_LIST with nonzero data length.
     */
    NBD_REP_ERR_INVALID(2 ^ 31 + 3),
    /*
     * The option sent by the client is not supported on the platform on which the server is running. Not currently
     * used.
     */
    NBD_REP_ERR_PLATFORM(2 ^ 31 + 4);

    private final long value;

    private static LongObjectOpenHashMap<OptionReplyCmd> mapping;

    static {
        OptionReplyCmd.mapping = new LongObjectOpenHashMap<OptionReplyCmd>(values().length);
        for (final OptionReplyCmd s : values()) {
            OptionReplyCmd.mapping.put(s.value, s);
        }
    }

    private OptionReplyCmd(final long newValue) {
        value = newValue;
    }

    public final long value() {

        return value;
    }

    public static final OptionReplyCmd valueOf(final long value) {

        return OptionReplyCmd.mapping.get(value);
    }
}
