package com.oodrive.nuage.nbdsrv.client;

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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nbdsrv.packet.DataPushingCmd;
import com.oodrive.nuage.nbdsrv.packet.DataPushingPacket;
import com.oodrive.nuage.nbdsrv.packet.DataPushingReplyPacket;
import com.oodrive.nuage.nbdsrv.packet.NbdException;

public class TrimTask extends TaskAbstract {

    /** NBD client */
    private final NbdClient client;

    /** Position of the first byte to trim */
    private final long offset;

    /** Number of bytes to trim */
    private final int length;

    private static final Logger LOGGER = LoggerFactory.getLogger(TrimTask.class);

    TrimTask(final NbdClient connection, final long offset, final int length) {
        super();
        this.client = connection;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public final Boolean call() throws IOException, NbdException {

        // Send write request
        final DataPushingPacket dataPushingPacket = new DataPushingPacket(DataPushingPacket.MAGIC,
                DataPushingCmd.NBD_CMD_TRIM, getHandle(), offset, length);

        final ByteBuffer header = DataPushingPacket.serialize(dataPushingPacket);
        try {
            client.writeSocket(header);
        }
        finally {
            DataPushingPacket.release(header);
        }

        // Wait Answer
        LOGGER.debug("Read Data Pushing Reply");

        final ByteBuffer dst = DataPushingReplyPacket.allocateHeader();
        try {
            client.readSocket(dst);
            // TODO : check the handle
            DataPushingReplyPacket.deserialize(dst);
        }
        finally {
            DataPushingReplyPacket.release(dst);
        }
        return true;
    }

}
