package com.oodrive.nuage.nbdsrv.client;

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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nbdsrv.packet.DataPushingCmd;
import com.oodrive.nuage.nbdsrv.packet.DataPushingPacket;
import com.oodrive.nuage.nbdsrv.packet.DataPushingReplyPacket;
import com.oodrive.nuage.nbdsrv.packet.NbdException;

/**
 * Represents the task to send a read request.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
class ReadTask extends TaskAbstract {

    /** NBD client */
    private final NbdClient client;
    /** Offset to read the bytes */
    private final long offset;
    /** the Buffer to store the result */
    private final ByteBuffer dst;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadTask.class);

    ReadTask(final NbdClient client, final ByteBuffer dst, final long offset) {
        super();
        this.client = client;
        this.offset = offset;
        this.dst = dst;
    }

    @Override
    public final Boolean call() throws IOException, NbdException {

        // Send read request
        final DataPushingPacket dataPushingPacket = new DataPushingPacket(DataPushingPacket.MAGIC,
                DataPushingCmd.NBD_CMD_READ, getHandle(), offset, dst.limit() - dst.position());

        final ByteBuffer header = DataPushingPacket.serialize(dataPushingPacket);
        try {
            client.writeSocket(header);
        }
        finally {
            DataPushingPacket.release(header);
        }

        // Wait Answer
        LOGGER.debug("Read Data Pushing Reply");

        final ByteBuffer reply = DataPushingReplyPacket.allocateHeader();

        try {
            client.readSocket(reply);
            // TODO : check the handle
            DataPushingReplyPacket.deserialize(reply);
        }
        finally {
            DataPushingReplyPacket.release(reply);
        }
        client.readSocket(dst);
        return true;
    }
}
