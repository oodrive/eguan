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

/**
 * Represents the task to send a write request.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
class WriteTask extends TaskAbstract {

    /** NBD client */
    private final NbdClient client;

    /** Buffer which contains the data to write */
    private final ByteBuffer src;

    /** Position of the first byte to write */
    private final long offset;

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteTask.class);

    WriteTask(final NbdClient connection, final ByteBuffer src, final long offset) {
        super();
        this.client = connection;
        this.src = src;
        this.offset = offset;
    }

    @Override
    public final Boolean call() throws IOException, NbdException {

        // Send write request
        final DataPushingPacket dataPushingPacket = new DataPushingPacket(DataPushingPacket.MAGIC,
                DataPushingCmd.NBD_CMD_WRITE, getHandle(), offset, src.limit() - src.position());

        final ByteBuffer header = DataPushingPacket.serialize(dataPushingPacket);
        try {
            // Send the two buffers with scatter gather
            final ByteBuffer[] buffers = { header, src };
            client.writeSocket(buffers);
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
