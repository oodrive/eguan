package io.eguan.nbdsrv.client;

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

import io.eguan.nbdsrv.packet.ExportFlagsPacket;
import io.eguan.nbdsrv.packet.OptionCmd;
import io.eguan.nbdsrv.packet.OptionPacket;
import io.eguan.nbdsrv.packet.OptionReplyCmd;
import io.eguan.nbdsrv.packet.OptionReplyPacket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the task to negotiate option during the handshake.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
class OptionNegotiationTask implements Callable<String[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OptionNegotiationTask.class);

    /** NBD client */
    private final NbdClient nbdClient;

    /** Option to negotiate */
    private final OptionCmd cmd;

    /** Data to send with the option. Can be null **/
    private final String data;

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    OptionNegotiationTask(final NbdClient nbdClient, final OptionCmd cmd, final String data) {
        this.nbdClient = nbdClient;
        this.cmd = cmd;
        this.data = data;
    }

    @Override
    public final String[] call() throws IOException {
        switch (cmd) {
        case NBD_OPT_LIST:
            sendListOption();
            return readListReply();
        case NBD_OPT_EXPORT_NAME:
            sendExportNameOption(data);
            // Next step : server sends its export flags
            nbdClient.setExportName(data);
            handleExportFlags();
            break;
        case NBD_OPT_ABORT:
            sendAbortOption();
            // Close the client
            nbdClient.close();
            break;
        default:
            break;
        }
        return EMPTY_STRING_ARRAY;
    }

    /**
     * Send a request to have the list of the export.
     * 
     * @throws IOException
     *             if I/O errors occurs
     */
    private final void sendListOption() throws IOException {
        LOGGER.debug("Ask export list");
        final OptionPacket optionPacket = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_LIST, 0);
        final ByteBuffer[] src = OptionPacket.serialize(optionPacket, null);
        try {
            nbdClient.writeSocket(src);
        }
        finally {
            OptionPacket.release(src);
        }
    }

    /**
     * Send a request to be logged into an export.
     * 
     * @param name
     *            the name of the export
     * @throws IOException
     *             if I/O errors occur
     */
    private final void sendExportNameOption(final String name) throws IOException {
        LOGGER.debug("Set export name= " + name);
        final OptionPacket optionPacket = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_EXPORT_NAME,
                name.length());
        final ByteBuffer[] src = OptionPacket.serialize(optionPacket, name);
        try {
            nbdClient.writeSocket(src);
        }
        finally {
            OptionPacket.release(src);
        }

    }

    /**
     * Send abort to stop the handshake.
     * 
     * @throws IOException
     *             if I/O errors occur
     */
    private final void sendAbortOption() throws IOException {
        LOGGER.debug("Abort");
        final OptionPacket optionPacket = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_ABORT, 0);
        final ByteBuffer[] src = OptionPacket.serialize(optionPacket, null);
        try {
            nbdClient.writeSocket(src);
        }
        finally {
            OptionPacket.release(src);
        }
    }

    /**
     * Read the answer of the server.
     * 
     * @return a {@link OptionReplyPacket} for the reply
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    private final OptionReplyPacket readOptionReply() throws IOException {
        LOGGER.debug("Read Option Reply");
        final ByteBuffer dst = OptionReplyPacket.allocateHeader();
        try {
            nbdClient.readSocket(dst);
            return OptionReplyPacket.deserialize(dst);
        }
        finally {
            OptionReplyPacket.release(dst);
        }
    }

    /**
     * Read Data from the option reply.
     * 
     * @param reply
     *            the {@link OptionReplyPacket} with contains the header of the reply
     * @return the data as an array of String
     * 
     * @throws IOException
     *             if an I/O error occurs
     */
    private final String[] readDataOptionReply(final OptionReplyPacket reply) throws IOException {
        LOGGER.debug("Read data from Option Reply");
        final ByteBuffer dst = OptionReplyPacket.allocateData((int) (reply.getDataSize()));
        try {
            nbdClient.readSocket(dst);
            return OptionReplyPacket.getData(dst, reply);
        }
        finally {
            OptionPacket.release(dst);
        }
    }

    /**
     * Read and save export flags.
     * 
     * @throws IOException
     *             if I/O error occurs
     */
    private final void handleExportFlags() throws IOException {
        final ExportFlagsPacket exportFlagsPacket = readExportFlagsPacket();
        nbdClient.setExportFlags(exportFlagsPacket.getExportFlags());
        nbdClient.setExportSize(exportFlagsPacket.getExportSize());
    }

    /**
     * Read export flags.
     * 
     * @return the {@link ExportFlagsPacket} of the server
     * 
     * @throws IOException
     *             if I/O error occurs
     */
    private final ExportFlagsPacket readExportFlagsPacket() throws IOException {
        LOGGER.debug("Read Export Flags");
        final ByteBuffer dst = ExportFlagsPacket.allocateHeader();
        try {
            nbdClient.readSocket(dst);
            return ExportFlagsPacket.deserialize(dst);
        }
        finally {
            ExportFlagsPacket.release(dst);
        }
    }

    /**
     * Read the reply for list request.
     * 
     * @return the list as an array of String
     * 
     * @throws IOException
     *             if I/O error occurs
     */
    private final String[] readListReply() throws IOException {
        LOGGER.debug("Read Export List");
        boolean end = false;
        final List<String> exports = new ArrayList<>();
        while (!end) {
            final OptionReplyPacket packet = readOptionReply();
            final OptionCmd cmd = packet.getOptionCmd();
            final OptionReplyCmd replyCmd = packet.getReplyCmd();

            switch (cmd) {
            case NBD_OPT_LIST:
                switch (replyCmd) {
                case NBD_REP_SERVER:
                    if (packet.getDataSize() != 0) {
                        final String[] s = readDataOptionReply(packet);
                        LOGGER.debug("Receive: " + s[0]);
                        exports.add(s[0]);
                    }
                    break;
                case NBD_REP_ACK:
                    LOGGER.debug("End of list reception");
                    end = true;
                    break;
                default:
                    break;
                }
            default:
                break;
            }
        }
        return exports.toArray(EMPTY_STRING_ARRAY);
    }
}
