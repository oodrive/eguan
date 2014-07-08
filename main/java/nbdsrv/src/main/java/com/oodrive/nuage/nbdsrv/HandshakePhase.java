package com.oodrive.nuage.nbdsrv;

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

import com.oodrive.nuage.nbdsrv.packet.ExportFlagsPacket;
import com.oodrive.nuage.nbdsrv.packet.GlobalFlagsPacket;
import com.oodrive.nuage.nbdsrv.packet.InitPacket;
import com.oodrive.nuage.nbdsrv.packet.NbdException;
import com.oodrive.nuage.nbdsrv.packet.OptionCmd;
import com.oodrive.nuage.nbdsrv.packet.OptionPacket;
import com.oodrive.nuage.nbdsrv.packet.OptionReplyCmd;
import com.oodrive.nuage.nbdsrv.packet.OptionReplyPacket;
import com.oodrive.nuage.nbdsrv.packet.Utils;

/**
 * Represents the Handshake phase. Handle the different states during this phase
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class HandshakePhase extends PhaseAbstract {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandshakePhase.class);

    /** The current state of this phase */
    private HandshakeState state;

    HandshakePhase(final ClientConnection client) {
        super(client);
        // Init state
        this.state = HandshakeState.INITIALIZATION;
    }

    @Override
    final boolean execute() throws NbdException, IOException {
        final ClientConnection connection = getConnection();
        switch (this.state) {
        case INITIALIZATION:
            LOGGER.debug("enter INITIALIZATION phase");
            writeInitPacket(connection);
            this.state = HandshakeState.GLOBAL_FLAGS_RECEPTION;
            return true;

        case GLOBAL_FLAGS_RECEPTION:
            LOGGER.debug("enter GLOBAL_FLAGS_RECEPTION phase");
            final long flags = readGlobalFlagsPacket(connection);
            connection.setRemoteFlags(flags);
            LOGGER.debug("Remote flags: " + flags);
            this.state = HandshakeState.OPTIONS_NEGOCIATION;
            return true;

        case OPTIONS_NEGOCIATION:
            LOGGER.debug("enter OPTIONS_NEGOCIATION phase");
            final OptionPacket option = readOptionPacket(connection);
            switch (option.getOptionCode()) {
            case NBD_OPT_EXPORT_NAME:
                final String name = handleExportName(option, connection);
                connection.setExportName(name);
                connection.setNbdDevice(name);
                LOGGER.debug("Export Name: " + name);
                EndNegotiation(connection);
                connection.setPhase(new DataPushingPhase(connection));
                break;
            case NBD_OPT_ABORT:
                // Session terminated by the client
                LOGGER.info("Session aborted by the client");
                return false;
            case NBD_OPT_LIST:
                handleList(connection);
                break;
            default:
                // Skip unknown options
                break;
            }
            break;

        default:
            LOGGER.warn("Phase not expected " + this.state);
            break;
        }
        return true;
    }

    /**
     * Send the init packet to the client.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the write on the socket failed
     * 
     */
    private final void writeInitPacket(final ClientConnection connection) throws IOException {
        LOGGER.debug("Write Init Packet");

        int flags = 0;
        flags |= InitPacket.NBD_FLAG_FIXED_NEWSTYLE;
        final InitPacket packet = new InitPacket(InitPacket.MAGIC_STR, InitPacket.MAGIC, flags);
        final ByteBuffer src = InitPacket.serialize(packet);
        try {
            connection.write(src);
        }
        finally {
            InitPacket.release(src);
        }
    }

    /**
     * Read the global flags from the client.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the read on the socket failed
     * @throws NbdException
     *             if the packet is not conform to the NBD protocol
     * 
     */
    private final long readGlobalFlagsPacket(final ClientConnection connection) throws NbdException, IOException {
        LOGGER.debug("Read GlobalFlags Packet");
        final ByteBuffer dst = GlobalFlagsPacket.allocateHeader();
        try {
            connection.read(dst);
            return GlobalFlagsPacket.deserialize(dst);
        }
        finally {
            GlobalFlagsPacket.release(dst);
        }
    }

    /**
     * Read Option from the client.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the read on the socket failed
     * @throws NbdException
     *             if the packet is not conform to the NBD protocol
     * 
     */
    private final OptionPacket readOptionPacket(final ClientConnection connection) throws NbdException, IOException {
        LOGGER.debug("Read Option Packet");
        final ByteBuffer dst = OptionPacket.allocateHeader();
        try {
            connection.read(dst);
            return OptionPacket.deserialize(dst);
        }
        finally {
            OptionPacket.release(dst);
        }
    }

    /**
     * Handle the NBD_OPT_LIST command reply.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the read on the socket failed
     * 
     */
    private final void handleList(final ClientConnection connection) throws IOException {
        LOGGER.debug("Handle List");
        final String[] exportsName = connection.getExportList();

        // Send list
        final OptionReplyPacket packetList = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                OptionReplyCmd.NBD_REP_SERVER);
        final ByteBuffer[] list = OptionReplyPacket.serializeMultiple(packetList, exportsName);
        try {
            connection.write(list);
        }
        finally {
            OptionReplyPacket.release(list);
        }
        // Send Ack
        final OptionReplyPacket packetAck = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                OptionReplyCmd.NBD_REP_ACK);
        final ByteBuffer ack = OptionReplyPacket.serialize(packetAck, "");
        try {
            connection.write(ack);
        }
        finally {
            OptionReplyPacket.release(ack);
        }
    }

    /**
     * Handle the export name option.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the read on the socket failed
     * 
     */
    private final String handleExportName(final OptionPacket option, final ClientConnection connection)
            throws IOException {
        LOGGER.debug("Handle ExportName");

        final ByteBuffer dst = OptionPacket.allocateData(Utils.getUnsignedIntPositive(option.getSize()));
        try {
            connection.read(dst);
            return OptionPacket.getData(dst);
        }
        finally {
            OptionPacket.release(dst);
        }
    }

    /**
     * Send server export information and flags to the client.
     * 
     * @param connection
     *            the {@link ClientConnection}
     * @throws IOException
     *             if the read on the socket failed
     * @throws NbdException
     *             if the export name was not set before
     * 
     */
    private final void EndNegotiation(final ClientConnection connection) throws IOException, NbdException {
        LOGGER.debug("End Negotiation");

        // End of negotiation : send export flags and size
        int flags = ExportFlagsPacket.NBD_FLAG_HAS_FLAGS;

        if (connection.isExportReadOnly()) {
            LOGGER.debug("Set export read only");
            flags |= ExportFlagsPacket.NBD_FLAG_READ_ONLY;
        }
        if (connection.isTrimEnabled()) {
            LOGGER.debug("Set trim allowed");
            flags |= ExportFlagsPacket.NBD_FLAG_SEND_TRIM;
        }
        /*
         * Not supported flags |= NBD_FLAG_SEND_FLUSH; flags |= NBD_FLAG_SEND_FUA; flags |= NBD_FLAG_ROTATIONAL;
         */
        final ExportFlagsPacket packet = new ExportFlagsPacket(connection.getExportSize(), flags);
        final ByteBuffer src = ExportFlagsPacket.serialize(packet);
        try {
            connection.write(src);
        }
        finally {
            InitPacket.release(src);
        }
    }
}
