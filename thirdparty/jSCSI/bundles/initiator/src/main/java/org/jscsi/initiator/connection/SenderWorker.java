/**
 * Copyright (c) 2012, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.jscsi.initiator.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.DigestException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.InitiatorMessageParser;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.ProtocolDataUnitFactory;
import org.jscsi.parser.TargetMessageParser;
import org.jscsi.parser.datasegment.OperationalTextKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

/**
 * <h1>SenderWorker</h1>
 * <p/>
 * The worker caller to send all the protocol data units over the socket of this connection.
 * 
 * @author Volker Wildi
 */
public final class SenderWorker {

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /** The logger interface. */
    private static final Logger LOGGER = LoggerFactory.getLogger(SenderWorker.class);

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /** The <code>Connection</code> instance of this worker caller. */
    private final Connection connection;

    /**
     * Non-blocking socket connection to use for the data transfer.
     */
    private final SocketChannel socketChannel;

    /**
     * Factory class for creating the several <code>ProtocolDataUnit</code> instances.
     */
    private final ProtocolDataUnitFactory protocolDataUnitFactory;

    // OODRIVE
    /**
     * Read PDUs.
     */
    private final IntObjectOpenHashMap<ProtocolDataUnit> readPDUs = new IntObjectOpenHashMap<>();
    private final Lock readPDUsLock = new ReentrantLock();

    /**
     * Socket selector.
     */
    private final Selector selector;
    
    
    /**
     * Timeout on reception.
     * 
     */
    private static final long RECEPTION_TIMEOUT = 20 * 1000; 
    
    
    /**
     * Timeout on connection.
     * 
     */
    private static final long CONNECTION_TIMEOUT = 10 * 1000; 

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /**
     * Creates a new, empty <code>SenderWorker</code> instance.
     * 
     * @param initConnection
     *            The reference connection of this worker caller.
     * @param inetAddress
     *            The InetSocketAddress of the Target.
     * @throws IOException
     *             if any IO error occurs.
     */
    public SenderWorker(final Connection initConnection, final InetSocketAddress inetAddress)
        throws IOException {

        connection = initConnection;

        // OODRIVE
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        selector = Selector.open();
        connectSocket(inetAddress);
        // ODRIVE END
       
        socketChannel.socket().setTcpNoDelay(true);

        protocolDataUnitFactory = new ProtocolDataUnitFactory();

    }

    // OODRIVE
    private void connectSocket(InetSocketAddress inetAddress) throws IOException{
        socketChannel.connect(inetAddress);
        socketChannel.register(selector, SelectionKey.OP_CONNECT);

        while (selector.select(CONNECTION_TIMEOUT) > 0) {
            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (key.isValid()) {
                    if (key.isConnectable()) {
                        socketChannel.finishConnect();
                        return ;
                    }
                }
            }

        }
        // Timeout
        close();
        throw new IOException("Connection Timeout");
    }

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /**
     * This method does all the necessary steps, which are needed when a
     * connection should be closed.
     * 
     * @throws IOException
     *             if an I/O error occurs.
     */
    public final void close() throws IOException {

        socketChannel.close();

    }

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /**
     * Receives a <code>ProtocolDataUnit</code> from the socket and appends it
     * to the end of the receiving queue of this connection.
     * 
     * @return Queue with the resulting units
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InternetSCSIException
     *             if any violation of the iSCSI-Standard emerge.
     * @throws DigestException
     *             if a mismatch of the digest exists.
     */
    public ProtocolDataUnit receiveFromWire(final int task) throws DigestException, InternetSCSIException, IOException {

        // OODRIVE
        long end = System.currentTimeMillis() + RECEPTION_TIMEOUT; // timeout
        ProtocolDataUnit protocolDataUnit = null;
        do {
            synchronized (readPDUs) {
                protocolDataUnit = readPDUs.remove(task);
                if (protocolDataUnit != null) {
                    break;
                }
            }

            // Timeout?
            if (System.currentTimeMillis() >= end) {
                // Timeout
                LOGGER.warn("Timeout on response reception");
                throw new InternetSCSIException("Timeout");
            }

            // Attempt read
            boolean readLock = readPDUsLock.tryLock();
            if (readLock) {
                // Can read
                try {
                    try {
                        final ProtocolDataUnit protocolDataUnitTmp = readProtocolDataUnit();
                        if (protocolDataUnitTmp != null) {
                            final int taskTmp = protocolDataUnitTmp.getBasicHeaderSegment().getInitiatorTaskTag();
                            if (taskTmp == task) {
                                // Found our PDU
                                protocolDataUnit = protocolDataUnitTmp;
                            }
                            else {
                                synchronized (readPDUs) {
                                    protocolDataUnit = readPDUs.put(taskTmp, protocolDataUnitTmp);
                                }
                            }
                        }
                    }
                    finally {
                        synchronized (readPDUs) {
                            readPDUs.notifyAll();
                        }
                    }
                }
                finally {
                    readPDUsLock.unlock();
                }
            }
            else {
                // Another thread is reading
                synchronized (readPDUs) {
                    try {
                        readPDUs.wait();
                    }
                    catch (InterruptedException e) {
                        throw new InternetSCSIException(e);
                    }
                }
            }
        } while (protocolDataUnit == null);
        // OODRIVE END
        
        final Exception isCorrect = connection.getState(task).isCorrect(protocolDataUnit);
        if (isCorrect == null) {
            LOGGER.trace("Adding PDU to Receiving Queue.");

            final TargetMessageParser parser =
                (TargetMessageParser)protocolDataUnit.getBasicHeaderSegment().getParser();
            final Session session = connection.getSession();

            // the PDU maxCmdSN is greater than the local maxCmdSN, so we
            // have to update the local one
            if (session.getMaximumCommandSequenceNumber().compareTo(parser.getMaximumCommandSequenceNumber()) < 0) {
                session.setMaximumCommandSequenceNumber(parser.getMaximumCommandSequenceNumber());
            }

            // the PDU expCmdSN is greater than the local expCmdSN, so we
            // have to update the local one
            if (parser.incrementSequenceNumber()) {
                if (connection.getExpectedStatusSequenceNumber().compareTo(parser.getStatusSequenceNumber()) >= 0) {
                    connection.incrementExpectedStatusSequenceNumber();
                } else {
                    LOGGER.error("Status Sequence Number Mismatch (received, expected): "
                        + parser.getStatusSequenceNumber() + ", "
                        + (connection.getExpectedStatusSequenceNumber().getValue() - 1));
                }

            }

        } else {
            throw new InternetSCSIException(isCorrect);
        }
        return protocolDataUnit;
    }
    // OODRIVE
    private final ProtocolDataUnit readProtocolDataUnit() throws DigestException, InternetSCSIException, IOException {
        final ProtocolDataUnit protocolDataUnit = protocolDataUnitFactory.create(
                connection.getSetting(OperationalTextKey.HEADER_DIGEST),
                connection.getSetting(OperationalTextKey.DATA_DIGEST));

        socketChannel.register(selector, SelectionKey.OP_READ);

        while (selector.select(RECEPTION_TIMEOUT) > 0) {
            final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove();

                if (key.isValid()) {
                    if (key.isReadable()) {
                        try {
                            protocolDataUnit.read(socketChannel, Integer.MAX_VALUE);
                        }
                        catch (ClosedChannelException e) {
                            throw new InternetSCSIException(e);
                        }
                        LOGGER.debug("Receiving this PDU: " + protocolDataUnit);
                        return protocolDataUnit;
                    }
                }
            }
        }
        return null;
    }

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

    /**
     * Sends the given <code>ProtocolDataUnit</code> instance over the socket to
     * the connected iSCSI Target.
     * 
     * @param unit
     *            The <code>ProtocolDataUnit</code> instances to send.
     * @throws InternetSCSIException
     *             if any violation of the iSCSI-Standard emerge.
     * @throws IOException
     *             if an I/O error occurs.
     * @throws InterruptedException
     *             if another caller interrupted the current caller before or
     *             while the current caller was waiting for a notification. The
     *             interrupted status of the current caller is cleared when this
     *             exception is thrown.
     */
    public final synchronized void sendOverWire(final ProtocolDataUnit unit) throws InternetSCSIException, IOException,
        InterruptedException {

        final Session session = connection.getSession();

        /* OODRIVE : initiator task tag is set during pdu creation
        unit.getBasicHeaderSegment().setInitiatorTaskTag(session.getInitiatorTaskTag());
        */
        final InitiatorMessageParser parser =
            (InitiatorMessageParser)unit.getBasicHeaderSegment().getParser();
        parser.setCommandSequenceNumber(session.getCommandSequenceNumber());
        parser.setExpectedStatusSequenceNumber(connection.getExpectedStatusSequenceNumber().getValue());

        unit.write(socketChannel);

        LOGGER.debug("Sending this PDU: " + unit);

        // increment the Command Sequence Number
        if (parser.incrementSequenceNumber()) {
            connection.getSession().incrementCommandSequenceNumber();
        }

    }

    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------
    // --------------------------------------------------------------------------

}
