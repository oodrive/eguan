package org.jscsi.target.connection.phase;

import java.io.IOException;
import java.security.DigestException;
import java.util.concurrent.Callable;

import javax.naming.OperationNotSupportedException;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.target.connection.Connection;
import org.jscsi.target.settings.SettingsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this class represent a connection's phase (see {@link Connection} for a description of
 * the relationship between
 * stages, phases, connections, and sessions).
 * <p>
 * To start a phase, one of the <i>execute</i> methods must be called, which one is sub-class-specific.
 * 
 * @author Andreas Ergenzinger, University of Konstanz
 */
public abstract class TargetPhase implements Callable<Boolean> {
    
    enum PHASE_EXEC_STATUS { DONE, CLOSE, GO_ON};

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetPhase.class);

    /**
     * The connection this phase is a part of.
     */
    protected final Connection connection;

    /**
     * The target server this phase is a part of.
     */
    private final String threadName;

    /**
     * The abstract constructor.
     * 
     * @param connection
     *            the connection is phase is a part of
     */
    public TargetPhase(Connection connection) {
        this.connection = connection;
        this.threadName = "iSCSI connection from " + connection.getSenderWorkerPeerInfo();
    }

    /**
     * Throws an {@link OperationNotSupportedException} unless overwritten.
     * 
     * @param pdu
     *            the first PDU to be processes as part of the phase
     * @return <code>true</code> if and only if the phase was completed
     *         successfully
     * @throws OperationNotSupportedException
     *             if the method is not overwritten
     * @throws IOException
     *             if an I/O error occurs
     * @throws InterruptedException
     *             if the current Thread is interrupted
     * @throws InternetSCSIException
     *             if a iSCSI protocol violation is detected
     * @throws DigestException
     *             if a PDU digest error is detected
     * @throws SettingsException
     *             if the target tries to access a parameter that has not been
     *             declared or negotiated and that has no default value
     */
    public PHASE_EXEC_STATUS execute(ProtocolDataUnit pdu) throws OperationNotSupportedException, IOException,
        InterruptedException, InternetSCSIException, DigestException, SettingsException {
        throw new OperationNotSupportedException();
    }

    /**
     * Throws an {@link OperationNotSupportedException} unless overwritten.
     * 
     * @return <code>true</code> if and only if the phase was completed
     *         successfully
     * @throws OperationNotSupportedException
     *             if the method is not overwritten
     * @throws IOException
     *             if an I/O error occurs
     * @throws InterruptedException
     *             if the current Thread is interrupted
     * @throws InternetSCSIException
     *             if a iSCSI protocol violation is detected
     * @throws DigestException
     *             if a PDU digest error is detected
     * @throws SettingsException
     *             if the target tries to access a parameter that has not been
     *             declared or negotiated and that has no default value
     */
    public boolean execute() throws OperationNotSupportedException, InternetSCSIException, DigestException,
        IOException, InterruptedException, SettingsException {
        throw new OperationNotSupportedException();
    }

    /**
     * Getting the related connection
     * 
     * @return the connection
     */
    public Connection getTargetConnection() {
        return connection;
    }

    // OODRIVE target phase became callable
    public final Boolean call() throws Exception {
        // OODRIVE: change the name of the current thread
        final Thread myThread = Thread.currentThread();
        final String prevThreadName = myThread.getName();

        myThread.setName(threadName);
        try {
            ProtocolDataUnit pdu = null;
            PHASE_EXEC_STATUS status = PHASE_EXEC_STATUS.GO_ON;
            next: while (status == PHASE_EXEC_STATUS.GO_ON) {
                status = execute(pdu);
                if (status == PHASE_EXEC_STATUS.DONE) {
                    connection.enableRead();
                    return true;
                }
                else if (status == PHASE_EXEC_STATUS.GO_ON) {
                    pdu = connection.receivePdu(100);
                    if (pdu != null) {
                        continue next;
                    }
                    connection.enableRead();
                    return true;
                }
                else if (status == PHASE_EXEC_STATUS.CLOSE) {
                    connection.close();
                    LOGGER.debug("closed connection");
                    return false;
                }
                else {
                    throw new AssertionError("status=" + status);
                }
            }

            // Unreachable
            throw new AssertionError();
        }
        catch (Throwable t) {
            LOGGER.error("Exception thrown", t);
            connection.close();
            return false;
        }
        finally {
            myThread.setName(prevThreadName);
        }
    }
}
