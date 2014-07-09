package org.jscsi.target.connection.phase;

import java.io.IOException;
import java.security.DigestException;

import javax.naming.OperationNotSupportedException;

import org.jscsi.exception.InternetSCSIException;
import org.jscsi.parser.BasicHeaderSegment;
import org.jscsi.parser.OperationCode;
import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.parser.login.ISID;
import org.jscsi.parser.login.LoginRequestParser;
import org.jscsi.parser.login.LoginStage;
import org.jscsi.target.TargetServer;
import org.jscsi.target.connection.Connection;
import org.jscsi.target.connection.SessionType;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.connection.stage.login.LoginOperationalParameterNegotiationStage;
import org.jscsi.target.connection.stage.login.SecurityNegotiationStage;
import org.jscsi.target.connection.stage.login.TargetLoginStage;
import org.jscsi.target.settings.ConnectionSettingsNegotiator;
import org.jscsi.target.settings.Settings;
import org.jscsi.target.settings.SettingsException;

/**
 * Objects of this class represent the Target Login Phase of a connection.
 * 
 * @see TargetPhase
 * @author Andreas Ergenzinger
 */
public final class TargetLoginPhase extends TargetPhase {

    /**
     * The current stage of this phase
     */
    private TargetLoginStage stage;

    /**
     * This variable indicates if the initiator is to be considered as
     * authenticated, i.e. if it has given sufficient proof of its identity to
     * proceed to the next (Target Full Feature) phase.
     * <p>
     * Currently the jSCSI Target does not support any authentication methods and this value is initialized to
     * <code>true</code> for all initiators.
     */
    private boolean authenticated = true;// TODO false if authentication
                                         // required

    /**
     * This variable will be <code>true</code> until the first call of {@link #getFirstPduAndSetToFalse()} has
     * happened.
     * <p>
     * This value will be <code>true</code> if the currently processed PDU is the first PDU sent by the
     * initiator over this phase's connection. This means that it must contain all text parameters necessary
     * for either starting a discovery session or a normal session.
     */
    private boolean firstPdu = true;

    /**
     * The constructor.
     * 
     * @param connection
     *            {@inheritDoc}
     */
    public TargetLoginPhase(Connection connection) {
        super(connection);
    }

    /**
     * Starts the login phase.
     * 
     * @param pdu
     *            {@inheritDoc}
     * @return {@inheritDoc}
     * @throws OperationNotSupportedException
     *             {@inheritDoc}
     * @throws IOException
     *             {@inheritDoc}
     * @throws InterruptedException
     *             {@inheritDoc}
     * @throws InternetSCSIException
     *             {@inheritDoc}
     * @throws DigestException
     *             {@inheritDoc}
     * @throws SettingsException
     *             {@inheritDoc}
     */
    @Override
    public PHASE_EXEC_STATUS execute(ProtocolDataUnit pduInput) throws IOException, InterruptedException,
        InternetSCSIException, DigestException, SettingsException {

        assert pduInput == null;
        ProtocolDataUnit pdu ;
        LoginRequestParser parser;
        TargetSession session;
        
        boolean loginSuccessful = false;// will determine if settings are
        // committed
        
        // OODRIVE
        try {
            pdu = connection.receivePdu(); // TODO: socket should be in blocking mode
            // confirm OpCode
            if (pdu.getBasicHeaderSegment().getOpCode() != OperationCode.LOGIN_REQUEST)
                throw new InternetSCSIException();
            // get initiatorSessionID
            parser = (LoginRequestParser)pdu.getBasicHeaderSegment().getParser();
            ISID initiatorSessionID = parser.getInitiatorSessionID();
            int cid = parser.getConnectionID();
            short tsih = parser.getTargetSessionIdentifyingHandle();
            

            /*
             * TODO get (new or existing) session based on TSIH But
             * since we don't do session reinstatement and
             * MaxConnections=1, we can just create a new one.*/
            TargetServer targetServer = connection.getTargetServer(); 
            session = new TargetSession(targetServer, connection, initiatorSessionID, parser
                    .getCommandSequenceNumber(),// set ExpCmdSN
                                                // (PDU is
                                                // immediate,
                                                // hence no ++)
                    parser.getExpectedStatusSequenceNumber()
                    , cid, tsih);
            
        } catch (DigestException | InternetSCSIException | SettingsException e) {
           // LOGGER.info("Throws Exception", e);
            throw e;
        }
        
        // begin login negotiation
        final ConnectionSettingsNegotiator negotiator = connection.getConnectionSettingsNegotiator();
        while (!negotiator.beginNegotiation()) {
            // do nothing, just wait for permission to begin, method is blocking
        }

        try {
            // if possible, enter LOPN Stage
            //BasicHeaderSegment bhs = pdu.getBasicHeaderSegment();
            //LoginRequestParser parser = (LoginRequestParser)bhs.getParser();

            LoginStage nextStageNumber;// will store return value from the last
                                       // login stage

            // Security Negotiation Stage (optional)
            if (parser.getCurrentStageNumber() == LoginStage.SECURITY_NEGOTIATION) {
                // complete SNS
                stage = new SecurityNegotiationStage(this);
                stage.execute(pdu);
                nextStageNumber = stage.getNextStageNumber();

                if (nextStageNumber != null)
                    authenticated = true;
                else {
                    loginSuccessful = false;
                    return PHASE_EXEC_STATUS.CLOSE;
                }

                if (nextStageNumber == LoginStage.LOGIN_OPERATIONAL_NEGOTIATION) {
                    // receive first PDU from LOPNS
                    pdu = connection.receivePdu();
                    BasicHeaderSegment bhs = pdu.getBasicHeaderSegment();
                    parser = (LoginRequestParser)bhs.getParser();
                } else if (nextStageNumber == LoginStage.FULL_FEATURE_PHASE) {
                    // we are done here
                    loginSuccessful = true;
                    return PHASE_EXEC_STATUS.DONE;
                } else {
                    // should be unreachable, since SNS may not return NSG==SNS
                    loginSuccessful = false;
                    return PHASE_EXEC_STATUS.CLOSE;
                }
            }

            // Login Operational Parameter Negotiation Stage (also optional, but
            // either SNS or LOPNS must be passed before proceeding to FFP)
            if (authenticated && parser.getCurrentStageNumber() == LoginStage.LOGIN_OPERATIONAL_NEGOTIATION) {
                stage = new LoginOperationalParameterNegotiationStage(this);
                stage.execute(pdu);
                nextStageNumber = stage.getNextStageNumber();
                if (nextStageNumber == LoginStage.FULL_FEATURE_PHASE) {
                    loginSuccessful = true;
                    return PHASE_EXEC_STATUS.DONE;
                }
                    
            }
            // else
            loginSuccessful = false;
            return PHASE_EXEC_STATUS.CLOSE;
        } catch (DigestException e) {
            loginSuccessful = false;
            throw e;
        } catch (IOException e) {
            loginSuccessful = false;
            throw e;
        } catch (InterruptedException e) {
            loginSuccessful = false;
            throw e;
        } catch (InternetSCSIException e) {
            loginSuccessful = false;
            throw e;
        } catch (SettingsException e) {
            loginSuccessful = false;
            throw e;
        } finally {
            // commit or roll back changes and release exclusive negotiator lock
            negotiator.finishNegotiation(loginSuccessful);

            // OODRIVE
            if (loginSuccessful) {    
                // if this is the leading connection, set the session type
                final Settings settings = connection.getSettings();
                if (connection.isLeadingConnection())
                    session.setSessionType(SessionType.getSessionType(settings.getSessionType()));
                session.setTargetName(settings.getTargetName());
                connection.setPhase(new TargetFullFeaturePhase(connection));
            }
        }
    }

    /**
     * This method will return <code>true</code> if currently processed PDU is
     * the first PDU sent by the initiator over this phase's connection.
     * Subsequent calls will always return <code>false</code>.
     * 
     * @return <code>true</code> if and only if this method is called for the
     *         first time
     */
    public boolean getFirstPduAndSetToFalse() {
        if (!firstPdu)
            return false;
        firstPdu = false;
        return true;
    }

    public final boolean getAuthenticated() {
        return authenticated;
    }
}
