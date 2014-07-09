package org.jscsi.target;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jscsi.parser.ProtocolDataUnit;
import org.jscsi.target.connection.Connection.TargetConnection;
import org.jscsi.target.connection.TargetSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The central class of the jSCSI Target, which keeps track of all active {@link TargetSession}s, stores
 * target-wide parameters and variables, and
 * which contains the {@link #main(String[])} method for starting the program.
 * 
 * @author Andreas Ergenzinger, University of Konstanz
 * @author Sebastian Graf, University of Konstanz
 */
public final class TargetServer implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetServer.class);

    private static final TargetConnection[] EMPTY_TARGET_CONNECTION_ARRAY = new TargetConnection[0];

    /**
     * A {@link SocketChannel} used for listening to incoming connections.
     */
    private ServerSocketChannel serverSocketChannel;

    /**
     * A {@link Selector} used for listening to incoming connections and reading incoming datas
     */
    // Guarded by this
    private Selector selector;

    /**
     * Mark the server as cancelled (atomic access to selector)
     */
    // OODRIVE
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    
    /**
     * Contains all active {@link TargetSession}s.
     */
    private final List<TargetConnection> connections = new ArrayList<>();

    /**
     * The jSCSI Target's global parameters.
     */
    private final Configuration config;

    /**
     * 
     */
    // OODRIVE private DeviceIdentificationVpdPage deviceIdentificationVpdPage;

    /*
     * The table of targets, not case sensitive
     */
    private static final Comparator<String> IGNORECASE_COMPARATOR = new Comparator<String>() {
        public final int compare(final String s1, final String s2) {
            return s1.compareToIgnoreCase(s2);
        }
    };
    private final Map<String, Target> targets = new TreeMap<String, Target>(IGNORECASE_COMPARATOR);
    private final ReadWriteLock targetsLock = new ReentrantReadWriteLock();

    /**
     * A target-wide counter used for providing the value of sent {@link ProtocolDataUnit}s'
     * <code>Target Transfer Tag</code> field, unless
     * that field is reserved.
     */
    private static final AtomicInteger nextTargetTransferTag = new AtomicInteger();

    /**
     * The connection the target server is using.
     */
    // private Connection connection; //OODRIVE UNUSED

    public TargetServer(final Configuration conf) {
        this.config = conf;

        LOGGER.debug("Starting jSCSI-target: ");

        // read target settings from configuration file

        LOGGER.debug("   port:           " + getConfig().getPort());
        LOGGER.debug("   loading targets.");
        // open the storage medium
        targetsLock.writeLock().lock();
        try{
        List<Target> targetInfo = getConfig().getTargets();
        for (Target curTargetInfo : targetInfo) {
            
            targets.put(curTargetInfo.getTargetName(), curTargetInfo);
            // print configuration and medium details
            LOGGER.debug("   target name:    " + curTargetInfo.getTargetName() + " loaded.");
        }
        } finally {targetsLock.writeLock().unlock();}
        
        // OODRIVE this.deviceIdentificationVpdPage = new DeviceIdentificationVpdPage(this);
    }

    /**
     * Gets and increments the value to use in the next unreserved <code>Target Transfer Tag</code> field of
     * the next PDU to be sent by the
     * jSCSI Target.
     * 
     * @see #nextTargetTransferTag
     * @return the value to use in the next unreserved <code>Target Transfer Tag
     * </code> field
     */
    public static int getNextTargetTransferTag() {
        // value 0xffffffff is reserved
        int tag;
        do {
            tag = nextTargetTransferTag.getAndIncrement();
        } while (tag == -1);
        return tag;
    }

    /**
     * Starts the jSCSI target.
     * 
     * @param args
     *            all command line arguments are ignored
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {
        TargetServer target;
        switch (args.length) {
        case 0:
            target = new TargetServer(Configuration.create());
            break;
        case 1:
            target =
                new TargetServer(Configuration.create(Configuration.CONFIGURATION_SCHEMA_FILE, new File(
                    args[0])));
            break;
        case 2:
            target = new TargetServer(Configuration.create(new File(args[0]), new File(args[1])));
            break;
        default:
            throw new IllegalArgumentException(
                "Only zero or one Parameter (Path to Configuration-File) allowed!");
        }
        target.call();
    }

    @Override
    public Void call() throws Exception {

        ExecutorService threadPool = Executors.newFixedThreadPool(4);
        // Create a blocking server socket and check for connections
        try {
            // Create a blocking server socket channel on the specified/default
            // port
            // cancelled: stop now
            if (cancelled.get()) {
                return null;
            }
            serverSocketChannel = ServerSocketChannel.open();
            try {
            serverSocketChannel.configureBlocking(false);
            
            serverSocketChannel.socket().bind(new InetSocketAddress(getConfig().getTargetAddressInetAddress(), getConfig().getPort()));

            //OODRIVE
            synchronized (this) {
                if (cancelled.get()) {
                    return null;
                }
                selector = Selector.open();
            }
            try {
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (selector.select() >= 0 && !cancelled.get()) {
                final Iterator<SelectionKey> keys = selector.selectedKeys()
                        .iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (key.isValid()) {
                        if (key.isAcceptable()) {

                // Accept the connection request.
                // If serverSocketChannel is blocking, this method blocks.
                // The returned channel is in blocking mode.
                final SocketChannel socketChannel = serverSocketChannel.accept();
                TargetConnection connection = null  ;
                try {

                // deactivate Nagle algorithm
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setKeepAlive(true);
                
                            // non-blocking mode 
                            socketChannel.configureBlocking(false);
                
                                    // selection key can not be registered if a thread is blocked in the selector 
                                    // so register it now with null interest set and update interest in another thread
                                    // when necessary
                                    SelectionKey selectionKey = socketChannel.register(selector, 0);
                                    // create a new connection for this client
                                    connection = new TargetConnection(socketChannel, true, selectionKey, this);
                                    synchronized (connections) {
                                        connections.add(connection);
                                    }
                                    threadPool.submit(connection.getPhase());
                                    
                                } catch(Exception e) {
                                    // Client connection not handled: must close
                                    socketChannel.close();
                                    removeTargetConnection(connection);
                                    LOGGER.info("Throws Exception", e);
                                    continue;
                                }
              
                        }
                        else{
                            try{
                            if (key.isReadable()) {
                        
                            SocketChannel clientSocketChannel = (SocketChannel) key.channel();
                            // Disable read selection
                            int opsMask = key.interestOps();
                            opsMask &= ~(SelectionKey.OP_READ);
                            key.interestOps(opsMask);
                            // Handle request
                            final TargetConnection connection = findConnection(clientSocketChannel);
                            if (connection != null) {
                                threadPool.submit(connection.getPhase());
                            }
                        }
                            }catch(final Exception e){
                                LOGGER.info("Throws Exception", e);
                                continue;
                            }
                        }
                }
                }
            }
            } finally {
                selector.close();
                synchronized (this) {
                    selector = null;
                }
            }
            } finally {
                serverSocketChannel.close();
                serverSocketChannel = null;
            }
        } catch (AsynchronousCloseException e) {
            // this block is entered if the server socket is close by cancel()
            LOGGER.debug("Throws Exception", e);
        } catch (IOException e) {
            // this block is entered if the desired port is already in use
            LOGGER.error("Throws Exception", e);
        } finally {
            threadPool.shutdownNow();
        }
        return null;
    }
    
    // OODRIVE
    /**
     * Cancel the server if it is running. 
     */
    public void cancel() {
        final Selector selectorTmp;

        synchronized (this) {
            cancelled.set(true);
            selectorTmp = selector;
        }
        if (selectorTmp != null) {
            try {
                selectorTmp.wakeup();
            }
            catch (Exception e) {
                // Already closed?
                LOGGER.debug("Throws Exception", e);
            }
            // Close the sessions - take a snapshot of the session list
            // to avoid a concurrent access during the close of the sessions
            final TargetConnection[] connectionsTmp;
            synchronized (connections) {
                connectionsTmp = connections.toArray(EMPTY_TARGET_CONNECTION_ARRAY);
            }
            for (int i = 0; i < connectionsTmp.length; i++) {
                connectionsTmp[i].close();
            }
        }
        
    }
    
    // OODRIVE
    public List<TargetStats> getTargetStats() {
        // Get a snapshot of the current list of sessions
        targetsLock.readLock().lock();
        try {
            final List<TargetStats> result = new ArrayList<>(targets.size());
            for (Iterator<Target> iterator = targets.values().iterator(); iterator.hasNext();) {
                final Target target = iterator.next();
                final String targetName = target.getTargetName();
                // Look for sessions associated to that target
                final int count = getSessionsCount(targetName);
                final long size = target.getStorageModule().getSizeInBlocks()
                        * target.getStorageModule().getBlockSize();
                final TargetStats stats = new TargetStats(targetName, target.getTargetAlias(), count, size, target
                        .getStorageModule().isWriteProtected());

                result.add(stats);
            }
            return result;
        }
        finally {
            targetsLock.readLock().unlock();
        }
    }
    
    // OODRIVE
    private int getSessionsCount(String targetName){
        final TargetConnection[] connectionsTmp;
        synchronized (connections) {
            connectionsTmp = connections.toArray(EMPTY_TARGET_CONNECTION_ARRAY);
        }
        int count = 0;
        for (TargetConnection connection : connectionsTmp) {
            final TargetSession targetSession = connection.getTargetSession();
            if (targetSession == null){
                continue;
            }
            if (targetName.equalsIgnoreCase(targetSession.getTargetName())) {
                // one connection per session yet
                count++;
            }
        }
        return count;
    }
    
    // OODRIVE
    public boolean checkSessionParameters(TargetConnection newConnection, String targetName) {

        final TargetConnection[] connectionsTmp;
        final TargetSession newSession = newConnection.getTargetSession();

        synchronized (connections) {
            connectionsTmp = connections.toArray(EMPTY_TARGET_CONNECTION_ARRAY);
        }

        for (TargetConnection targetConnection : connectionsTmp) {
            final TargetSession targetSession = targetConnection.getTargetSession();
            if (targetSession == null) {
                continue;
            }

            if (targetName.equalsIgnoreCase(targetSession.getTargetName()) // same target name
                    && newSession.getInitiatorSessionID().equals(targetSession.getInitiatorSessionID())) { // existing
                                                                                                           // ISID
                if (newSession.getInitiatorSessionHandle() == 0) { // TSIH zero
                    // CID : any
                    LOGGER.debug("Session reinstatement : disconnect old session");
                    targetConnection.close(); // disconnect the old socket
                    return true;
                }
                else { // TSIH non-zero
                    if (newSession.getInitiatorSessionHandle() == targetSession.getTargetSessionIdentifyingHandle()) { // existing
                                                                                                                       // TSIH
                        if (newConnection.getConnectionID() == targetConnection.getConnectionID()) { // existing CID
                            LOGGER.debug("Connection reinstatement : disconnect old connection");
                            targetConnection.close(); // disconnect the old socket
                            return true;
                        }
                        else {
                            LOGGER.debug("Client is trying to add a new connection to an existing session : FAIL");
                            return false;
                        }
                    }
                    else { // new TSIH
                           // CID : any
                        LOGGER.debug("Client is trying to use a none zero TSIH which does not correspond to the ISID : FAIL");
                        return false;
                    }
                }
            }
        }
        // No session present, just check the Session handle
        if (newSession.getInitiatorSessionHandle() == 0) { // TSIH zero
            LOGGER.debug("Open a new session : OK");
            return true;
        }
        else {
            // CID : any
            LOGGER.debug("Client is trying to use a non zero TSIH for a new session : FAIL");
            return false;
        }
    }

    /**
     * Add a target.
     * 
     * @param target target to add
     * @return previous target associated to that TargetName or null
     */
    public Target addTarget(Target target) {
        final Target prev;
        targetsLock.writeLock().lock();
        try {
            prev = targets.put(target.getTargetName(), target);
        }
        finally {
            targetsLock.writeLock().unlock();
        }
        // print configuration and medium details
        if (prev == null) {
            LOGGER.debug("Target name: " + target.getTargetName() + " loaded");
        }
        else {
            LOGGER.debug("Target name: " + target.getTargetName() + " reloaded");
        }
        return prev;
    }

    public Target removeTarget(String targetName) {
        if (targetName == null)
            throw new NullPointerException();
        final Target prev;
        targetsLock.writeLock().lock();
        try {
            prev = targets.remove(targetName);
        }
        finally {
            targetsLock.writeLock().unlock();
        }
        // print configuration and medium details
        if (prev == null) {
            // Nothing removed
            return null;
        }
        LOGGER.debug("Target name: " + targetName + " removed");

        // Close sessions: get a snapshot of the session list (do
        // not lock the session list during the close of the sessions)
        final TargetConnection[] connectionsTmp;
        synchronized (connections) {
            connectionsTmp = connections.toArray(EMPTY_TARGET_CONNECTION_ARRAY);
        }
        for (int i = 0; i < connectionsTmp.length; i++) {
            final TargetConnection connection = connectionsTmp[i];
            final TargetSession targetSession = connection.getTargetSession();
            if (targetSession != null) {
                if (targetName.equalsIgnoreCase(targetSession.getTargetName())) {
                    connection.close();
                }
            }
        }
        return prev;
    }

    public Configuration getConfig() {
        return config;
    }

    /* OODRIVE 
    public DeviceIdentificationVpdPage getDeviceIdentificationVpdPage() {
        return deviceIdentificationVpdPage;
    }
    */

    public Target getTarget(String targetName) {
        targetsLock.readLock().lock();
        try {
            return targets.get(targetName);
        } finally {targetsLock.readLock().unlock();}
    }

    /**
     * Removes a session from the jSCSI Target's list of active sessions.
     * 
     * @param session
     *            the session to remove from the list of active sessions
     */
    public void removeTargetConnection(TargetConnection connection) {
        synchronized(connections) {
        connections.remove(connection);
        }
    }

    // OODRIVE
    private final TargetConnection findConnection(SocketChannel socket) {
        synchronized (connections) {
            for (int i = connections.size() - 1; i >= 0; i--) {
                final TargetConnection connection = connections.get(i);
                if (socket.equals(connection.getSocketChannel())) {
                    return connection;
                }
            }
            // Not found
            return null;
        }
    }

    public String[] getTargetNames() {
        targetsLock.readLock().lock();
        try {
        final String[] returnNames = new String[targets.size()];
        return targets.keySet().toArray(returnNames);
        } finally {targetsLock.readLock().unlock();}
    }

    /**
     * Checks to see if this target name is valid.
     * 
     * @param checkTargetName
     * @return true if the the target name is configured
     */
    public boolean isValidTargetName(String checkTargetName) {
        targetsLock.readLock().lock();
        try {
        return targets.containsKey(checkTargetName);
        } finally {targetsLock.readLock().unlock();}
    }

    /**
     * Using this connection mainly for test pruposes.
     * 
     * @return the connection the target server established.
     */
//    public Connection getConnection() {
//        return this.connection;
//    }

    /**
     * Gets the {@link TargetServer} selector. May be null.
     * 
     * @return the selector
     */
    public final Selector getSelector() {
        synchronized (this) {
            return selector;
        }
    }


}
