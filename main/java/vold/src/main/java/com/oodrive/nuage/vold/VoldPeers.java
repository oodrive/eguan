package com.oodrive.nuage.vold;

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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.management.JMException;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;

import com.oodrive.nuage.configuration.AbstractConfigKey;
import com.oodrive.nuage.configuration.ConfigValidationException;
import com.oodrive.nuage.dtx.DtxTaskApi;
import com.oodrive.nuage.dtx.DtxTaskFutureVoid;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.Common.Uuid;
import com.oodrive.nuage.proto.vvr.VvrRemote;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.proto.vvr.VvrRemote.VoldPeerMsg.Action;
import com.oodrive.nuage.vold.model.Constants;
import com.oodrive.nuage.vvr.remote.VvrRemoteUtils;

/**
 * Update the list of peers.
 * 
 * @author oodrive
 * @author jmcaba
 * @author ebredzinski
 */
final class VoldPeers {

    public interface Schedulable extends Runnable {
        public abstract void schedule();
    }

    /**
     * TimerTask to auto exit vold after a scheduled delay.
     * 
     */
    static final class VoldSuicideTask extends TimerTask implements Schedulable {
        private static final long SUICIDE_DELAY = 2 * 1000 * 1000;// 2 min delay
        private static Timer suicideTimer = new Timer();

        public VoldSuicideTask() {
            super();
        }

        @Override
        public final void run() {
            System.exit(0);
        }

        /**
         * Schedule auto exit of this vold.
         */
        @Override
        public final void schedule() {
            suicideTimer.schedule(this, SUICIDE_DELAY);
        }
    }

    private static final Logger LOGGER = Constants.LOGGER;
    private static Schedulable suicideTask = new VoldSuicideTask();

    private enum DtxOperation {
        PREPARE, COMMIT, ROLLBACK;
    }

    private final Vold vold;

    VoldPeers(final Vold vold) {
        super();
        this.vold = vold;
    }

    final Boolean prepare(final VoldDtxRmContext dtxContext) throws XAException {
        try {
            parseHandleOperation(dtxContext, DtxOperation.PREPARE);
            return Boolean.TRUE;
        }
        catch (final IllegalStateException | IllegalArgumentException e) {
            // Most of the time, a pre-condition error
            LOGGER.error("Exception on prepare", e);
            final XAException xaException = new XAException(XAException.XA_RBROLLBACK);
            xaException.initCause(e);
            throw xaException;
        }
    }

    final void commit(final VoldDtxRmContext dtxContext) throws XAException {
        try {
            parseHandleOperation(dtxContext, DtxOperation.COMMIT);
        }
        catch (final IllegalStateException | IllegalArgumentException e) {
            LOGGER.error("Exception on commit", e);
            final XAException xaException;
            if (e instanceof IllegalArgumentException) {
                xaException = new XAException(XAException.XAER_INVAL);
            }
            else {
                xaException = new XAException(XAException.XA_RBROLLBACK);
            }
            xaException.initCause(e);
            throw xaException;
        }
    }

    final void rollback(final VoldDtxRmContext dtxContext) throws XAException {
        try {
            parseHandleOperation(dtxContext, DtxOperation.ROLLBACK);
        }
        catch (final IllegalStateException | IllegalArgumentException e) {
            LOGGER.error("Exception on rollback", e);
            final XAException xaException;
            if (e instanceof IllegalArgumentException) {
                xaException = new XAException(XAException.XAER_INVAL);
            }
            else {
                xaException = new XAException(XAException.XA_RBROLLBACK);
            }
            xaException.initCause(e);
            throw xaException;
        }
    }

    private final void handlePrepareAddPeer(final VoldDtxRmContext dtxContext, final UUID node,
            final InetSocketAddress addr) {
        final List<VoldLocation> oldPeersList = vold.getPeersList();
        // maybe the node to add is ourself
        final VoldLocation localNode = vold.getVoldLocation();
        if (localNode.getNode().equals(node)) {
            LOGGER.debug("The Node to add is ourself, do nothing");
            return;
        }
        final List<VoldLocation> newPeersList = constructNewPeersListForAdd(node, addr, oldPeersList);
        saveNewPeers(newPeersList);
        dtxContext.setOldPeersList(oldPeersList);
    }

    private final void handlePrepareRemovePeer(final VoldDtxRmContext dtxContext, final UUID node) {
        final List<VoldLocation> oldPeersList = vold.getPeersList();
        final List<VoldLocation> newPeersList = new ArrayList<VoldLocation>();
        final VoldLocation nodeToRemove = constructNewPeersListForDel(node, oldPeersList, newPeersList);
        if (nodeToRemove == null) {
            // maybe the node to remove is ourself
            final VoldLocation localNode = vold.getVoldLocation();
            if (localNode.getNode().equals(node)) {
                LOGGER.debug("Need to stop this node='" + node + "'");
            }
            else {
                throw new IllegalArgumentException("Could not find node='" + node + "' to remove");
            }
        }
        saveNewPeers(newPeersList);
        dtxContext.setOldPeersList(oldPeersList);
        // need address to remove for commit
        dtxContext.setNodeAddress(nodeToRemove.getSockAddr());
    }

    private final void handleCommitAddPeer(final VoldDtxRmContext dtxContext, final UUID node,
            final InetSocketAddress addr) {
        LOGGER.debug("Commit add peer started ...");
        // maybe the node to add is ourself
        final VoldLocation localNode = vold.getVoldLocation();
        if (localNode.getNode().equals(node)) {
            LOGGER.debug("The Node to add is ourself, do nothing");
            return;
        }
        vold.registerPeer(node, addr);
        LOGGER.debug("Commit add peer ended normally ...");
    }

    private final void handleCommitRemovePeer(final VoldDtxRmContext dtxContext, final UUID node) {
        LOGGER.debug("Commit remove peer started ...");
        final VoldLocation localNode = vold.getVoldLocation();
        if (localNode.getNode().equals(node)) {
            // the node to remove is ourself
            // in this case just schedule exit ...
            LOGGER.debug("Schedule stop of this node='" + node + "'");
            suicideTask.schedule();
        }
        else {
            vold.unregisterPeer(node, dtxContext.getNodeAddress());
        }
        LOGGER.debug("Commit remove peer ended normally ...");
    }

    private final void restoreOldPeersList(final VoldDtxRmContext dtxContext) {
        final List<VoldLocation> oldPeersList = dtxContext.getOldPeersList();
        if (oldPeersList != null) {
            // restore old peer list
            saveNewPeers(oldPeersList);
        }
    }

    private final void handleRollbackAddPeer(final VoldDtxRmContext dtxContext, final UUID node,
            final InetSocketAddress addr) {
        restoreOldPeersList(dtxContext);
    }

    private final void handleRollbackRemovePeer(final VoldDtxRmContext dtxContext, final UUID node) {
        restoreOldPeersList(dtxContext);
    }

    private final void parseHandleOperation(final VoldDtxRmContext dtxContext, final DtxOperation dtxOperation)
            throws IllegalArgumentException {
        final RemoteOperation op = dtxContext.getOperation();
        final OpCode opCode = op.getOp();

        switch (opCode) {
        case SET: {
            if (!op.hasPeer()) {
                throw new IllegalArgumentException("Peer not set");
            }
            final VvrRemote.VoldPeerMsg peer = op.getPeer();
            if (!peer.hasAction()) {
                throw new IllegalArgumentException("Action not set");
            }
            final VvrRemote.VoldPeerMsg.Action action = peer.getAction();
            if (!peer.hasNode()) {
                throw new IllegalArgumentException("Node not set");
            }
            final Uuid nodeUuid = peer.getNode();
            final UUID node = VvrRemoteUtils.fromUuid(nodeUuid);
            switch (action) {
            case ADD: {
                if (!peer.hasIp()) {
                    throw new IllegalArgumentException("Ip not set");
                }
                final String ip = peer.getIp();
                if (!peer.hasPort()) {
                    throw new IllegalArgumentException("Port not set");
                }
                final int port = peer.getPort();
                final InetSocketAddress addr = new InetSocketAddress(ip, port);
                if (dtxOperation == DtxOperation.PREPARE) {
                    handlePrepareAddPeer(dtxContext, node, addr);
                }
                else if (dtxOperation == DtxOperation.COMMIT) {
                    handleCommitAddPeer(dtxContext, node, addr);
                }
                else {
                    handleRollbackAddPeer(dtxContext, node, addr);
                }
            }
                break;
            case REM: {
                if (dtxOperation == DtxOperation.PREPARE) {
                    handlePrepareRemovePeer(dtxContext, node);
                }
                else if (dtxOperation == DtxOperation.COMMIT) {
                    handleCommitRemovePeer(dtxContext, node);
                }
                else {
                    handleRollbackRemovePeer(dtxContext, node);
                }
            }
                break;
            default:
                LOGGER.warn("Unexpected message on VOLD, action=" + action);
            }
        }
            break;
        default:
            LOGGER.warn("Unexpected message on VOLD, op=" + op);
        }
    }

    /**
     * Construct list of peers as String to save configuration.
     * 
     * @param peers
     * @return
     */
    private final static String constructPeers(@Nonnull final List<VoldLocation> peers) {
        final StringBuilder result = new StringBuilder();
        boolean filled = false;
        if (peers != null) {
            for (final VoldLocation peer : peers) {
                if (filled) {
                    result.append(",");
                }
                filled = true;
                result.append(peer.toString());
            }
        }
        return result.toString();
    }

    /**
     * Construct new list of peers for addition of a node
     * 
     * @param node
     * @param addr
     * @param peers
     * @return
     */
    private final static ArrayList<VoldLocation> constructNewPeersListForAdd(@Nonnull final UUID node,
            @Nonnull final InetSocketAddress addr, final List<VoldLocation> peers) {
        final ArrayList<VoldLocation> newPeers = new ArrayList<>();
        if (peers != null) {
            for (final VoldLocation oldLocation : peers) {
                if (oldLocation.getNode().equals(node)) {
                    final String warnMsg = "uuid='" + node + "' already in peers list";
                    LOGGER.warn(warnMsg);
                    throw new IllegalArgumentException(warnMsg);
                }
                newPeers.add(oldLocation);
            }
        }
        final VoldLocation newPeer = new VoldLocation(node, addr);
        newPeers.add(newPeer);
        return newPeers;
    }

    /**
     * Construct new list of peers for deletion of a node
     * 
     * @param uuid
     * @param peers
     * @param returnList
     * @return
     */
    private final static VoldLocation constructNewPeersListForDel(@Nonnull final UUID uuid,
            final List<VoldLocation> peers, final List<VoldLocation> returnList) {
        final List<VoldLocation> newPeers = new ArrayList<VoldLocation>();
        VoldLocation nodeToRemove = null;
        if (peers != null) {
            for (final VoldLocation oldLocation : peers) {
                if (oldLocation.getNode().equals(uuid)) {
                    // Node to remove
                    nodeToRemove = new VoldLocation(oldLocation.getNode(), oldLocation.getSockAddr());
                }
                else {
                    newPeers.add(oldLocation);
                }
            }
        }
        returnList.clear();
        returnList.addAll(newPeers);
        return nodeToRemove;
    }

    /**
     * Save new peers list in configuration. Configuration is always consistent.
     * 
     * @param newPeers
     * @throws IllegalStateException
     */
    private void saveNewPeers(@Nonnull final List<VoldLocation> newPeers) {
        final String newValue = constructPeers(newPeers);
        LOGGER.debug("Vold peers new list=" + newValue);
        final Map<AbstractConfigKey, Object> newKeyValueMap = new HashMap<>();
        newKeyValueMap.put(PeersConfigKey.getInstance(), newPeers);
        try {
            vold.updateConfiguration(newKeyValueMap);
        }
        catch (IOException | ConfigValidationException e) {
            final String msg = "Failed to save peers new list=" + newValue;
            LOGGER.warn(msg, e);
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Check port range according to RFC6335
     * 
     * @see http://tools.ietf.org/html/rfc6335#section-8.1
     * 
     * @param port
     */
    private static void checkPortRange(final int port) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port=" + port + " is not in [0-65535] range!");
        }
        if (port < 1024) {
            throw new IllegalArgumentException("Can not use port=" + port + " in [0-1024] system range!");
        }
    }

    /**
     * Add a new peer to the {@link Vold}.
     * 
     * @param uuid
     * @param address
     * @param port
     * @throws JMException
     */
    final void addPeer(@Nonnull final String uuid, @Nonnull final String address, @Nonnegative final int port,
            final DtxTaskApi dtxTaskApi) throws JMException {
        // 1) check parameters
        Objects.requireNonNull(uuid, "Uuid parameter not provided !");
        Objects.requireNonNull(address, "Ip address parameter not provided !");
        checkPortRange(port);
        final VoldLocation localPeer = vold.getVoldLocation();
        if (UUID.fromString(uuid) == localPeer.getNode()) {
            throw new IllegalArgumentException("Can't add local peer='" + uuid + "' !");
        }

        final String newPeer = uuid + "@" + address + ":" + port;
        final VoldLocation location = VoldLocation.fromString(newPeer);

        final InetAddress addrTmp = location.getSockAddr().getAddress();
        if (addrTmp == null) {
            throw new IllegalArgumentException("address='" + addrTmp + "' unresolved !");
        }

        // 2) Submit transaction
        if (dtxTaskApi == null) {
            throw new IllegalArgumentException("Can't start transaction without DTX manager !");
        }
        submitAddPeerTask(location.getNode(), location.getSockAddr(), dtxTaskApi);
    }

    /**
     * Add a new peer to the {@link Vold}.
     * 
     * @param uuid
     * @param address
     * @param port
     * @throws JMException
     * @return The {@link UUID} of the task handling the operation as a String
     */
    final String addPeerNoWait(final String uuid, final String address, final int port, final DtxTaskApi dtxTaskApi)
            throws JMException {
        // 1) check parameters
        Objects.requireNonNull(uuid, "Uuid parameter not provided !");
        Objects.requireNonNull(address, "Ip address parameter not provided !");
        checkPortRange(port);
        final VoldLocation localPeer = vold.getVoldLocation();
        if (UUID.fromString(uuid) == localPeer.getNode()) {
            throw new IllegalArgumentException("Can't add local peer='" + uuid + "' !");
        }

        final String newPeer = uuid + "@" + address + ":" + port;
        final VoldLocation location = VoldLocation.fromString(newPeer);

        final InetAddress addrTmp = location.getSockAddr().getAddress();
        if (addrTmp == null) {
            throw new IllegalArgumentException("address='" + addrTmp + "' unresolved !");
        }

        // 2) Submit transaction
        if (dtxTaskApi == null) {
            throw new IllegalArgumentException("Can't start transaction without DTX manager !");
        }
        return submitAddPeerTaskNoWait(location.getNode(), location.getSockAddr(), dtxTaskApi).toString();
    }

    /**
     * Remove a peer from the {@link Vold}.
     * 
     * @param peer
     * @throws JMException
     */
    final void removePeer(@Nonnull final String uuid, final DtxTaskApi dtxTaskApi) throws JMException {
        // 1) check parameters
        Objects.requireNonNull(uuid, "Uuid parameter not provided !");
        final UUID node = UUID.fromString(uuid);
        final VoldLocation localPeer = vold.getVoldLocation();
        if (UUID.fromString(uuid) == localPeer.getNode()) {
            LOGGER.debug("Will remove local peer='" + uuid + "' !");
        }

        // 2) Submit transaction
        if (dtxTaskApi == null) {
            throw new IllegalArgumentException("Can't start transaction without DTX manager !");
        }
        submitRemovePeerTask(node, dtxTaskApi);
    }

    /**
     * Remove a peer from the {@link Vold}.
     * 
     * @param peer
     * @throws JMException
     * @return The {@link UUID} of the task handling the operation as a String
     */
    final String removePeerNoWait(final String uuid, final DtxTaskApi dtxTaskApi) throws JMException {
        // 1) check parameters
        Objects.requireNonNull(uuid, "Uuid parameter not provided !");
        final UUID node = UUID.fromString(uuid);
        final VoldLocation localPeer = vold.getVoldLocation();
        if (UUID.fromString(uuid) == localPeer.getNode()) {
            LOGGER.debug("Will remove local peer='" + uuid + "' !");
        }

        // 2) Submit transaction
        if (dtxTaskApi == null) {
            throw new IllegalArgumentException("Can't start transaction without DTX manager !");
        }
        return submitRemovePeerTaskNoWait(node, dtxTaskApi).toString();
    }

    private final UUID submitAddPeerTaskNoWait(@Nonnull final UUID node, @Nonnull final InetSocketAddress addr,
            @Nonnull final DtxTaskApi dtxTaskApi) {
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();

        final VvrRemote.VoldPeerMsg.Builder peerBuilder = VvrRemote.VoldPeerMsg.newBuilder();
        peerBuilder.setAction(Action.ADD);
        peerBuilder.setNode(VvrRemoteUtils.newUuid(node));
        peerBuilder.setIp(addr.getAddress().getHostAddress());
        peerBuilder.setPort(addr.getPort());

        final VvrRemote.VoldPeerMsg peerMsg = peerBuilder.build();
        opBuilder.setPeer(peerMsg);

        return submitSetTransaction(opBuilder, dtxTaskApi);
    }

    private final void submitAddPeerTask(@Nonnull final UUID node, @Nonnull final InetSocketAddress addr,
            @Nonnull final DtxTaskApi dtxTaskApi) throws IllegalStateException {
        final UUID taskId = submitAddPeerTaskNoWait(node, addr, dtxTaskApi);
        waitTaskEnd(taskId, dtxTaskApi);
    }

    private final UUID submitRemovePeerTaskNoWait(@Nonnull final UUID node, @Nonnull final DtxTaskApi dtxTaskApi) {
        final RemoteOperation.Builder opBuilder = RemoteOperation.newBuilder();

        final VvrRemote.VoldPeerMsg.Builder peerBuilder = VvrRemote.VoldPeerMsg.newBuilder();
        peerBuilder.setAction(Action.REM);
        peerBuilder.setNode(VvrRemoteUtils.newUuid(node));

        final VvrRemote.VoldPeerMsg peerMsg = peerBuilder.build();
        opBuilder.setPeer(peerMsg);

        return submitSetTransaction(opBuilder, dtxTaskApi);
    }

    private final void submitRemovePeerTask(@Nonnull final UUID node, @Nonnull final DtxTaskApi dtxTaskApi)
            throws IllegalStateException {
        final UUID taskId = submitRemovePeerTaskNoWait(node, dtxTaskApi);
        waitTaskEnd(taskId, dtxTaskApi);
    }

    /**
     * Wait for a task end.
     * 
     * @param taskId
     */
    private final void waitTaskEnd(@Nonnull final UUID taskId, @Nonnull final DtxTaskApi dtxTaskApi) {

        // Wait for task end
        final DtxTaskFutureVoid future = new DtxTaskFutureVoid(taskId, dtxTaskApi);
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Submit a SET transaction
     * 
     * @param opBuilder
     * @return
     */
    private final UUID submitSetTransaction(@Nonnull final RemoteOperation.Builder opBuilder,
            @Nonnull final DtxTaskApi dtxTaskApi) {
        final UUID ownerUuid = vold.getOwnerUuid();
        final UUID nodeUuid = vold.getNodeUuid();
        final Uuid msgSource = VvrRemoteUtils.newUuid(nodeUuid);
        final UUID resourceId = ownerUuid;

        // for debug message in exceptions only
        opBuilder.setUuid(msgSource);

        final UUID taskId = VvrRemoteUtils.submitTransaction(opBuilder, dtxTaskApi, resourceId, msgSource, Type.VOLD,
                OpCode.SET);
        return taskId;
    }

}
