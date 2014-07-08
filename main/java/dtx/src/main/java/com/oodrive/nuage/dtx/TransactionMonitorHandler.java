package com.oodrive.nuage.dtx;

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

import static com.oodrive.nuage.dtx.DtxResourceManagerState.UNDETERMINED;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;

/**
 * Transaction monitor to be sent to all nodes participating in a transaction.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class TransactionMonitorHandler extends AbstractDistOpHandler implements Runnable {

    private class ShutdownListener implements LifecycleListener, Serializable {

        private static final long serialVersionUID = 3075326218179487411L;

        @Override
        public void stateChanged(final LifecycleEvent event) {
            switch (event.getState()) {
            case SHUTTING_DOWN:
            case SHUTDOWN:
                shutdown = true;
                break;
            default:
                // nothing
            }

        }
    }

    private static final long serialVersionUID = 3261167430220912186L;

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMonitorHandler.class);

    private static final int TX_CHECK_OCCURRENCES = 10;

    private final long txId;
    private final long timeout;

    private final UUID resId;

    private final LifecycleListener shutdownListener = new ShutdownListener();

    private boolean shutdown;

    /**
     * Constructs an instance for a given transaction.
     * 
     * @param txId
     *            the transaction's ID
     * @param resId
     *            the target resource manager's {@link UUID}
     * @param timeout
     *            the timeout in milliseconds beyond which the transaction is to be rolled back locally
     * @param participants
     *            the {@link Set} of participants to record with the transaction rollback
     */
    TransactionMonitorHandler(final long txId, final UUID resId, final long timeout, final Set<DtxNode> participants) {
        super(participants);
        this.txId = txId;
        this.resId = resId;
        this.timeout = timeout;
    }

    @Override
    public final void run() {
        final HazelcastInstance hzInstance = getHazelcastInstance();
        final AtomicNumber currCounter = hzInstance.getAtomicNumber(TransactionInitiator.TX_CURRENT_ID);
        final long limit = System.currentTimeMillis() + timeout;

        hzInstance.getLifecycleService().addLifecycleListener(shutdownListener);

        try {

            final TransactionManager txMgr = getTransactionManager();

            final long txCheckInterval = timeout / TX_CHECK_OCCURRENCES;
            long currTxId;
            do {
                try {
                    currTxId = currCounter.get();
                    Thread.sleep(txCheckInterval);
                }
                catch (IllegalStateException | InterruptedException e) {
                    // gracefully exit if monitoring conditions are degraded
                    return;
                }
            } while (currTxId <= txId && System.currentTimeMillis() < limit);

            /*
             * TODO: before calling any "starveable" methods (getting resource managers or last tx IDs), seek out and
             * destroy any previous deadlocked or starving thread still holding those locks
             */

            final DtxResourceManager targetResMgr = txMgr.getRegisteredResourceManager(resId);

            if (targetResMgr == null) {
                // resource manager vanished, abort
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Resource manager gone, aborting; txId=" + txId + ", resId=" + resId + ", node="
                            + txMgr.getLocalNode());
                }
                return;
            }

            final long lastTxId = txMgr.getLastCompleteTxIdForResMgr(resId);

            // transaction passed
            if (currTxId > txId) {
                if (lastTxId < txId && targetResMgr != null) {
                    // transaction end phase didn't end up here -> resync
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Transaction not recorded locally; txId=" + txId + ", last local=" + lastTxId);
                    }
                    txMgr.setResManagerSyncState(resId, UNDETERMINED);
                }
                return;
            }

            // transaction monitor timed out
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Monitor on transaction timed out; txId=" + txId + ", nodeId=" + hzInstance.getName());
            }
            try {
                try {
                    if (lastTxId < txId && targetResMgr != null) {
                        // roll back locally
                        txMgr.rollback(txId, getParticipants());
                    }
                }
                catch (final XAException e) {
                    LOGGER.error("Rollback after timeout failed; txId=" + txId + ", nodeId=" + hzInstance.getName()
                            + ", errorCode=" + e.errorCode);
                    txMgr.setResManagerSyncState(resId, UNDETERMINED);
                }
                finally {
                    if (!shutdown) {
                        DtxUtils.updateAtomicNumberToAtLeast(currCounter, txId);
                    }
                }
            }
            catch (final IllegalStateException e) {
                LOGGER.error("Illegal state; txId=" + txId + ", nodeId=" + hzInstance.getName(), e);
            }
        }
        finally {
            hzInstance.getLifecycleService().removeLifecycleListener(shutdownListener);
        }

    }
}
