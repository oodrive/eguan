package io.eguan.dtx;

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

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In charge to rollback the transaction.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class RollBackHandler extends AbstractDistOpHandler implements Callable<DistOpResult>, Serializable {

    private static final long serialVersionUID = -7167432370069401655L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RollBackHandler.class);

    private final long txId;

    /**
     * Constructs an instance from any given transaction ID and a set of participants.
     * 
     * @param txId
     *            the transaction id
     * @param participants
     *            {@link Set} of participating nodes
     */
    RollBackHandler(final long txId, final Set<DtxNode> participants) {
        super(participants);
        this.txId = txId;
    }

    @Override
    public final DistOpResult call() {
        try {
            final TransactionManager txMgr = getTransactionManager();
            if (txMgr == null) {
                LOGGER.error("No transaction manager available");
                throw new XAException(XAException.XAER_RMFAIL);
            }
            txMgr.rollback(txId, getParticipants());
        }
        catch (final XAException e) {
            LOGGER.error("Rollback failed; nodeId=" + getNodeId(), e);
            return new DistOpResult(e.errorCode, "Transaction failure; error code=" + e.errorCode);
        }
        catch (final Throwable te) {
            LOGGER.error("Rollback threw exception; nodeId=" + getNodeId(), te);
            return new DistOpResult(-1, te);
        }
        return DistOpResult.NO_ERROR;
    }
}
