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

import java.util.Set;
import java.util.concurrent.Callable;

import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In charge to commit the transaction.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class CommitHandler extends AbstractDistOpHandler implements Callable<DistOpResult> {

    private static final long serialVersionUID = -5927010774805523622L;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommitHandler.class);

    private final long txId;

    /**
     * Constructs an instance from a given transaction ID and a set of participants.
     * 
     * @param txId
     *            the transaction id
     * @param participants
     *            the set of {@link DtxNode}s participating at this stage
     */
    CommitHandler(final long txId, final Set<DtxNode> participants) {
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
            getTransactionManager().commit(txId, getParticipants());
        }
        catch (final XAException e) {
            LOGGER.error("Commit failed; nodeId=" + getNodeId(), e);
            return new DistOpResult(e.errorCode, "Transaction failure; error code=" + e.errorCode);
        }
        catch (final Throwable te) {
            LOGGER.error("Commit threw exception; nodeId=" + getNodeId(), te);
            return new DistOpResult(-1, te);
        }
        return DistOpResult.NO_ERROR;
    }

}
