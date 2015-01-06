package com.oodrive.nuage.dtx;

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

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.proto.dtx.DistTxWrapper.TxMessage;

/**
 * In charge to start the transaction.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class StartHandler extends AbstractDistOpHandler implements Callable<DistOpResult>, Serializable {

    private static final long serialVersionUID = 7398759747007852148L;

    private static final Logger LOGGER = LoggerFactory.getLogger(StartHandler.class);

    private final TxMessage transaction;

    /**
     * Constructs an instance from a given transaction and a set of participants.
     * 
     * @param transaction
     *            a non-<code>null</code> {@link TxMessage}
     * @param participants
     *            the set of {@link DtxNode}s participating at this stage
     */
    @ParametersAreNonnullByDefault
    StartHandler(final TxMessage transaction, final Set<DtxNode> participants) {
        super(participants);
        this.transaction = Objects.requireNonNull(transaction);
    }

    @Override
    public final DistOpResult call() {
        try {
            final TransactionManager txMgr = getTransactionManager();
            if (txMgr == null) {
                LOGGER.error("No transaction manager available");
                throw new XAException(XAException.XAER_RMFAIL);
            }
            txMgr.start(transaction, getParticipants());
        }
        catch (final XAException e) {
            LOGGER.error("Start failed; nodeId=" + getNodeId(), e);
            return new DistOpResult(e.errorCode, "Transaction failure; error code=" + e.errorCode);
        }
        catch (final Throwable te) {
            LOGGER.error("Start threw exception; nodeId=" + getNodeId(), te);
            return new DistOpResult(-1, te);
        }

        return DistOpResult.NO_ERROR;
    }

}
