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
import java.util.concurrent.Callable;

import javax.transaction.xa.XAException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In charge to prepare the transaction.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class PrepareHandler extends AbstractDistOpHandler implements Callable<DistOpResult>, Serializable {

    private static final long serialVersionUID = -3741072614196574835L;

    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareHandler.class);

    private final long txId;

    /**
     * Constructs an instance from a given transaction ID.
     * 
     * @param txId
     *            the transaction id
     */
    public PrepareHandler(final long txId) {
        super(null);
        this.txId = txId;
    }

    @Override
    public final DistOpResult call() throws Exception {
        Boolean result = Boolean.FALSE;

        try {
            final TransactionManager txMgr = getTransactionManager();
            if (txMgr == null) {
                LOGGER.error("No transaction manager available");
                throw new XAException(XAException.XAER_RMFAIL);
            }
            result = txMgr.prepare(txId);
        }
        catch (final XAException e) {
            LOGGER.error("Prepare failed; nodeId=" + getNodeId(), e);
            return new DistOpResult(e.errorCode, "Transaction failure; error code=" + e.errorCode);
        }
        catch (final Throwable te) {
            LOGGER.error("Prepare threw exception; nodeId=" + getNodeId(), te);
            return new DistOpResult(-1, te);
        }

        return new DistOpResult(result.booleanValue() ? 0 : -1);
    }
}
