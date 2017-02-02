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

import io.eguan.proto.dtx.DistTxWrapper.TxJournalEntry;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed operation handler returning an {@link Iterable Iterable<TxJournalEntry>} of transactions retrieved from
 * the distant node for synchronization purposes.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class SyncDataHandler extends AbstractDistOpHandler implements Callable<Iterable<TxJournalEntry>> {

    private static final long serialVersionUID = -3576038253973389705L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataHandler.class);

    private final UUID resMgrId;
    private final long lastLocalTxId;
    private final long targetTxId;

    /**
     * Constructs a new instance for a given synchronization data retrieval.
     * 
     * @param targetResMgrId
     *            the non-<code>null</code> {@link UUID} of the target resource manager
     * @param lastLocalTxId
     *            the last local transaction ID, i.e. the one from which upward to retrieve synchronization data
     * @param targetTxId
     *            the synchronization target transaction ID, i.e. the ID up to which to retrieve synchronization data
     */
    public SyncDataHandler(@Nonnull final UUID targetResMgrId, final long lastLocalTxId, final long targetTxId) {
        super(null);
        this.resMgrId = Objects.requireNonNull(targetResMgrId);
        this.lastLocalTxId = lastLocalTxId;
        this.targetTxId = targetTxId;
    }

    @Override
    public final Iterable<TxJournalEntry> call() throws Exception {
        final TransactionManager txMgr = getTransactionManager();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting sync data; resId=" + resMgrId + ", minTxId=" + lastLocalTxId + ", targetTxId="
                    + targetTxId);
        }
        return txMgr.extractTransactions(resMgrId, lastLocalTxId, targetTxId);
    }

}
