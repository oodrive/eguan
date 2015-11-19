package io.eguan.dtx;

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

import java.util.UUID;

import javax.transaction.xa.XAException;

/**
 * Interface to be implemented by classes managing objects to be included in transactional processing.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public interface DtxResourceManager {

    /**
     * Gets the global unique ID of the entity represented by this instance.
     * 
     * @return a {@link UUID} identifying the {@link DtxResourceManager}
     */
    public UUID getId();

    /**
     * Starts processing a transaction by creating the transient context in which all state related to the transaction
     * will be stored.
     * 
     * @param payload
     *            the binary payload describing the transaction's operation
     * @return a context holding all state associated to the transaction
     * @throws XAException
     *             if the transaction context cannot be created with the following error return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_INVAL} if the payload is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public DtxResourceManagerContext start(byte[] payload) throws XAException, NullPointerException;

    /**
     * Prepares the transaction given by the context argument for commit.
     * 
     * @param context
     *            the {@link DtxResourceManagerContext} associated with the transaction
     * @return <code>true</code> if this instance is prepared to commit the transaction, <code>false</code> otherwise
     * @throws XAException
     *             if preparing the transaction in this context failed, with the following error return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_PROTO} if the given context is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBDEADLOCK} transaction must be rolled back due to a deadlock</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     */
    Boolean prepare(DtxResourceManagerContext context) throws XAException;

    /**
     * Commits the transaction given by the context argument.
     * 
     * @param context
     *            the {@link DtxResourceManagerContext} associated with the transaction
     * @throws XAException
     *             if committing the transaction in this context failed, with the following error return codes:
     *             <ul>
     *             <li>{@link XAException#XA_RETRY} if the commit cannot be completed at this time, but may be retried
     *             later</li>
     *             <li>{@link XAException#XAER_PROTO} if the given context is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             <li>{@link XAException#XA_RBROLLBACK} transaction must be rolled back for an unspecified reason</li>
     *             <li>{@link XAException#XA_RBDEADLOCK} transaction must be rolled back due to a deadlock</li>
     *             <li>{@link XAException#XA_RBINTEGRITY} transaction must be rolled back as it violates resource
     *             integrity</li>
     *             <li>{@link XAException#XA_RBPROTO} transaction must be rolled back following an internal protocol
     *             error</li>
     *             </ul>
     */
    void commit(DtxResourceManagerContext context) throws XAException;

    /**
     * Rolls back the transaction given by the context argument.
     * 
     * As this may be called to resolve a deadlock, its execution must not block indefinitely.
     * 
     * @param context
     *            the {@link DtxResourceManagerContext} associated with the transaction
     * @throws XAException
     *             if rolling back the transaction failed, with the following error return codes:
     *             <ul>
     *             <li>{@link XAException#XAER_PROTO} if the given context is not in a valid state</li>
     *             <li>{@link XAException#XAER_INVAL} if the context is invalid</li>
     *             <li>{@link XAException#XAER_RMFAIL} if the resource manager is unavailable</li>
     *             <li>{@link XAException#XAER_RMERR} if an internal error occurred</li>
     *             </ul>
     */
    void rollback(DtxResourceManagerContext context) throws XAException;

    /**
     * Create the task information.
     * 
     * @param payload
     *            the payload associated with the transaction
     * 
     * @return a {@link DtxTaskInfo} identifying the information of the task, null if the payload can not be parsed
     */
    DtxTaskInfo createTaskInfo(final byte[] payload);

    /**
     * Executes optional post-synchronization processing.
     * 
     * This method is called when entering the
     * {@link io.eguan.dtx.DtxResourceManagerState#POST_SYNC_PROCESSING POST_SYNC_PROCESSING} state, i.e.
     * between the {@link io.eguan.dtx.DtxResourceManagerState#SYNCHRONIZING SYNCHRONIZING} and
     * {@link io.eguan.dtx.DtxResourceManagerState#UP_TO_DATE UP_TO_DATE} states. Throwing any exception is
     * considered a temporary failure and thus the post-synchronization phase is considered incomplete and may be
     * repeated.
     * 
     * @throws Exception
     *             if post-synchronization processing fails and should be re-attempted
     */
    void processPostSync() throws Exception;

}
