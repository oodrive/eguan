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

import java.beans.ConstructorProperties;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Immutable representation of a resource manager. This version is exported as a MXBean attribute.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@Immutable
public final class DtxResourceManagerAdm {

    public static enum DtxJournalStatus {
        STOPPED, STARTED
    }

    private final String uuid;
    private final DtxResourceManagerState status;
    private final long lastTransaction;
    private final String journalPath;
    private final DtxJournalStatus journalStatus;

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this resource manager
     * @param name
     *            the current status of the resource manager
     * @param lastTransaction
     *            the last transaction executed
     * @param journalPath
     *            the optional journal path. May be <code>null</code>
     * @throws journalStatus
     *             the status of the journal
     */
    DtxResourceManagerAdm(@Nonnull final UUID uuid, @Nonnull final DtxResourceManagerState status,
            final long lastTransaction, final String journalPath, final boolean journalStatus) {
        this(uuid.toString(), status, lastTransaction, journalPath, journalStatus == true ? DtxJournalStatus.STARTED
                : DtxJournalStatus.STOPPED);
    }

    /**
     * Constructs an immutable instance.
     * 
     * @param uuid
     *            the globally unique ID of this resource manager
     * @param name
     *            the current status of the resource manager
     * @param lastTransaction
     *            the last transaction executed
     * @param journalPath
     *            the optional journal path. May be <code>null</code>
     * @throws journalStatus
     *             the status of the journal
     */
    @ConstructorProperties({ "uuid", "status", "lastTransaction", "journalPath", "journalStatus" })
    public DtxResourceManagerAdm(@Nonnull final String uuid, @Nonnull final DtxResourceManagerState status,
            final long lastTransaction, final String journalPath, final DtxJournalStatus journalStatus) {
        super();
        this.uuid = Objects.requireNonNull(uuid);
        this.status = Objects.requireNonNull(status);
        this.lastTransaction = lastTransaction;
        this.journalPath = journalPath == null ? "" : journalPath;
        this.journalStatus = journalStatus;
    }

    /**
     * Gets the uuid of the resource manager. May not be null.
     * 
     * @return the uuid of the resource manager
     */
    public final String getUuid() {
        return uuid;
    }

    /**
     * Gets the status of the resource manager. May not be null.
     * 
     * @return the status of the resource manager
     */
    public final DtxResourceManagerState getStatus() {
        return status;
    }

    /**
     * Gets the last transaction executed by the resource manager. May not be -1.
     * 
     * @return the last transaction ID.
     */
    public final long getLastTransaction() {
        return lastTransaction;
    }

    /**
     * Gets the journal path of the resource manager. May be empty.
     * 
     * @return the journal path
     */
    public final String getJournalPath() {
        return journalPath;
    }

    /**
     * Gets the journal status of the resource manager.
     * 
     * @return the journal path
     */
    public final DtxJournalStatus getJournalStatus() {
        return journalStatus;
    }
}
