package com.oodrive.nuage.vold;

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

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;

import org.slf4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.oodrive.nuage.dtx.DtxResourceManager;
import com.oodrive.nuage.dtx.DtxResourceManagerContext;
import com.oodrive.nuage.dtx.DtxTaskInfo;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.vold.model.Constants;
import com.oodrive.nuage.vold.model.VvrManager;

/**
 * Redirect DTX messages to {@link Vold} and/or {@link VvrManager}.
 * 
 * @author oodrive
 * @author jmcaba
 */
public final class VoldDtxResourceManager implements DtxResourceManager {
    private static final Logger LOGGER = Constants.LOGGER;

    /** Owner of the Vold */
    private final UUID owner;
    /** Associated vold */
    private final Vold vold;
    /** Associated vvr manager */
    private final VvrManager vvrManager;

    /**
     * @param owner
     * @param vvrManager
     */
    VoldDtxResourceManager(@Nonnull final UUID owner, @Nonnull final Vold vold, @Nonnull final VvrManager vvrManager) {
        super();
        this.owner = Objects.requireNonNull(owner, "owner must not be null !");
        this.vold = Objects.requireNonNull(vold, "vold must not be null !");
        this.vvrManager = Objects.requireNonNull(vvrManager, "vvrManager must not be null !");
    }

    @Override
    public final UUID getId() {
        return owner;
    }

    @Override
    public final DtxResourceManagerContext start(final byte[] payload) throws XAException, NullPointerException {
        try {
            final RemoteOperation operation = RemoteOperation.parseFrom(payload);
            return new VoldDtxRmContext(owner, operation);
        }
        catch (final InvalidProtocolBufferException e) {
            LOGGER.warn("Could not start VoldDtxResourceManager !", e);
            throw new XAException(XAException.XAER_INVAL);
        }
    }

    @Override
    public final Boolean prepare(final DtxResourceManagerContext context) throws XAException {
        final VoldDtxRmContext vvrDtxRmContext = (VoldDtxRmContext) context;
        final Type type = vvrDtxRmContext.getOperation().getType();
        if (type == Type.VOLD) {
            return vold.prepare(vvrDtxRmContext);
        }
        else {
            return vvrManager.prepare(vvrDtxRmContext);
        }
    }

    @Override
    public final void commit(final DtxResourceManagerContext context) throws XAException {
        final VoldDtxRmContext vvrDtxRmContext = (VoldDtxRmContext) context;
        final Type type = vvrDtxRmContext.getOperation().getType();
        if (type == Type.VOLD) {
            vold.commit(vvrDtxRmContext);
        }
        else {
            vvrManager.commit(vvrDtxRmContext);
        }
    }

    @Override
    public final void rollback(final DtxResourceManagerContext context) throws XAException {
        final VoldDtxRmContext vvrDtxRmContext = (VoldDtxRmContext) context;
        final Type type = vvrDtxRmContext.getOperation().getType();
        if (type == Type.VOLD) {
            vold.rollback(vvrDtxRmContext);
        }
        else {
            vvrManager.rollback(vvrDtxRmContext);
        }
    }

    @Override
    public final void processPostSync() {
        vold.processPostSync();
        vvrManager.processPostSync();
    }

    @Override
    public final DtxTaskInfo createTaskInfo(final byte[] payload) {
        final RemoteOperation operation;
        try {
            operation = RemoteOperation.parseFrom(payload);
        }
        catch (final InvalidProtocolBufferException e) {
            return null;
        }
        final Type type = operation.getType();
        if (type == Type.VOLD) {
            return vold.createTaskInfo(operation);
        }
        else {
            return vvrManager.createTaskInfo(operation);
        }
    }

}
