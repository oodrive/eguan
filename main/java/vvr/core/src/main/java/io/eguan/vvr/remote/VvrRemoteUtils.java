package io.eguan.vvr.remote;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.DtxTaskApi;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.net.MsgServerRemoteStatus;
import io.eguan.net.MsgServerTimeoutException;
import io.eguan.proto.Common.OpCode;
import io.eguan.proto.Common.ProtocolVersion;
import io.eguan.proto.Common.Type;
import io.eguan.proto.Common.Uuid;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.UuidT;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utilities around {@link VvrRemote}.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
public final class VvrRemoteUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(VvrRemoteUtils.class.getSimpleName());

    /**
     * No instance.
     */
    private VvrRemoteUtils() {
        throw new AssertionError();
    }

    /**
     * {@link Uuid} to {@link UUID}.
     * 
     * @param uuid
     * @return {@link UUID} corresponding to <code>uuid</code>
     */
    public static final UUID fromUuid(@Nonnull final Uuid uuid) {
        return new UUID(uuid.getMsb(), uuid.getLsb());
    }

    /**
     * {@link Uuid} to {@link UuidT}.
     * 
     * @param uuid
     * @return {@link UuidT} corresponding to <code>uuid</code>
     */
    public static final <F> UuidT<F> fromUuidT(@Nonnull final Uuid uuid) {
        return new UuidT<>(uuid.getMsb(), uuid.getLsb());
    }

    /**
     * {@link UUID} to {@link Uuid}.
     * 
     * @param uuid
     * @return {@link Uuid} corresponding to <code>uuid</code>
     */
    public static final Uuid newUuid(@Nonnull final UUID uuid) {
        return Uuid.newBuilder().setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits()).build();
    }

    /**
     * {@link UuidT} to {@link Uuid}.
     * 
     * @param uuid
     * @return {@link Uuid} corresponding to <code>uuid</code>
     */
    public static final <F> Uuid newTUuid(@Nonnull final UuidT<F> uuid) {
        return Uuid.newBuilder().setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits()).build();
    }

    /**
     * Tells if the given {@link Uuid}s are equals.
     * 
     * @param uuid1
     *            a valid Uuid or <code>null</code>
     * @param uuid2
     *            a valid Uuid or <code>null</code>
     * @return <code>true</code> if the <code>uuid1</code> and <code>uuid2</code> are equals or both <code>null</code>
     */
    public static final boolean equalsUuid(final Uuid uuid1, final Uuid uuid2) {
        // true if both are null
        if (uuid1 == uuid2) {
            return true;
        }
        if (uuid1 == null || uuid2 == null) {
            return false;
        }
        // Nor uuid1 nor uuid2 is null
        return uuid1.getMsb() == uuid2.getMsb() && uuid1.getLsb() == uuid2.getLsb();
    }

    /**
     * Sends a {@link VvrRemote} message to the given destinations. The source, type and opCode are filled. Nothing is
     * sent if <code>startpoint</code> or <code>source</code> is <code>null</code>.
     * 
     * @param opBuilder
     *            message builder
     * @param startpoint
     *            message destination
     * @param source
     *            message source
     * @param type
     *            type of object
     * @param opCode
     *            operation on object
     * @return status of sync request or <code>null</code> for async messages or stand alone mode
     * @throws InterruptedException
     * @throws MsgServerTimeoutException
     * @throws ConnectException
     *             if peer != null
     */
    public static final Collection<MsgServerRemoteStatus> sendMessage(final RemoteOperation.Builder opBuilder,
            final MsgClientStartpoint startpoint, final Uuid source, final Type type, final OpCode opCode,
            final boolean async, final UUID peer) throws MsgServerTimeoutException, InterruptedException,
            ConnectException {
        // Stand alone?
        if (startpoint == null) {
            return null;
        }

        // Source is immutable, no need to make a defensive copy
        opBuilder.setVersion(ProtocolVersion.VERSION_1);
        opBuilder.setSource(Objects.requireNonNull(source));
        opBuilder.setType(type);
        opBuilder.setOp(opCode);
        final RemoteOperation operation = opBuilder.build();
        if (async) {
            try {
                startpoint.sendAsyncMessage(operation);
            }
            catch (final Exception e) {
                LOGGER.warn(
                        "Failed to send message type=" + type + ", uuid="
                                + (operation == null ? "?" : fromUuid(operation.getUuid()).toString()) + ", op="
                                + opCode, e);
            }
            return null;
        }
        else {
            if (peer == null) {
                return startpoint.sendSyncMessage(operation);
            }
            else {
                final MsgServerRemoteStatus status = startpoint.sendSyncMessageNewChannel(peer, operation);
                final Collection<MsgServerRemoteStatus> result = new ArrayList<>(1);
                result.add(status);
                return result;
            }
        }
    }

    /**
     * Submit a {@link VvrRemote} message to the given destinations as a transaction. The source, type and opCode are
     * filled.
     * 
     * @param opBuilder
     *            message builder
     * @param dtxTaskApi
     *            dtx manager
     * @param resourceId
     *            id of resource manager
     * @param source
     *            message source
     * @param type
     *            type of object
     * @param opCode
     *            operation on object
     * @return the task ID or null if on mode stand alone
     */
    public static final UUID submitTransaction(final RemoteOperation.Builder opBuilder, final DtxTaskApi dtxTaskApi,
            final UUID resourceId, final Uuid source, final Type type, final OpCode opCode) {

        // Fill builder
        // source is immutable, no need to make a defensive copy
        opBuilder.setVersion(ProtocolVersion.VERSION_1);
        opBuilder.setSource(source);
        opBuilder.setType(type);
        opBuilder.setOp(opCode);

        // Submit transaction in mode distributed
        RemoteOperation operation = null;
        try {
            operation = opBuilder.build();
            final byte[] payload = operation.toByteArray();
            return dtxTaskApi.submit(resourceId, payload);
        }
        catch (final Error | IllegalStateException e) {
            LOGGER.warn("Failed to submit transaction resourceId=" + resourceId + ", type=" + type + ", uuid="
                    + (operation == null ? "?" : fromUuid(operation.getUuid()).toString()) + ", op=" + opCode, e);
            throw e;
        }
        catch (final Exception e) {
            LOGGER.warn("Failed to submit transaction resourceId=" + resourceId + ", type=" + type + ", uuid="
                    + (operation == null ? "?" : fromUuid(operation.getUuid()).toString()) + ", op=" + opCode, e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Create dtx context containing the deserialized payload.
     */
    public static final DtxResourceManagerContext createDtxContext(final UUID resourceManagerId, final byte[] payload)
            throws InvalidProtocolBufferException {
        final RemoteOperation operation = RemoteOperation.parseFrom(payload);
        return new VvrDtxRmContext(resourceManagerId, operation);
    }
}
