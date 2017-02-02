package io.eguan.vvr.persistence.repository;

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

import io.eguan.nrs.NrsFile;
import io.eguan.nrs.NrsFileFlag;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.nrs.NrsFileJanitor;
import io.eguan.proto.nrs.NrsRemote.NrsFileHeaderMsg;
import io.eguan.proto.nrs.NrsRemote.NrsFileHeaderMsg.Flags;
import io.eguan.proto.vvr.VvrRemote;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.utils.UuidT;
import io.eguan.vvr.remote.VvrRemoteUtils;
import io.eguan.vvr.repository.core.api.Device;

import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Utility for remote messages.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * @author ebredzinski
 * 
 */
final class NrsRemoteUtils {

    /**
     * No instance.
     */
    private NrsRemoteUtils() {
        throw new AssertionError();
    }

    /**
     * Fill the {@link NrsFileHeaderMsg} in the message to build.
     * 
     * @param opBuilder
     * @param fileHeader
     */
    public static final void addNrsFileHeaderMsg(final RemoteOperation.Builder opBuilder,
            final NrsFileHeader<NrsFile> fileHeader) {
        final NrsFileHeaderMsg.Builder builder = NrsFileHeaderMsg.newBuilder();
        builder.setParent(VvrRemoteUtils.newTUuid(fileHeader.getParentId()));
        builder.setDevice(VvrRemoteUtils.newUuid(fileHeader.getDeviceId()));
        builder.setNode(VvrRemoteUtils.newUuid(fileHeader.getNodeId()));
        builder.setId(VvrRemoteUtils.newTUuid(fileHeader.getFileId()));
        builder.setSize(fileHeader.getSize());
        builder.setBlockSize(fileHeader.getBlockSize());
        builder.setClusterSize(fileHeader.getClusterSize());
        builder.setHashSize(fileHeader.getHashSize());
        builder.setTimestamp(fileHeader.getTimestamp());

        // Add flags
        if (fileHeader.isRoot()) {
            builder.addFlags(Flags.ROOT);
        }
        if (fileHeader.isPartial()) {
            builder.addFlags(Flags.PARTIAL);
        }
        if (fileHeader.isBlocks()) {
            builder.addFlags(Flags.BLOCKS);
        }

        // Set value
        opBuilder.addNrsFileHeader(builder);
    }

    /**
     * Create a {@link NrsFileHeader} from a {@link NrsFileHeaderMsg}
     * 
     * @param nrsFileJanitor
     *            local {@link NrsFile} janitor.
     * @param msg
     * @param newParentUuid
     *            if not <code>null</code>, the parent {@link UUID} to take instead of the one contained in the message.
     * @return a new {@link NrsFileHeader} corresponding to <code>msg</code>
     */
    public static final NrsFileHeader<NrsFile> fromNrsFileHeaderMsg(final NrsFileJanitor nrsFileJanitor,
            final NrsFileHeaderMsg msg, final UuidT<NrsFile> newParentUuid) {
        final NrsFileHeader.Builder<NrsFile> builder = nrsFileJanitor.newNrsFileHeaderBuilder();

        final UuidT<NrsFile> parent;
        if (newParentUuid == null) {
            parent = VvrRemoteUtils.fromUuidT(msg.getParent());
        }
        else {
            parent = newParentUuid;
        }

        builder.parent(parent);
        builder.device(VvrRemoteUtils.fromUuid(msg.getDevice()));
        builder.node(VvrRemoteUtils.fromUuid(msg.getNode()));
        final UuidT<NrsFile> file = VvrRemoteUtils.fromUuidT(msg.getId());
        builder.file(file);
        builder.size(msg.getSize());
        builder.blockSize(msg.getBlockSize());
        builder.hashSize(msg.getHashSize());
        builder.timestamp(msg.getTimestamp());

        final List<Flags> flags = msg.getFlagsList();
        for (final Flags flag : flags) {
            if (flag == Flags.ROOT) {
                builder.addFlags(NrsFileFlag.ROOT);
            }
            else if (flag == Flags.PARTIAL) {
                builder.addFlags(NrsFileFlag.PARTIAL);
            }
            else if (flag == Flags.BLOCKS) {
                builder.addFlags(NrsFileFlag.BLOCKS);
            }
            else {
                throw new AssertionError("flag=" + flag);
            }
        }

        return builder.build();
    }

    /**
     * Set fields specific to a {@link Device}.
     * 
     * @param opBuilder
     * @param device
     */
    public static final void setNrsDeviceNrsHeader(final RemoteOperation.Builder opBuilder, final NrsDevice device,
            @Nonnull final NrsFileHeader<NrsFile> newDeviceNrsFileHeader) {

        opBuilder.setSnapshot(VvrRemoteUtils.newUuid(device.getParent()));
        opBuilder.setUuid(VvrRemoteUtils.newUuid(device.getUuid()));

        // Set NRS header
        addNrsFileHeaderMsg(opBuilder, newDeviceNrsFileHeader);
    }

    /**
     * Set fields specific to a {@link Device} from its header.
     * 
     * @param opBuilder
     * @param fileHeader
     * @param snapshot
     *            parent snapshot of the device or {@link UUID} of the snapshot to create.
     * @param name
     * @param description
     */
    public static final void addNrsDevice(final RemoteOperation.Builder opBuilder,
            final NrsFileHeader<NrsFile> fileHeader, final UUID snapshot, final String name, final String description) {

        opBuilder.setSnapshot(VvrRemoteUtils.newUuid(snapshot));
        opBuilder.setUuid(VvrRemoteUtils.newUuid(fileHeader.getDeviceId()));

        // Create item if needed
        if (name != null || description != null) {
            final VvrRemote.Item.Builder itemBuilder = VvrRemote.Item.newBuilder();
            if (name != null) {
                itemBuilder.setName(name);
            }
            if (description != null) {
                itemBuilder.setDescription(description);
            }
            opBuilder.setItem(itemBuilder.build());
        }

        // Set NRS header
        addNrsFileHeaderMsg(opBuilder, fileHeader);
    }

}
