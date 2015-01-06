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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;
import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.dtx.DtxTaskApiAbstract.TaskKeeperParameters;
import com.oodrive.nuage.dtx.config.DtxJournalFileDirConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperAbsoluteDurationConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperAbsoluteSizeConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperMaxDurationConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperMaxSizeConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperPurgeDelayConfigKey;
import com.oodrive.nuage.dtx.config.DtxTaskKeeperPurgePeriodConfigKey;
import com.oodrive.nuage.dtx.config.DtxTransactionTimeoutConfigKey;

/**
 * Configuration of the {@link DtxManager}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
@Immutable
public final class DtxManagerConfig {

    /**
     * Name of the cluster.
     */
    private final String clusterName;

    /**
     * Password of the cluster in order to join it.
     */
    private final String clusterPassword;

    /**
     * The local peer address.
     */
    private final DtxNode localPeer;

    /**
     * The transaction timeout in milliseconds.
     */
    private final long txTimeout;

    /**
     * List of peers, the local peer excluded.
     */
    private final List<DtxNode> peers;

    private final Path journalDir;

    private final TaskKeeperParameters parameters;

    /**
     * Constructs an immutable configuration to initialize a {@link DtxManager} with.
     * 
     * @param metaConfiguration
     *            a non-<code>null</code> {@link MetaConfiguration} with all required parameters of the
     *            {@link com.oodrive.nuage.dtx.DtxConfigurationContext}
     * @param baseDir
     *            {@link Path} to the base directory within which DTX data is to be stored
     * @param clusterName
     *            the unique cluster name
     * @param clusterPassword
     *            the shared secret to join the cluster
     * @param localNode
     *            the {@link DtxNode} considered to be the local node
     * @param peers
     *            a list of peers to connect to (the others)
     * @throws NullPointerException
     *             if any of the parameters is <code>null</code>
     */
    @ParametersAreNonnullByDefault
    public DtxManagerConfig(final MetaConfiguration metaConfiguration, final Path baseDir, final String clusterName,
            final String clusterPassword, final DtxNode localNode, final DtxNode... peers) throws NullPointerException {
        super();
        this.clusterName = Objects.requireNonNull(clusterName);
        this.clusterPassword = Objects.requireNonNull(clusterPassword);
        this.localPeer = Objects.requireNonNull(localNode);

        this.peers = Collections.unmodifiableList(Arrays.asList(Objects.requireNonNull(peers)));

        this.txTimeout = DtxTransactionTimeoutConfigKey.getInstance().getTypedValue(metaConfiguration).longValue();
        this.journalDir = FileSystems.getDefault().getPath(baseDir.toAbsolutePath().toString(),
                DtxJournalFileDirConfigKey.getInstance().getTypedValue(metaConfiguration).toString());
        this.parameters = new TaskKeeperParameters(DtxTaskKeeperAbsoluteDurationConfigKey.getInstance()
                .getTypedValue(metaConfiguration).longValue(), DtxTaskKeeperAbsoluteSizeConfigKey.getInstance()
                .getTypedValue(metaConfiguration).intValue(), DtxTaskKeeperMaxDurationConfigKey.getInstance()
                .getTypedValue(metaConfiguration).longValue(), DtxTaskKeeperMaxSizeConfigKey.getInstance()
                .getTypedValue(metaConfiguration).intValue(), DtxTaskKeeperPurgePeriodConfigKey.getInstance()
                .getTypedValue(metaConfiguration).longValue(), DtxTaskKeeperPurgeDelayConfigKey.getInstance()
                .getTypedValue(metaConfiguration).longValue());
    }

    /**
     * Gets the configured cluster name.
     * 
     * @return the non-<code>null</code> cluster name
     */
    @Nonnull
    public final String getClusterName() {
        return clusterName;
    }

    /**
     * Gets the shared secret used for joining the cluster.
     * 
     * @return the non-<code>null</code> cluster access password
     */
    @Nonnull
    public final String getClusterPassword() {
        return clusterPassword;
    }

    /**
     * Gets the distinct node considered to be the local node.
     * 
     * @return a valid, non-<code>null</code> {@link DtxNode}
     */
    @Nonnull
    public final DtxNode getLocalPeer() {
        return localPeer;
    }

    /**
     * Gets the defined transaction timeout.
     * 
     * @return the transaction timeout in milliseconds
     */
    public final long getTransactionTimeout() {
        return txTimeout;
    }

    /**
     * Gets the journal directory.
     * 
     * @return the directory {@link Path} where journal files are to be stored
     */
    @Nonnull
    public final Path getJournalDirectory() {
        return journalDir;
    }

    /**
     * Gets the parameters for the task Keeper.
     * 
     * @return the {@link TaskKeeperParameters} where parameters are stored.
     */
    @Nonnull
    public TaskKeeperParameters getParameters() {
        return parameters;
    }

    /**
     * Gets the list of all cluster peers registered with this configuration.
     * 
     * @return a (possibly empty) {@link List} of {@link DtxNode}s
     */
    @Nonnull
    public final List<DtxNode> getPeers() {
        return peers;
    }

    @Override
    public final String toString() {
        return MoreObjects.toStringHelper(this).add("clusterName", clusterName)
                .add("clusterPassword", clusterPassword).add("localPeer", localPeer).add("peers", peers)
                .add("journalDir", journalDir).add("taskKeeperParameters", parameters).toString();
    }

}
