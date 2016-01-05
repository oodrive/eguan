package io.eguan.vvr.configuration;

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

import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.AbstractConfigurationContext;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.ValidationError.ErrorType;
import io.eguan.vvr.configuration.keys.IbsAutoConfRamSize;
import io.eguan.vvr.configuration.keys.IbsBufferRotationDelay;
import io.eguan.vvr.configuration.keys.IbsBufferRotationThreshold;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayIncrement;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayLevelSize;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayThreshold;
import io.eguan.vvr.configuration.keys.IbsCompressionConfigKey;
import io.eguan.vvr.configuration.keys.IbsConfigKey;
import io.eguan.vvr.configuration.keys.IbsDisableBackgroundCompactionForIbpgenConfigKey;
import io.eguan.vvr.configuration.keys.IbsDumpAtStopBestEffortDelayConfigKey;
import io.eguan.vvr.configuration.keys.IbsHotDataConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsLdbBlockRestartIntervalConfigKey;
import io.eguan.vvr.configuration.keys.IbsLdbBlockSizeConfigKey;
import io.eguan.vvr.configuration.keys.IbsLogLevelConfigKey;
import io.eguan.vvr.configuration.keys.IbsOwnerUuidConfigKey;
import io.eguan.vvr.configuration.keys.IbsRecordExecutionConfigKey;
import io.eguan.vvr.configuration.keys.IbsSyslogConfigKey;
import io.eguan.vvr.configuration.keys.IbsUuidConfigKey;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Context for configuration keys specific to the IBS subsystem.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * @author jmcaba
 * @author llambert
 * 
 */
public final class IbsConfigurationContext extends AbstractConfigurationContext {

    private static final String NAME = "io.eguan.vvr.ibs";

    private static final IbsConfigurationContext INSTANCE = new IbsConfigurationContext();

    private static class ChildDirectoryCollector extends SimpleFileVisitor<Path> {
        private Collection<File> childCollection;

        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
            final FileVisitResult result = super.preVisitDirectory(dir, attrs);
            this.childCollection.add(dir.toFile());
            return result;
        }

    }

    public static IbsConfigurationContext getInstance() {
        return INSTANCE;
    }

    private IbsConfigurationContext() throws IllegalArgumentException, NullPointerException {
        super(NAME, new AbstractConfigKey[] { IbsIbpPathConfigKey.getInstance(), IbsIbpGenPathConfigKey.getInstance(),
                IbsHotDataConfigKey.getInstance(), IbsCompressionConfigKey.getInstance(),
                IbsUuidConfigKey.getInstance(), IbsOwnerUuidConfigKey.getInstance(),
                IbsLogLevelConfigKey.getInstance(), IbsLdbBlockSizeConfigKey.getInstance(),
                IbsLdbBlockRestartIntervalConfigKey.getInstance(), IbsBufferRotationThreshold.getInstance(),
                IbsBufferRotationDelay.getInstance(), IbsBufferWriteDelayThreshold.getInstance(),
                IbsBufferWriteDelayLevelSize.getInstance(), IbsBufferWriteDelayIncrement.getInstance(),
                IbsRecordExecutionConfigKey.getInstance(), IbsDumpAtStopBestEffortDelayConfigKey.getInstance(),
                IbsDisableBackgroundCompactionForIbpgenConfigKey.getInstance(), IbsAutoConfRamSize.getInstance(),
                IbsSyslogConfigKey.getInstance() });
    }

    @Override
    public final List<ValidationError> validateConfiguration(final MetaConfiguration configuration) {
        final List<ValidationError> result = super.validateConfiguration(configuration);

        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(configuration);
        final File ibpGenPath = IbsIbpGenPathConfigKey.getInstance().getTypedValue(configuration);

        if (ibpPaths == null) {
            return result;
        }
        result.addAll(checkForPathCollisions(ibpPaths));

        if (ibpGenPath == null) {
            return result;
        }
        result.addAll(checkForPathCollisions(ibpGenPath, ibpPaths));

        return result;
    }

    /**
     * Stores the values belonging to the native IBS configuration to the given {@link OutputStream}.
     * 
     * @param configuration
     *            the {@link MetaConfiguration} from which to read the values
     * @param outputStream
     *            the {@link OutputStream} to which to write
     * @throws IOException
     *             if storing to the {@link OutputStream} fails
     * @throws IllegalStateException
     *             if the {@link MetaConfiguration} was not created with {@link IbsConfigurationContext}
     * @throws NullPointerException
     *             if either argument is <code>null</code>
     */
    public final void storeIbsConfig(@Nonnull final MetaConfiguration configuration,
            @Nonnull final OutputStream outputStream) throws IOException, IllegalStateException, NullPointerException {
        Objects.requireNonNull(configuration);
        Objects.requireNonNull(outputStream);

        final Properties outputProps = new Properties();
        for (final AbstractConfigKey currKey : getConfigKeys()) {
            // TODO: add a key that's not an IbsConfigKey to this context or
            // remove this
            if (!(currKey instanceof IbsConfigKey)) {
                continue;
            }
            final IbsConfigKey currIbsKey = (IbsConfigKey) currKey;
            outputProps.setProperty(currIbsKey.getBackendConfigKey(), currIbsKey.getBackendConfigValue(configuration));
        }
        outputProps.store(outputStream, "Automatically generated IBS configuration file, do not modify");
    }

    /**
     * Checks for path collisions (i.e. identical or subpaths of each other) for all elements of the given list.
     * 
     * This method does not verify if the given {@link File} objects point to directories or files.
     * 
     * @param dirList
     *            a {@link List} of {@link File}s to check
     * @return a list of {@link ValidationError} to include in a {@link ConfigValidationException}
     */
    private static List<ValidationError> checkForPathCollisions(final List<File> dirList) {
        final ArrayList<ValidationError> report = new ArrayList<ValidationError>();
        final int size = dirList.size();

        for (final ListIterator<File> iter = dirList.listIterator(); iter.hasNext();) {
            final File currFile = iter.next();
            report.addAll(checkForPathCollisions(currFile, dirList.subList(iter.nextIndex(), size)));
        }
        return report;
    }

    /**
     * Checks if any of the given list's directories are identical or children or parents of the first directory.
     * 
     * All files passed to this method must exist and be directories.
     * 
     * @param file
     *            the {@link File directory} to compare
     * @param fileList
     *            the list of {@link File directories} to check for children or parents of the first file
     * @return a list of {@link ValidationError} to include in a {@link ConfigValidationException}
     */
    private static List<ValidationError> checkForPathCollisions(final File file, final List<File> fileList) {
        final ArrayList<ValidationError> report = new ArrayList<ValidationError>();

        final ChildDirectoryCollector collector = new ChildDirectoryCollector();
        collector.childCollection = new HashSet<File>();
        collector.childCollection.add(file);

        try {
            Files.walkFileTree(file.toPath(), collector);
        }
        catch (final IOException e) {
            report.add(new ValidationError(ErrorType.VALUE_INVALID, null, IbsIbpGenPathConfigKey.getInstance(),
                    fileList, "exception while getting children of " + file + ": " + e.getMessage()));
        }

        if (!Collections.disjoint(collector.childCollection, fileList)) {
            report.add(new ValidationError(ErrorType.VALUE_INVALID, null, IbsIbpGenPathConfigKey.getInstance(),
                    fileList, "common directories found between the children of " + file + ": "
                            + collector.childCollection + " and the following directories:" + fileList));
        }

        File parent = file.getParentFile();
        final HashSet<File> parentSet = new HashSet<File>();
        while (parent != null) {
            parentSet.add(parent);
            parent = parent.getParentFile();
        }

        if (!Collections.disjoint(parentSet, fileList)) {
            report.add(new ValidationError(ErrorType.VALUE_INVALID, null, IbsIbpGenPathConfigKey.getInstance(),
                    fileList, "Common directories found between the parents of " + file + ": " + parentSet
                            + " and the following directories:" + fileList));
        }

        return report;
    }

}
