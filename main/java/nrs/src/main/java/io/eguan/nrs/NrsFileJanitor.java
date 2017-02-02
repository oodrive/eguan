package io.eguan.nrs;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.net.MsgClientStartpoint;
import io.eguan.proto.nrs.NrsRemote.NrsVersion;
import io.eguan.utils.UuidCharSequence;
import io.eguan.utils.UuidT;
import io.eguan.utils.Files.OpenedFileHandler;
import io.eguan.utils.mapper.FileMapper;
import io.eguan.utils.mapper.FileMapperConfigKey;
import io.eguan.utils.mapper.FileMapper.Type;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the {@link NrsFile}s. Handles the creation, the deletion, the opening and the closing of these
 * files. Each {@link NrsFile} must have at most one instance at a time to handle read/write access to its contents.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public final class NrsFileJanitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(NrsFileJanitor.class);

    /** Constant string key to flag a {@link NrsFile} as 'sealed'. */
    private static final String ATTR_SEALED = "NrsJanitor.sealed";

    /** The {@link FileMapper} used for {@link NrsFile}s. */
    private final FileMapper imagesFileMapper;
    /** The {@link FileMapper} used for {@link NrsFileBlock}s. */
    private final FileMapper blocksFileMapper;

    /** target directory for NRS file storage. */
    private final File directory;

    /** The limit blocking space left percentage in percent. */
    private final int limitPercentage;

    /** The cluster size in bytes. */
    private final int clusterSize;

    /** Directory to store the {@link NrsFile}s */
    private final File imagesDirectory;

    /** Handler of opened NrsFiles */
    private OpenedFileHandler<NrsFile, UuidT<NrsFile>> openedFileHandler;

    /** Optional client start point for remote notification */
    private final AtomicReference<NrsMsgPostOffice> postOfficeRef = new AtomicReference<>();

    public NrsFileJanitor(@Nonnull final MetaConfiguration configuration) {
        LOGGER.trace(String.format("Constructing a new instance of %s", NrsFileJanitor.class.getSimpleName()));

        this.clusterSize = NrsClusterSizeConfigKey.getInstance().getTypedValue(Objects.requireNonNull(configuration))
                .intValue();

        this.directory = NrsStorageConfigKey.getInstance().getTypedValue(configuration);

        // Images
        {
            final File imagesDir = ImagesFileDirectoryConfigKey.getInstance().getTypedValue(configuration);
            this.imagesDirectory = new File(this.directory, imagesDir.getPath());
            if (!imagesDirectory.exists() && !imagesDirectory.mkdirs()) {
                throw new IllegalStateException("Failed to create image storage directory '" + imagesDirectory + "'");
            }
            this.imagesFileMapper = getFileMapperFromConfig(configuration, imagesDirectory);
        }

        // Blocks
        {
            final File blocksDir = BlkCacheDirectoryConfigKey.getInstance().getTypedValue(configuration);
            final File blocksDirectory = new File(this.directory, blocksDir.getPath());
            if (!blocksDirectory.exists() && !blocksDirectory.mkdirs()) {
                throw new IllegalStateException("Failed to create block cache storage directory '" + blocksDirectory
                        + "'");
            }
            this.blocksFileMapper = getFileMapperFromConfig(configuration, blocksDirectory);
        }

        this.limitPercentage = RemainingSpaceCreateLimitConfigKey.getInstance().getTypedValue(configuration).intValue();
    }

    /**
     * Initialize the handling of {@link NrsFile}s.
     */
    public final void init() {
        // Opened files
        this.openedFileHandler = io.eguan.utils.Files.newOpenedFileHandler();
    }

    /**
     * Release internal resources.
     */
    public final void fini() {
        // Close opened files
        try {
            openedFileHandler.cancel();
        }
        catch (final Throwable t) {
            LOGGER.warn("Error while cancelling task", t);
        }
        try {
            openedFileHandler.closeAll();
        }
        catch (final Throwable t) {
            LOGGER.warn("Error while closing NrsFiles", t);
        }
        openedFileHandler = null;

        // Flush the optional post office
        flushPostOffice();
    }

    /**
     * Sets the {@link MsgClientStartpoint} for the remote update and synchronization of {@link NrsFile}.
     * <p>
     * Note: the {@link MsgClientStartpoint} is set when a file is opened. The caller should close the {@link NrsFile}s
     * before setting the <code>clientStartpoint</code>.
     * 
     * @param startpoint
     *            the {@link MsgClientStartpoint} for remote update or <code>null</code> to disable remote update.
     * @param enhancer
     *            message enhancer, may be <code>null</code>
     */
    public final void setClientStartpoint(final MsgClientStartpoint startpoint, final NrsMsgEnhancer enhancer) {
        // Flush previous instance (if any)
        flushPostOffice();

        this.postOfficeRef.set(startpoint == null ? null : new NrsMsgPostOffice(startpoint, enhancer));
    }

    private final void flushPostOffice() {
        final NrsMsgPostOffice postOffice = postOfficeRef.get();
        if (postOffice != null) {
            postOffice.flush();
        }
    }

    /**
     * Send now the pending messages for a file.
     * 
     * @param nrsFileUuid
     *            UUID of the {@link NrsFile}.
     */
    private final void flushNrsFileMessages(final UuidT<NrsFile> nrsFileUuid) {
        final NrsMsgPostOffice postOffice = postOfficeRef.get();
        if (postOffice != null) {
            postOffice.flush(nrsFileUuid);
        }
    }

    /**
     * Clear {@link NrsFile} cache. For unit test purpose only, to actually create new instances for files. Fails if
     * some files are opened.
     */
    final void clearCache() {
        openedFileHandler.cacheClear();
    }

    /**
     * Constructs a new NRS file.
     * 
     * This method gets all information not contained in the descriptor from the {@link MetaConfiguration} values
     * obtained upon {@link #NrsFileJanitor(MetaConfiguration) construction}.
     * 
     * @param header
     *            the header of the persistent image
     * @return a functional instance of {@link NrsFile}
     * @throws NrsException
     *             if the file cannot be created for any reason
     */
    public final NrsFile createNrsFile(final NrsFileHeader<NrsFile> header) throws NrsException {

        final NrsFile result;

        // checks the remaining space on the file store above alert level
        try {
            final FileStore targetStore = Files.getFileStore(directory.toPath());
            if (io.eguan.utils.Files.getRemainingUsablePercentage(targetStore) < limitPercentage) {
                throw new NrsException("Remaining storage space limit percentage reached");
            }

            result = new NrsFile(imagesFileMapper, header, postOfficeRef.get());
            result.create();

            // Create the block file if necessary
            try {
                if (header.isBlocks()) {
                    final NrsFileHeader<NrsFileBlock> headerBlocks = header.newBlocksHeader();
                    final NrsFileBlock nrsFileBlock = new NrsFileBlock(blocksFileMapper, headerBlocks,
                            postOfficeRef.get());
                    nrsFileBlock.create();
                    result.setFileBlock(nrsFileBlock);
                }
            }
            catch (IOException | RuntimeException | Error e) {
                result.delete();
                throw e;
            }
        }
        catch (final NrsException ne) {
            throw ne;
        }
        catch (final IOException ie) {
            throw new NrsException("Exception while creating persistent storage", ie);
        }

        openedFileHandler.cachePut(result.getDescriptor().getFileId(), result);

        return result;
    }

    /**
     * Loads an existing {@link NrsFile} from the provided file.
     * 
     * @param sourceFile
     *            the file from which to load the {@link NrsFile}
     * @return a functional instance read from the given file
     * @throws NrsException
     *             if load failed
     */
    public final NrsFile loadNrsFile(final Path sourceFile) throws NrsException {
        // Atomic get/create instance
        final NrsFileHeader<NrsFile> header = loadNrsFileHeader(sourceFile);
        final Lock nrsFileInstancesLock = openedFileHandler.getCacheWriteLock();
        nrsFileInstancesLock.lock();
        try {
            final UuidT<NrsFile> id = header.getFileId();
            final NrsFile nrsFile = openedFileHandler.cacheLookup(id);
            if (nrsFile != null) {
                return nrsFile;
            }
            final NrsFile result = new NrsFile(imagesFileMapper, header, postOfficeRef.get());
            assert result.getDescriptor().getFileId().equals(id);

            // Load the block file if necessary
            if (header.isBlocks()) {
                // Need to read from the file to get the stored parameters (cluster size, ...)
                final CharSequence blockId = new UuidCharSequence(id);
                final Path blockFile = blocksFileMapper.mapIdToFile(blockId).toPath();
                final NrsFileHeader<NrsFileBlock> headerBlocks = loadNrsFileHeader(blockFile);

                // Some basic checks
                assert headerBlocks.getDeviceId().equals(header.newBlocksHeader().getDeviceId());
                assert headerBlocks.getParentId().equals(header.newBlocksHeader().getParentId());
                assert headerBlocks.getFileId().equals(header.newBlocksHeader().getFileId());

                final NrsFileBlock nrsFileBlock = new NrsFileBlock(blocksFileMapper, headerBlocks, postOfficeRef.get());
                result.setFileBlock(nrsFileBlock);
            }

            openedFileHandler.cachePut(id, result);
            return result;
        }
        finally {
            nrsFileInstancesLock.unlock();
        }
    }

    public final NrsFile loadNrsFile(final UuidT<NrsFile> uuid) throws NrsException {
        // Look for an existing instance in the cache
        final NrsFile nrsFile = openedFileHandler.cacheLookup(uuid);
        if (nrsFile != null) {
            return nrsFile;
        }
        final CharSequence id = new UuidCharSequence(uuid);
        final Path imageFile = imagesFileMapper.mapIdToFile(id).toPath();
        return loadNrsFile(imageFile);
    }

    public final <U> NrsFileHeader<U> loadNrsFileHeader(final Path sourceFile) throws NrsException {
        try (FileChannel readChannel = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
            final ByteBuffer readBuffer = ByteBuffer.allocate(NrsFileHeader.HEADER_LENGTH);
            readBuffer.order(NrsAbstractFile.NRS_BYTE_ORDER);
            final int readLen = readChannel.read(readBuffer);
            if (readLen != NrsFileHeader.HEADER_LENGTH) {
                throw new NrsException("Error reading file '" + sourceFile + "' header, readLen=" + readLen
                        + ", headerlen=" + NrsFileHeader.HEADER_LENGTH);
            }
            readBuffer.position(0);
            return NrsFileHeader.readFromBuffer(readBuffer);
        }
        catch (final NrsException ne) {
            throw ne;
        }
        catch (final IOException ie) {
            throw new NrsException("Error reading file '" + sourceFile + "' header", ie);
        }
    }

    /**
     * Opens the {@link NrsFile} of ID <code>uuid</code>.
     * 
     * @param uuid
     *            ID of the file to open
     * @param readOnly
     *            <code>true</code> for a file opened read-only.
     * @return the opened file
     * @throws IllegalStateException
     * @throws IOException
     */
    public final NrsFile openNrsFile(final UuidT<NrsFile> uuid, final boolean readOnly) throws IOException {
        final NrsFile nrsFile = loadNrsFile(uuid);
        return openedFileHandler.open(nrsFile, readOnly);
    }

    /**
     * Decrements the opened count and closes the {@link NrsFile} if the counter reached 0.
     * 
     * @param nrsFile
     *            opened file to close
     * @param setReadOnly
     *            if <code>true</code>, sets the file 'not writable' after the close.
     * @throws IOException
     */
    public final void closeNrsFile(final NrsFile nrsFile, final boolean setReadOnly) {
        openedFileHandler.close(nrsFile);
        if (setReadOnly) {
            assert nrsFile.isWriteable();
            try {
                // Do not set the file read-only yet: some update messages may be still pending
                sealNrsFile(nrsFile);
            }
            catch (final IOException e) {
                LOGGER.warn("Failed to seal file '" + nrsFile.getFile() + "'", e);
            }
        }
        // Flush messages related to this file
        if (nrsFile.wasWritten()) {
            flushNrsFileMessages(nrsFile.getDescriptor().getFileId());
        }
    }

    /**
     * Decrements the opened count of the {@link NrsFile} but does not close it.
     * 
     * @param nrsFile
     *            opened file to unlock
     * @throws IOException
     */
    public final void unlockNrsFile(final NrsFile nrsFile) {
        openedFileHandler.unlock(nrsFile);
    }

    /**
     * Close the file if it is not locked.
     * 
     * @param nrsFile
     *            to flush/close.
     */
    public final void flushNrsFile(final NrsFile nrsFile) {
        openedFileHandler.flush(nrsFile);
    }

    /**
     * Tells if the file may be written.
     * 
     * @param nrsFile
     * @return <code>true</code> if the file can be written.
     */
    public final boolean isNrsFileWritable(final NrsFile nrsFile) {
        return nrsFile.isWriteable() && !isSealed(nrsFile);
    }

    /**
     * Sets the file as read-only.
     * 
     * @param nrsFile
     *            file to set read-only.
     */
    public final void setNrsFileNoWritable(final NrsFile nrsFile) {
        // First seal the file if not done yet
        try {
            sealNrsFile(nrsFile);
        }
        catch (final IOException e) {
            LOGGER.warn("Failed to seal file " + nrsFile, e);
        }
        finally {
            nrsFile.setNotWritable();
        }
    }

    /**
     * Prepare the update of the given {@link NrsFile}.
     * 
     * @param nrsFile
     * @param nrsVersion
     * @throws IOException
     */
    public final void prepareNrsFileUpdate(final NrsFile nrsFile, final NrsVersion nrsVersion) throws IOException {
        // Must open the file in write mode
        if (!nrsFile.isWriteable()) {
            nrsFile.setWritable();
        }

        // Open, but do not lock the file
        final NrsFile nrsFileOpened = openNrsFile(nrsFile.getDescriptor().getFileId(), false);
        unlockNrsFile(nrsFileOpened);
        assert nrsFileOpened == nrsFile;
    }

    /**
     * Wait for the end of the update of the file and restore its status.
     * 
     * @param nrsFile
     * @param nrsVersion
     * @return <code>true</code> if the update of the file was aborted for some reason.
     */
    public final boolean endNrsFileUpdate(final NrsFile nrsFile, final NrsVersion nrsVersion) {
        // Wait for the end of the update
        final boolean inProgress = nrsFile.waitUpdateEnd(60, TimeUnit.SECONDS);
        if (inProgress) {
            nrsFile.resetUpdate();
        }

        // Restore write status
        if (!nrsVersion.getWritable()) {
            // File should not be in use locally: can safely close it
            flushNrsFile(nrsFile);
            nrsFile.setNotWritable();
        }
        return nrsFile.isLastUpdateAborted();
    }

    /**
     * Deletes the given file.
     * 
     * @param nrsFile
     *            an existing file created by this janitor
     * @throws IOException
     *             if deletion fails
     */
    public final void deleteNrsFile(final NrsFile nrsFile) throws IOException {
        nrsFile.delete();
    }

    /**
     * Visit the {@link NrsFile}s. TODO: visit NrsFiles, not Paths.
     * 
     * @throws IOException
     */
    public final void visitImages(final FileVisitor<? super Path> visitor) throws IOException {
        Files.walkFileTree(imagesDirectory.toPath(), visitor);
    }

    /**
     * Create a new {@link NrsFileHeader} builder, corresponding to the configuration of this janitor.
     * 
     * @return a new builder.
     */
    public final NrsFileHeader.Builder<NrsFile> newNrsFileHeaderBuilder() {
        final NrsFileHeader.Builder<NrsFile> builder = new NrsFileHeader.Builder<>();
        builder.clusterSize(clusterSize);
        return builder;
    }

    /**
     * Tag the file as sealed. Once sealed, a file should not be modified, except for update from a remote version if
     * necessary.
     * 
     * @param nrsFile
     *            {@link NrsFile} to seal.
     * @throws IOException
     */
    public final void sealNrsFile(final NrsFile nrsFile) throws IOException {
        if (!isSealed(nrsFile)) {
            final Path nrsFilePath = nrsFile.getFile();
            io.eguan.utils.Files.setUserAttr(nrsFilePath, ATTR_SEALED);
        }
    }

    /**
     * Tells is the {@link NrsFile} is sealed.
     * 
     * @param nrsFile
     * @return <code>true</code> if the file is sealed.
     * @throws IOException
     */
    public final boolean isSealed(final NrsFile nrsFile) {
        return isSealed(nrsFile.getFile());
    }

    /**
     * Tells is the {@link NrsFile} denoted by the given path is sealed.
     * 
     * @param nrsFilePath
     * @return <code>true</code> if the file is sealed.
     * @throws IOException
     */
    public final boolean isSealed(final Path nrsFilePath) {
        return io.eguan.utils.Files.isUserAttrSet(nrsFilePath, ATTR_SEALED);
    }

    /**
     * Instantiates the {@link FileMapper} from the provided {@link MetaConfiguration}.
     * 
     * @param the
     *            {@link MetaConfiguration}
     * @return the {@link FileMapper} configured in the provided {@link MetaConfiguration}
     */
    private static final FileMapper getFileMapperFromConfig(final MetaConfiguration configuration, final File baseDir) {

        final Type fileMapperValue = FileMapperConfigKey.getInstance().getTypedValue(configuration);
        assert fileMapperValue != null;

        return fileMapperValue.newInstance(baseDir, 32, configuration);
    }

}
