package com.oodrive.nuage.ibs;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.utils.ByteArrays;
import com.oodrive.nuage.utils.Files;
import com.oodrive.nuage.utils.Files.HandledFile;
import com.oodrive.nuage.utils.Files.OpenedFileHandler;
import com.oodrive.nuage.utils.mapper.FileMapper;
import com.oodrive.nuage.utils.mapper.FileMapperConfigurationContext;

/**
 * Stores blocks in files. The transactions are stored in memory.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class IbsFilesDB extends IbsDBAbstract {

    private static final Logger LOGGER = LoggerFactory.getLogger(IbsFilesDB.class);

    private static final class IbsHandledFile extends HandledFile<String> {

        /** Associated file */
        private final File file;
        /** Id of the file */
        private final String id;

        /** Set when the file is opened */
        private final Lock openedLock = new ReentrantLock();
        /** Set when the file is opened */
        @GuardedBy(value = "openedLock")
        private FileChannel channel;
        private boolean readOnly;
        private boolean newFile;

        IbsHandledFile(final File file, final String id, final boolean newFile) {
            super();
            this.file = file;
            this.id = id;
            this.newFile = newFile;
        }

        final boolean isNewFile() {
            return newFile;
        }

        final void setNewFile(final boolean newFile) {
            this.newFile = newFile;
        }

        @Override
        protected final void open(final boolean readOnly) throws IOException, IllegalStateException {
            openedLock.lock();
            try {
                if (channel != null) {
                    throw new IllegalStateException("opened");
                }
                this.readOnly = readOnly;
                if (readOnly) {
                    channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                }
                else {
                    channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        @Override
        protected final boolean isOpened() {
            openedLock.lock();
            try {
                return channel != null;
            }
            finally {
                openedLock.unlock();
            }
        }

        @Override
        protected final boolean isOpenedLock() {
            openedLock.lock();
            try {
                // No read/write in progress under openedLock
                return false;
            }
            finally {
                openedLock.unlock();
            }
        }

        @Override
        protected final boolean isOpenedReadOnly() {
            openedLock.lock();
            try {
                return channel != null && readOnly;
            }
            finally {
                openedLock.unlock();
            }
        }

        @Override
        protected final void close() {
            openedLock.lock();
            try {
                if (channel != null) {
                    try {
                        channel.close();
                    }
                    catch (final Throwable t) {
                        LOGGER.warn("Failed to close '" + file.getAbsolutePath() + "'", t);
                    }
                    channel = null;
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        @Override
        protected final String getId() {
            return id;
        }

        final int read(final ByteBuffer buffer) throws IOException {
            openedLock.lock();
            try {
                final long size = channel.size();
                if (buffer.remaining() < size) {
                    throw new IbsBufferTooSmallException((int) size);
                }
                channel.position(0);
                return channel.read(buffer);
            }
            finally {
                openedLock.unlock();
            }
        }

        /**
         * Writes only if the file is empty (does not override a key/pair association).
         * 
         * @param buffer
         * @throws IOException
         */
        final void write(final ByteBuffer buffer) throws IOException {
            openedLock.lock();
            try {
                if (channel.size() == 0) {
                    channel.truncate(buffer.remaining()).position(0);
                    channel.write(buffer);
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        /**
         * Deletes the file.
         */
        final void delete() {
            file.delete();
        }
    }

    /** Configuration for a <byte1>/<byte2>/<file name> mapping */
    private static final MetaConfiguration FILE_MAPPING_CONFIGURATION;
    static {
        try {
            // Write config
            final ByteArrayOutputStream baos = new ByteArrayOutputStream(10);
            try (PrintStream config = new PrintStream(baos)) {
                config.println("com.oodrive.nuage.filemapping.dir.structure.depth=2");
                config.println("com.oodrive.nuage.filemapping.dir.prefix.length=3");
            }
            finally {
                baos.close();
            }

            // Create config
            try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
                FILE_MAPPING_CONFIGURATION = MetaConfiguration.newConfiguration(bais,
                        FileMapperConfigurationContext.getInstance());
            }
        }
        catch (final Exception e) {
            throw new IllegalStateException("Failed to create configuration", e);
        }
    }

    /** File mapper to find files containing the blocks */
    private final FileMapper fileMapper;
    /** Transaction handling */
    private final IbsMemTransaction ibsMemTransaction;

    /** Handler of opened Ibs files */
    private OpenedFileHandler<IbsHandledFile, String> openedFileHandler;

    /**
     * Create a new instance.
     * 
     * @param ibsPath
     *            directory containing the files.
     */
    private IbsFilesDB(final String ibsPath, final File ibsDir) {
        super(ibsPath);

        fileMapper = FileMapper.Type.DEEP.newInstance(ibsDir, 9, FILE_MAPPING_CONFIGURATION);
        ibsMemTransaction = new IbsMemTransaction(this);
    }

    static final Ibs createIbs(final File ibsPath) throws IbsException {
        final File ibsDir = checkIbsPath(ibsPath);
        if (ibsDir.list().length != 0) {
            throw new IbsException(ibsDir.getAbsolutePath(), IbsErrorCode.CREATE_IN_NON_EMPTY_DIR);
        }
        // Add an empty marker file
        try {
            if (!new File(ibsDir, "created").createNewFile()) {
                // Failed to create a new file: already exist?
                throw new IbsException(ibsDir.getAbsolutePath(), IbsErrorCode.CREATE_IN_NON_EMPTY_DIR);
            }
        }
        catch (final IOException e) {
            // Failed to create a new file: access denied?
            throw new IbsException(ibsDir.getAbsolutePath(), e);
        }
        return new IbsFilesDB(ibsDir.getAbsolutePath(), ibsDir);
    }

    /**
     * Opens an existing Ibs.
     * 
     * @param ibsPath
     * @return the new opened Ibs
     * @throws IbsException
     */
    static final Ibs openIbs(final File ibsPath) throws IbsException {
        final File ibsDir = checkIbsPath(ibsPath);
        if (ibsDir.list().length == 0) {
            throw new IbsException(ibsDir.getAbsolutePath(), IbsErrorCode.INIT_FROM_EMPTY_DIR);
        }
        return new IbsFilesDB(ibsDir.getAbsolutePath(), ibsDir);
    }

    private static final File checkIbsPath(final File ibsPath) {
        // Make sure the path is a directory
        if (ibsPath.isDirectory()) {
            // Is a directory
            return ibsPath;
        }

        // The path may denote a IbsLevelDB configuration file
        final File ibpPath = selectDirectoryFromFile(ibsPath);
        if (ibpPath != null && ibpPath.isDirectory()) {
            return ibpPath;
        }

        // Nothing found
        throw new IbsException(ibsPath + " is not a directory", IbsErrorCode.INVALID_IBS_ID);
    }

    private static final String IBP_PATH_STR = "ibp_path";
    private static final String IBP_PATH_DELIM = ",";

    /**
     * Select a directory from the configuration of a {@link IbsLevelDB}. Take the first directory declared in
     * <code>ibp_path</code>.
     * 
     * @param ibsFile
     *            {@link IbsLevelDB} configuration file
     * @return the selected directory or <code>null</code> if the <code>ibsFile</code> is not a valid configuration file
     */
    private static final File selectDirectoryFromFile(final File ibsFile) {
        try {
            // Try to load the configuration
            final Properties config = new Properties();
            try (FileInputStream fis = new FileInputStream(ibsFile)) {
                config.load(fis);
            }
            final String ibpList = config.getProperty(IBP_PATH_STR);
            if (ibpList != null) {
                final String ibp = new StringTokenizer(ibpList, IBP_PATH_DELIM).nextToken();
                return new File(ibp);
            }

            // No IBP defined
            return null;
        }
        catch (final Exception e) {
            LOGGER.warn("'" + ibsFile.getAbsolutePath() + "' is not a valid configuration file");
            return null;
        }
    }

    @Override
    public final boolean isHotDataEnabled() throws IbsException {
        // No replace yet
        return false;
    }

    @Override
    public final int get(final byte[] key, final ByteBuffer data, final int offset, final int length)
            throws IbsException, IbsIOException, IbsBufferTooSmallException, IllegalArgumentException,
            IndexOutOfBoundsException, NullPointerException {
        // TODO shared access to the IBS state during the whole get?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        final IbsHandledFile file = getFile(key, false);
        if (file == null) {
            throw new IbsIOException(toString() + ": " + IbsErrorCode.NOT_FOUND, IbsErrorCode.NOT_FOUND);
        }
        try {
            openedFileHandler.open(file, true);
            try {
                final int prevPosition = data.position();
                try {
                    data.position(offset);
                    data.limit(offset + length);
                    return file.read(data);
                }
                finally {
                    data.rewind().position(prevPosition);
                }
            }
            finally {
                openedFileHandler.unlock(file);
            }
        }
        catch (final IbsIOException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new IbsIOException(ibsPath + ": fail to read file '" + file.getId() + "'", IbsErrorCode.IO_ERROR, e);
        }
    }

    @Override
    public final void del(final byte[] key) throws IbsException, IbsIOException, NullPointerException {
        // TODO shared access to the IBS state during the whole get?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        // Delete the file and remove it from openedFileHandler
        final IbsHandledFile file = getFile(key, true);
        try {
            openedFileHandler.flush(file);
            file.delete();
        }
        finally {
            openedFileHandler.cacheRemove(file.getId());
        }
    }

    @Override
    public final boolean put(final byte[] key, final ByteBuffer data) throws IbsException, IbsIOException,
            NullPointerException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        if (data == null) {
            if (getFile(key, false) == null) {
                throw new IbsIOException(toString() + ": " + IbsErrorCode.NOT_FOUND, IbsErrorCode.NOT_FOUND);
            }
            else {
                return false;
            }
        }
        return put(key, data, data.position(), data.remaining());

    }

    @Override
    public final boolean put(final byte[] key, final ByteString data) throws IbsException, IbsIOException,
            NullPointerException {
        return put(key, data.asReadOnlyByteBuffer());
    }

    @Override
    public final boolean replace(final byte[] oldKey, final byte[] newKey, final ByteBuffer data) throws IbsException,
            IbsIOException {
        // No replace yet
        Objects.requireNonNull(oldKey);

        return put(newKey, data);
    }

    @Override
    public final int createTransaction() throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        return ibsMemTransaction.createTransaction();
    }

    @Override
    public final void commit(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        ibsMemTransaction.commit(txId);
    }

    @Override
    public final void rollback(final int txId) throws IbsException, IllegalArgumentException, IbsIOException {
        if (!started || closed) {
            throw new IbsException(toString());
        }

        if (txId <= 0) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        ibsMemTransaction.rollback(txId);
    }

    @Override
    protected final boolean doPut(final int txId, final byte[] key, final ByteBuffer data, final int offset,
            final int length) throws IbsException, IbsIOException, IllegalArgumentException, IndexOutOfBoundsException,
            NullPointerException {
        // TODO shared access to the IBS state during the whole put?
        if (!started || closed) {
            throw new IbsException(toString());
        }

        checkArgs(key, data, offset, length);

        // Transaction?
        if (txId > 0) {
            // Check if it's a new key
            final boolean newKey = getFile(key, false) == null;
            ibsMemTransaction.put(txId, key, data, offset, length);
            return newKey;
        }

        final IbsHandledFile file = getFile(key, true);
        try {
            openedFileHandler.open(file, false);
            try {
                file.write(data);
            }
            finally {
                openedFileHandler.unlock(file);
            }
        }
        catch (final Exception e) {
            throw new IbsIOException(ibsPath + ": fail to write file '" + file.getId() + "'", IbsErrorCode.IO_ERROR, e);
        }
        return file.isNewFile();
    }

    @Override
    protected final boolean doReplace(final int txId, final byte[] oldKey, final byte[] newKey, final ByteBuffer data,
            final int offset, final int length) throws IbsException, IllegalArgumentException, IbsIOException,
            IndexOutOfBoundsException, NullPointerException {

        // Check references
        Objects.requireNonNull(oldKey);

        // No replace yet
        return doPut(txId, newKey, data, offset, length);
    }

    @Override
    protected final int doStart() {
        openedFileHandler = Files.newOpenedFileHandler();
        return 0;
    }

    @Override
    protected final int doStop() {
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
            LOGGER.warn("Error while closing Ibs files", t);
        }
        openedFileHandler = null;

        // Clear pending transactions
        ibsMemTransaction.clear();
        return 0;
    }

    @Override
    protected final int doClose() {
        return 0;
    }

    @Override
    protected final int doDestroy() {
        try {
            final File ibsDir = new File(ibsPath);
            Files.deleteRecursive(ibsDir.toPath());
        }
        catch (final IOException e) {
            LOGGER.warn("Failed to delete '" + ibsPath + "'", e);
        }
        return 0;
    }

    /**
     * @param key
     * @param create
     * @return the file created or found or <code>null</code> if <code>create</code> is <code>false</code>
     * @throws IbsIOException
     */
    private final IbsHandledFile getFile(final byte[] key, final boolean create) throws IbsIOException {
        final String id = ByteArrays.toHex(key);
        final IbsHandledFile ibsHandledFile;
        try {
            final Lock fileInstancesLock = openedFileHandler.getCacheWriteLock();
            fileInstancesLock.lock();
            try {

                // Already loaded?
                final IbsHandledFile ibsHandledFileTmp = openedFileHandler.cacheLookup(id);
                if (ibsHandledFileTmp != null) {
                    ibsHandledFileTmp.setNewFile(false);
                    return ibsHandledFileTmp;
                }

                final File file = fileMapper.mapIdToFile(id);
                // Create parents and the file if needed
                file.getParentFile().mkdirs();
                if (!create && !file.exists()) {
                    return null;
                }
                final boolean newFile = file.createNewFile();
                ibsHandledFile = new IbsHandledFile(file, id, newFile);

                openedFileHandler.cachePut(id, ibsHandledFile);
            }
            finally {
                fileInstancesLock.unlock();
            }
            return ibsHandledFile;
        }
        catch (final Exception e) {
            throw new IbsIOException(ibsPath + ": fail to get file '" + id + "'", IbsErrorCode.IO_ERROR, e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return "IBS[" + ibsPath + ", started=" + started + ", closed=" + closed + "]";
    }

}
