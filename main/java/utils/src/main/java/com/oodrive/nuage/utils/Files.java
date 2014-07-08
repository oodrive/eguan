package com.oodrive.nuage.utils;

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

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.LongMath;

/**
 * Utility class for {@link File}, {@link Path} and {@link java.nio.file.Files}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class Files {

    private static final Logger LOGGER = LoggerFactory.getLogger(Files.class);

    /**
     * No instance.
     */
    private Files() {
        throw new AssertionError("No instance");
    }

    /**
     * A file that can be kept opened. Should rather be an interface, but that would expose the methods that opens and
     * closes the file.
     * 
     * 
     */
    public static abstract class HandledFile<I> {

        protected HandledFile() {
            super();
        }

        /**
         * Opens the file.
         * 
         * @param readOnly
         *            true if the file is opened for read access only
         * @throws IOException
         * @throws IllegalStateException
         *             if the file is already opened
         */
        protected abstract void open(boolean readOnly) throws IOException, IllegalStateException;

        /**
         * Closes the file.
         */
        protected abstract void close();

        /**
         * Gets the id of the file.
         * 
         * @return the id of the file. Can not be <code>null</code>.
         */
        protected abstract I getId();

        /**
         * Tells if the file is opened.
         * 
         * @return <code>true</code> if the file is opened
         */
        protected abstract boolean isOpened();

        /**
         * Tells if some thread have locked the opening/closing of the file.
         * 
         * @return <code>true</code> if some thread is working on the file or if the file is being opened or closed.
         */
        protected abstract boolean isOpenedLock();

        /**
         * Tells if the file have been opened in read-only mode.
         * 
         * @return <code>true</code> if the file is opened read-only.
         */
        protected abstract boolean isOpenedReadOnly();
    }

    /**
     * Keep opened files that are recently accessed and close them after a while if they are not opened. Keep a cache of
     * the handled file.
     * 
     * @param <I>
     *            identifier of a file
     */
    public static final class OpenedFileHandler<F extends HandledFile<I>, I> implements Runnable {
        private final ReentrantLock openedLock = new ReentrantLock();
        /** List of opened files. A file may be opened more than one time. */
        private final List<F> locked = new ArrayList<>();

        /** Future to cancel the closing of files */
        private ScheduledFuture<?> openedFileHandlerFuture;

        /** Opened files, not accessed since the last task run */
        private Map<I, F> openedOld = new ConcurrentHashMap<>();
        /** Opened files, accessed since the last task run */
        private Map<I, F> openedNew = new ConcurrentHashMap<>();

        /** Maximum number of opened files */
        private final int limit;

        private final ReadWriteLock fileInstancesLock = new ReentrantReadWriteLock();
        /** Cache of created instances */
        @GuardedBy(value = "fileInstancesLock")
        private final HashMap<I, WeakReference<F>> fileInstances = new HashMap<>();

        OpenedFileHandler(final int limit) {
            super();
            this.limit = limit;
        }

        @Override
        public final void run() {
            openedLock.lock();
            try {
                // Switch Maps
                final Map<I, F> openedOldPrev = openedOld;
                openedOld = openedNew;
                openedNew = new ConcurrentHashMap<>();

                // Close old files
                for (final Map.Entry<I, F> entry : openedOldPrev.entrySet()) {
                    final F file = entry.getValue();
                    if (locked.contains(file) || file.isOpenedLock()) {
                        openedNew.put(entry.getKey(), file);
                    }
                    else {
                        doClose(file);
                    }
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        /**
         * Cancel background closing of the files.
         */
        public final void cancel() {
            if (openedFileHandlerFuture != null) {
                try {
                    openedFileHandlerFuture.cancel(false);
                }
                catch (final Throwable t) {
                    LOGGER.warn("Error while cancelling task", t);
                }
                openedFileHandlerFuture = null;
            }
        }

        /**
         * Look for an opened file for the given id.
         * 
         * @param id
         * @return a file found or <code>null</code>
         */
        private final F lookupFile(final I id) {
            openedLock.lock();
            try {
                // Look among old ones
                final F file = openedOld.remove(id);

                // Promote file
                if (file != null) {
                    openedNew.put(id, file);
                    return file;
                }
                // Look among new ones
                return openedNew.get(id);
            }
            finally {
                openedLock.unlock();
            }
        }

        /**
         * Add a file in the handler and increments the opened count.
         * 
         * @param file
         * @param readOnly
         *            if true, open the file read-only if necessary
         * @return an opened file associated to <code>id</code>. May not be <code>file</code>
         * @throws IllegalStateException
         * @throws IOException
         */
        public final F open(final F file, final boolean readOnly) throws IllegalStateException, IOException {
            openedLock.lock();
            try {
                final I id = file.getId();
                final F result = lookupFile(id);
                if (result != null) {
                    // Should be the same instance
                    assert result == file;

                    // Need to re-open File?
                    if (readOnly) {
                        locked.add(result);
                        return result;
                    }
                    if (!readOnly && !result.isOpenedReadOnly()) {
                        locked.add(result);
                        return result;
                    }

                    // Need to close it before in mode read-write
                    result.close();
                }

                // Must open file
                file.open(readOnly);
                openedNew.put(id, file);
                locked.add(file);

                // Have added a new file: check limit
                checkLimit();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Open '" + file + "'");
                }

                return file;
            }
            finally {
                openedLock.unlock();
            }
        }

        private final void checkLimit() {
            assert openedLock.isHeldByCurrentThread();

            final int count = openedOld.size() + openedNew.size();
            if (count > limit) {
                int toClose = count - limit;

                // First, close some old files
                for (final Map.Entry<I, F> entry : openedOld.entrySet()) {
                    final F file = entry.getValue();
                    if (!locked.contains(file) && !file.isOpenedLock()) {
                        doClose(file);
                        openedOld.remove(entry.getKey());
                        toClose--;
                        if (toClose == 0) {
                            return;
                        }
                    }
                }

                // May close new files
                for (final Map.Entry<I, F> entry : openedNew.entrySet()) {
                    final F file = entry.getValue();
                    if (!locked.contains(file) && !file.isOpenedLock()) {
                        doClose(file);
                        openedNew.remove(entry.getKey());
                        toClose--;
                        if (toClose == 0) {
                            return;
                        }
                    }
                }

            }
        }

        public final void close(final F file) {
            openedLock.lock();
            try {
                if (locked.remove(file) && !locked.contains(file)) {
                    // Can close the file
                    doClose(file);
                    final I id = file.getId();
                    openedNew.remove(id);
                    openedOld.remove(id);
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        public final void unlock(final F file) {
            openedLock.lock();
            try {
                if (!locked.remove(file)) {
                    LOGGER.warn("'" + file + "' was not opened", new Throwable());
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        public final void flush(final F file) {
            openedLock.lock();
            try {
                if (!locked.contains(file)) {
                    doClose(file);
                    final I id = file.getId();
                    openedNew.remove(id);
                    openedOld.remove(id);
                }
            }
            finally {
                openedLock.unlock();
            }
        }

        public final void closeAll() {
            openedLock.lock();
            try {
                // Reset reference count
                locked.clear();

                // Close old
                for (final F file : openedOld.values()) {
                    doClose(file);
                }
                openedOld.clear();

                // Close new
                for (final F file : openedNew.values()) {
                    doClose(file);
                }
                openedNew.clear();
            }
            finally {
                openedLock.unlock();
            }
        }

        /**
         * Lock to take for an atomic test/set in the cache.
         * 
         * @return the exclusive lock of the cache.
         */
        public final Lock getCacheWriteLock() {
            return fileInstancesLock.writeLock();
        }

        /**
         * Look for a matching file in the instance cache.
         * 
         * @param id
         * @return the file found or <code>null</code>
         */
        public final F cacheLookup(final I id) {
            fileInstancesLock.readLock().lock();
            try {
                final WeakReference<F> ref = fileInstances.get(id);
                if (ref != null) {
                    final F file = ref.get();
                    if (file != null) {
                        return file;
                    }
                }
            }
            finally {
                fileInstancesLock.readLock().unlock();
            }
            return null;
        }

        /**
         * Put a new Id/file pair in the cache.
         * 
         * @param id
         * @param file
         */
        public final void cachePut(final I id, final F file) {
            fileInstancesLock.writeLock().lock();
            try {
                fileInstances.put(id, new WeakReference<>(file));
            }
            finally {
                fileInstancesLock.writeLock().unlock();
            }
        }

        /**
         * Removes a Id/file pair from the cache.
         * 
         * @param id
         */
        public final void cacheRemove(final I id) {
            fileInstancesLock.writeLock().lock();
            try {
                fileInstances.remove(id);
            }
            finally {
                fileInstancesLock.writeLock().unlock();
            }
        }

        /**
         * Clear file cache. For unit test purpose only, to actually create new instances for files. Fails if some files
         * are opened.
         */
        public final void cacheClear() {
            fileInstancesLock.writeLock().lock();
            try {
                final Collection<WeakReference<F>> files = fileInstances.values();
                for (final Iterator<WeakReference<F>> iterator = files.iterator(); iterator.hasNext();) {
                    final WeakReference<F> reference = iterator.next();
                    final F file = reference.get();
                    if (file != null && file.isOpened()) {
                        throw new AssertionError(file + " opened");
                    }
                }
                fileInstances.clear();
            }
            finally {
                fileInstancesLock.writeLock().unlock();
            }
        }

        private final void doClose(final F file) {
            try {
                file.close();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close '" + file + "'");
                }
            }
            catch (final Throwable t) {
                LOGGER.warn("Failed to close '" + file + "'", t);
            }
        }
    }

    private static final long OPENED_FILE_HANDLER_DELAY = 15; // 15 seconds

    /** Executor to close the opened files. TODO: raise pool size and set thread timeout? */
    private static final ScheduledThreadPoolExecutor fileCloser = new ScheduledThreadPoolExecutor(1);

    /**
     * Create a new {@link OpenedFileHandler}.
     * 
     * @return a new instance.
     */
    public final static <F extends HandledFile<I>, I> OpenedFileHandler<F, I> newOpenedFileHandler() {
        final OpenedFileHandler<F, I> openedFileHandler = new OpenedFileHandler<F, I>(20);
        openedFileHandler.openedFileHandlerFuture = fileCloser.scheduleAtFixedRate(openedFileHandler,
                OPENED_FILE_HANDLER_DELAY, OPENED_FILE_HANDLER_DELAY, TimeUnit.SECONDS);
        return openedFileHandler;
    }

    /**
     * Singleton visitor that deletes recursively a {@link Path}.
     */
    private static final FileVisitor<Path> PATH_DELETE_REC = new SimpleFileVisitor<Path>() {

        @Override
        public final FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            java.nio.file.Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public final FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc == null) {
                java.nio.file.Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
            throw exc;
        }
    };

    /**
     * Deletes a path recursively. Does nothing if the path does not exist.
     * 
     * @param path
     *            path to delete
     * @throws IOException
     *             if <code>path</code> or its contents can not be deleted
     */
    public static final void deleteRecursive(@Nonnull final Path path) throws IOException {
        if (path.toFile().exists()) {
            java.nio.file.Files.walkFileTree(path, PATH_DELETE_REC);
        }
    }

    /**
     * Interface to follow the progress of a recursive deletion.
     * 
     * 
     */
    public interface DeleteRecursiveProgress {
        /**
         * Notification of the last deleted element.
         * 
         * @param deleted
         *            last deleted element
         * @return tells if the deletion should continue
         */
        FileVisitResult notify(Path deleted);
    }

    /**
     * {@link FileVisitor} that deletes recursively and notify deletion.
     * 
     * 
     */
    private static final class PathDeleteRecProgress extends SimpleFileVisitor<Path> {
        private final Path path;
        private final boolean keepPath;
        private final DeleteRecursiveProgress progress;

        PathDeleteRecProgress(@Nonnull final Path path, final boolean keepPath,
                @Nonnull final DeleteRecursiveProgress progress) {
            super();
            assert java.nio.file.Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);

            this.path = Objects.requireNonNull(path);
            this.keepPath = keepPath;
            this.progress = Objects.requireNonNull(progress);
        }

        @Override
        public final FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            // Always delete: path is a directory
            java.nio.file.Files.delete(file);
            return progress.notify(file);
        }

        @Override
        public final FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc == null) {
                if (keepPath && path.equals(dir)) {
                    // Do not delete root
                    return FileVisitResult.CONTINUE;
                }
                java.nio.file.Files.delete(dir);
                return progress.notify(dir);
            }
            throw exc;
        }
    }

    /**
     * Deletes a path recursively. Does nothing if the path does not exist.
     * 
     * @param path
     *            path to delete
     * @param keepPath
     *            if <code>true</code>, do not delete <code>path</code>, just its contents
     * @param progress
     *            notified of deletions and can stop deletion
     * @throws IOException
     *             if <code>path</code> or its contents can not be deleted
     */
    public static final void deleteRecursive(@Nonnull final Path path, final boolean keepPath,
            @Nonnull final DeleteRecursiveProgress progress) throws IOException {

        // Check here the type of path to avoid a comparison for non directories in the visitor
        if (java.nio.file.Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            final PathDeleteRecProgress visitor = new PathDeleteRecProgress(path, keepPath, progress);
            java.nio.file.Files.walkFileTree(path, visitor);
        }
        else if (!keepPath && java.nio.file.Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            java.nio.file.Files.delete(path);
            progress.notify(path);
        }
    }

    /**
     * Wait for a file to appear.
     * 
     * @param file
     *            file to test
     * @param timeout
     *            timeout in milliseconds
     * @return true if the file has appeared, false after timeout expiration
     */
    public static final boolean waitForFile(final File file, final long timeout) {
        // TODO implement with nio2 file watcher? see java.nio.file.WatchService
        final long wait = timeout / 10L;
        for (long i = 0; i < timeout; i += wait) {
            if (file.exists()) {
                return true;
            }
            try {
                Thread.sleep(wait);
            }
            catch (final InterruptedException e) {
                // Must not wait anymore
                return file.exists();
            }
        }
        return false;
    }

    /**
     * Wait for a file to disappear.
     * 
     * @param file
     *            file to test
     * @param timeout
     *            timeout in milliseconds
     * @return true if the file has disappeared, false after timeout expiration
     */
    public static final boolean waitForFileDeletion(final File file, final long timeout) {
        // TODO implement with nio2 file watcher? see java.nio.file.WatchService
        final long wait = timeout / 10L;
        for (long i = 0; i < timeout; i += wait) {
            if (!file.exists()) {
                return true;
            }
            try {
                Thread.sleep(wait);
            }
            catch (final InterruptedException e) {
                // Must not wait anymore
                return !file.exists();
            }
        }
        return false;
    }

    /**
     * Gets the remaining percentage of usable space for a given {@link FileStore}.
     * 
     * This uses the {@link FileStore#getUsableSpace()} and {@link FileStore#getTotalSpace()} methods to compute the
     * remaining usable storage space.
     * 
     * @param fileStore
     *            the file store for which to compute the value
     * @return the remaining percentage of usable storage space, rounded down to integer percentage points
     * @throws IOException
     *             if reading the corresponding file store properties fails
     */
    public static final int getRemainingUsablePercentage(final FileStore fileStore) throws IOException {
        final long usedSpace = fileStore.getUsableSpace();
        final long totalSpace = fileStore.getTotalSpace();
        return (int) LongMath.divide(usedSpace * 100L, totalSpace, RoundingMode.DOWN);
    }

    /*****************************/
    /** User defined attributes **/
    /*****************************/

    /** Constant string key to test support of user defined attributes. */
    private static final String ATTR_TEST = "Files.testAttr";

    private static final byte ATTR_VALUE_YES_BYTE = 121; // y
    private static final ByteBuffer ATTR_VALUE_YES = ByteBuffer.allocate(1);
    static {
        ATTR_VALUE_YES.put(ATTR_VALUE_YES_BYTE);
    }
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * Checks if the path supports user-defined file system attributes. See {@link UserDefinedFileAttributeView}.
     * 
     * @param path
     * @throws IOException
     *             thrown if <code>path</code> is read-only or does not support user-defined attributes.
     */
    public static final void checkUserAttrSupported(final Path path) throws IOException {
        setUserAttr(path, ATTR_TEST);
        unsetUserAttr(path, ATTR_TEST);
    }

    /**
     * Sets the attribute for path. The value is a boolean value set to <code>true</code>.
     * 
     * @param path
     * @param attr
     * @throws IOException
     *             thrown if writing the attribute has failed
     */
    public static final void setUserAttr(final Path path, final String attr) throws IOException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        synchronized (ATTR_VALUE_YES) {
            ATTR_VALUE_YES.rewind();
            view.write(attr, ATTR_VALUE_YES);
        }
    }

    /**
     * Tells is the given attribute is set for the given path.
     * 
     * @param path
     *            path to test. It must exist and be readable
     * @param attr
     * @return <code>true</code> if the attribute is set
     * @throws IllegalStateException
     *             thrown is the attributes of the path can not be read
     */
    public static final boolean isUserAttrSet(final Path path, final String attr) throws IllegalStateException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        try {
            for (final String name : view.list()) {
                if (attr.equals(name)) {
                    return true;
                }
            }
            return false;
        }
        catch (final IOException e) {
            // Read access to attribute list should not fail
            throw new IllegalStateException("Failed to read file attributes '" + path.toFile().getAbsolutePath() + "'",
                    e);
        }
    }

    /**
     * Sets the attribute value for path.
     * 
     * @param path
     * @param attr
     * @param value
     * @throws IOException
     *             thrown if writing the attribute has failed
     */
    public static final void setUserAttr(final Path path, final String attr, final String value) throws IOException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        // Convert the string to a byte array
        final byte[] valueArray = value.getBytes(UTF8_CHARSET);
        view.write(attr, ByteBuffer.wrap(valueArray));
    }

    /**
     * Gets the value for the given attribute. The value string is returned.
     * 
     * @param path
     * @param attr
     * @return the value found, possibly an empty string or null if the attribute is not set.
     * @throws IOException
     */
    public static final String getUserAttr(final Path path, final String attr) throws IOException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        try {
            for (final String name : view.list()) {
                if (attr.equals(name)) {
                    final ByteBuffer readValueBuf = ByteBuffer.allocate(view.size(name));
                    view.read(name, readValueBuf);
                    readValueBuf.flip();
                    return UTF8_CHARSET.decode(readValueBuf).toString();
                }
            }
            // Not found
            return null;
        }
        catch (final IOException e) {
            // Read access to attribute list should not fail
            throw new IllegalStateException("Failed to read file attributes '" + path.toFile().getAbsolutePath() + "'",
                    e);
        }
    }

    /**
     * Returns the list of the defined attributes for the path.
     * 
     * @param path
     * @return the list of user defined attributes, possibly empty.
     * @throws IOException
     */
    public static final String[] listUserAttr(final Path path) throws IOException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        try {
            final List<String> attrs = view.list();
            return attrs.toArray(EMPTY_STRING_ARRAY);
        }
        catch (final IOException e) {
            // Read access to attribute list should not fail
            throw new IllegalStateException("Failed to read file attributes '" + path.toFile().getAbsolutePath() + "'",
                    e);
        }
    }

    /**
     * Remove the attribute from the user attributes of path. Does nothing if the attribute is not set.
     * 
     * @param path
     * @param attr
     * @throws IOException
     */
    public static final void unsetUserAttr(final Path path, final String attr) throws IOException {
        final UserDefinedFileAttributeView view = java.nio.file.Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class);
        if (isUserAttrSet(path, attr)) {
            view.delete(attr);
        }
    }

}
