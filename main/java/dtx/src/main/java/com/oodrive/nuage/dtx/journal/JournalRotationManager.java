package com.oodrive.nuage.dtx.journal;

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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle the rotation schedule of {@link WritableTxJournal}s.
 * 
 * A dedicated thread pool is created with scheduled tasks that launch requested rotations by calling the
 * {@link WritableTxJournal#executeRotation()} method.
 * 
 * To keep this the least intrusive possible, only {@link WeakReference}s to the target {@link WritableTxJournal}s are
 * maintained and rotations are scheduled on a best-effort basis.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class JournalRotationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JournalRotationManager.class);

    private static final int MIN_NB_ROTATOR_THREADS = 1;

    private static final int ROTATOR_TERMINATION_TIMEOUT = 5;
    private static final int SUBMIT_TIMEOUT_MS = 200;

    /**
     * Listener interface for receivers of {@link RotationEvent}s.
     * 
     * 
     */
    public interface RotationListener {
        /**
         * Method called by RotationEvent sources after a rotation is complete.
         * 
         * @param rotevt
         *            the {@link RotationEvent} describing the rotation
         * @throws InterruptedException
         *             if the thread is interrupted during event processing
         */
        public void rotationEventOccured(RotationEvent rotevt) throws InterruptedException;
    }

    /**
     * Event triggered by a journal file rotation.
     * 
     * 
     */
    @Immutable
    public static final class RotationEvent {

        /**
         * The stages of journal file rotation.
         * 
         * 
         */
        public enum RotationStage {
            /**
             * Pre-rotation stage triggered if journal needs rotation, but before any modifications to files are
             * attempted.
             */
            PRE_ROTATE,
            /**
             * Post-rotation stage triggered once the journal was successfully rotated.
             */
            ROTATE_SUCCESS,
            /**
             * Post-rotation stage triggered if the journal rotation was attempted, but failed.
             */
            ROTATE_FAILURE;
        }

        private final String filename;
        private final RotationStage stage;

        /**
         * Constructs an event for a given filename.
         * 
         * @param filename
         *            the filename having been rotated
         * @param stage
         *            the {@link RotationStage} represented by the event
         */
        RotationEvent(final String filename, final RotationStage stage) {
            this.filename = filename;
            this.stage = stage;
        }

        /**
         * Gets the filename for which this event was created.
         * 
         * @return the filename the name of the file having been rotated
         */
        public final String getFilename() {
            return filename;
        }

        /**
         * @return the stage this event represents
         */
        public final RotationStage getStage() {
            return stage;
        }

    }

    /**
     * Rotation task implemented as a {@link Callable} that launches the rotation of the provided
     * {@link WritableTxJournal} .
     * 
     * 
     * This class is {@link Thread#interrupt() interrupt}-aware, i.e. it will stop short of starting a rotation if
     * {@link Thread#isInterrupted()} becomes <code>true</code>.
     * 
     * 
     */
    private final class RotationTask implements Callable<Void> {

        private final WeakReference<WritableTxJournal> targetJRef;

        RotationTask(@Nonnull final WeakReference<WritableTxJournal> targetJRef) {
            this.targetJRef = Objects.requireNonNull(targetJRef);
        }

        @Override
        public final Void call() {
            final Thread thread = Thread.currentThread();

            final WritableTxJournal targetJournal = targetJRef.get();

            if (targetJournal == null) {
                return null;
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Preparing rotation on reference; target=" + targetJRef.get() + ", thread="
                        + thread.getName());
            }

            // does not start a rotation if the thread is interrupted
            if (thread.isInterrupted()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Rotation thread is interrupted; thread=" + thread.getName());
                }
                removeJournalFromRunning(targetJournal);
                return null;
            }

            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Executing rotation on journal; " + targetJournal);
                }
                rotListenerLock.lockInterruptibly();
                try {
                    final Set<RotationListener> listenerList = rotationListeners
                            .get(targetJournal.getJournalFilename());
                    if (listenerList == null) {
                        targetJournal.executeRotation();
                    }
                    else {
                        final RotationListener[] listeners = listenerList.toArray(new RotationListener[listenerList
                                .size()]);
                        targetJournal.executeRotation(listeners);
                    }
                }
                finally {
                    rotListenerLock.unlock();
                }
            }
            catch (final InterruptedException e) {
                // gracefully exit the method on being interrupted
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Interrupted while executing rotation; thread=" + thread.getName());
                }
            }
            finally {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Finished rotation, removing from active list; journal=" + targetJournal + ", thread="
                            + thread.getName());
                }
                removeJournalFromRunning(targetJournal);
            }
            return null;
        }
    }

    /**
     * Lock to guard access to the {@link #runningRotations} map.
     */
    private final Object runningLock = new Object();

    /**
     * Map holding {@link WeakReference}s to {@link WritableTxJournal}s being rotated with their {@link Future} handles
     * on currently running rotation tasks.
     * 
     * Currently {@link Future}s are added to the map on {@link #submitRotation(WritableTxJournal) submission} and
     * removed either upon completion of {@link RotationTask#call() rotation} or {@link #cleanRotationWorkers() cleanup}
     * . The map is read and cleared when calling {@link #stop()} to explicitly {@link Future#cancel(boolean) cancel}
     * all running workers.
     */
    @GuardedBy("runningLock")
    private final ConcurrentHashMap<WeakReference<WritableTxJournal>, Future<?>> runningRotations;

    /**
     * Lock to guard access to this instances runtime state.
     */
    private final Object statusLock = new Object();

    @GuardedBy("statusLock")
    private volatile boolean started = false;

    /**
     * Fixed-sized {@link ExecutorService} for {@link RotationTask}s.
     */
    @GuardedBy("statusLock")
    private ExecutorService executor;

    private final int nbRotatorThreads;

    private final ReentrantLock rotListenerLock = new ReentrantLock();

    private final HashMap<String, Set<RotationListener>> rotationListeners;

    /**
     * Constructs an autonomous instance that must be {@link #start() started} to begin operations.
     * 
     * @param nbRotatorThreads
     *            the number of worker threads to provision for rotation, defaults to {@value #MIN_NB_ROTATOR_THREADS}
     *            if given an inferior value
     */
    public JournalRotationManager(final int nbRotatorThreads) {
        runningRotations = new ConcurrentHashMap<WeakReference<WritableTxJournal>, Future<?>>();
        rotationListeners = new HashMap<String, Set<RotationListener>>();
        this.nbRotatorThreads = Math.max(MIN_NB_ROTATOR_THREADS, nbRotatorThreads);
    }

    /**
     * Starts the instance. After successful completion, rotation requests can be
     * {@link #submitRotation(WritableTxJournal) submitted} and will be serviced.
     */
    public final void start() {
        synchronized (statusLock) {
            if (started) {
                return;
            }
            executor = Executors.newFixedThreadPool(nbRotatorThreads, new ThreadFactory() {

                private int serial;

                @Override
                public final Thread newThread(final Runnable r) {
                    serial++;
                    final Thread result = new Thread(r, "JournalRotation-" + serial);
                    result.setPriority(Thread.NORM_PRIORITY + 1);
                    result.setDaemon(true);
                    return result;
                }
            });
            started = true;
        }
    }

    /**
     * Gets the started status.
     * 
     * @return <code>true</code> if this instance has been successfully {@link #start() started}, <code>false</code>
     *         otherwise
     */
    public final boolean isStarted() {
        return started;
    }

    /**
     * Stops the instance. After successful completion, no more rotation requests can be
     * {@link #submitRotation(WritableTxJournal) submitted}. This is however reversible by calling {@link #start()}.
     */
    public final void stop() {
        synchronized (statusLock) {

            if (!started) {
                return;
            }

            synchronized (runningLock) {
                // at this point no more new rotations should be launched
                for (final Future<?> currWorker : runningRotations.values()) {
                    currWorker.cancel(true);
                }
                // clear the list of running workers (to avoid memory leaks)
                runningRotations.clear();
            }

            executor.shutdown();
            try {
                if (!executor.awaitTermination(ROTATOR_TERMINATION_TIMEOUT, TimeUnit.SECONDS)) {
                    LOGGER.warn("Rotation manager terminated running tasks");
                }
            }
            catch (final InterruptedException e) {
                LOGGER.error("Interrupted while shutting down rotators.");
            }
            finally {
                final List<Runnable> rotationBacklog = executor.shutdownNow();
                started = false;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Shutdown rotation executor; pending tasks remaining=" + rotationBacklog.size());
                }
            }
        }
    }

    /**
     * Adds a {@link RotationListener} that will be notified on rotations of any of the given files.
     * 
     * @param listener
     *            the non-<code>null</code> {@link RotationListener}
     * @param filenames
     *            the filenames for which to register the event listener
     */
    public final void addRotationEventListener(@Nonnull final RotationListener listener, final String... filenames) {
        Objects.requireNonNull(listener);
        if (filenames.length == 0) {
            return;
        }
        try {
            rotListenerLock.lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted", e);
        }
        try {
            for (final String currFilename : filenames) {
                Set<RotationListener> listeners = rotationListeners.get(currFilename);
                if (listeners == null) {
                    listeners = new HashSet<RotationListener>();
                    rotationListeners.put(currFilename, listeners);
                }
                listeners.add(listener);
            }
        }
        finally {
            rotListenerLock.unlock();
        }
    }

    /**
     * Removes a registered listener.
     * 
     * @param listener
     *            the {@link RotationListener} to remove
     */
    public final void removeRotationEventListener(final RotationListener listener) {
        try {
            rotListenerLock.lockInterruptibly();
        }
        catch (final InterruptedException e) {
            throw new IllegalStateException("Interrupted", e);
        }
        try {
            for (final String currFilename : rotationListeners.keySet()) {
                rotationListeners.get(currFilename).remove(listener);
            }
        }
        finally {
            rotListenerLock.unlock();
        }
    }

    /**
     * Submits a journal for rotation.
     * 
     * This class only retains {@link WeakReference}s to the given {@link WritableTxJournal}
     * 
     * @param journal
     *            a non-<code>null</code> {@link WritableTxJournal}
     */
    final void submitRotation(@Nonnull final WritableTxJournal journal) {

        if (!started) {
            throw new IllegalStateException("Not started");
        }

        Objects.requireNonNull(journal);

        synchronized (runningLock) {

            cleanRotationWorkers();

            for (final WeakReference<WritableTxJournal> currJournalRef : runningRotations.keySet()) {
                if (journal.equals(currJournalRef.get())) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Rotation of journal is running; journal=" + journal);
                    }
                    return;
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Enqueueing journal for rotation; journal=" + journal);
            }
            runningRotations.put(new WeakReference<WritableTxJournal>(journal),
                    executor.submit(new RotationTask(new WeakReference<WritableTxJournal>(journal))));
        }
    }

    /**
     * Checks if there are finished tasks in {@link #runningRotations} and removes them if necessary.
     * 
     * This methods need external synchronization on {@link #runningRotations}.
     */
    private final void cleanRotationWorkers() {

        final ArrayList<WeakReference<WritableTxJournal>> removeJournalRefs = new ArrayList<>();
        for (final WeakReference<WritableTxJournal> currJournalRef : runningRotations.keySet()) {
            final Future<?> currFuture = runningRotations.get(currJournalRef);
            if (currFuture.isDone()) {
                try {
                    currFuture.get(SUBMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException | ExecutionException | TimeoutException e) {
                    LOGGER.warn("Worker ended with error", e);
                }
                removeJournalRefs.add(currJournalRef);
            }
        }
        for (final WeakReference<WritableTxJournal> removeJournal : removeJournalRefs) {
            runningRotations.remove(removeJournal);
        }
    }

    /**
     * Removes a journal instance from the list of {@link #runningRotations running rotations}.
     * 
     * @param targetJournal
     *            the {@link WritableTxJournal} to remove
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    private final void removeJournalFromRunning(@Nonnull final WritableTxJournal targetJournal)
            throws NullPointerException {
        Objects.requireNonNull(targetJournal);
        synchronized (runningLock) {
            for (final Iterator<WeakReference<WritableTxJournal>> iter = this.runningRotations.keySet().iterator(); iter
                    .hasNext();) {
                final WeakReference<WritableTxJournal> currRef = iter.next();
                final WritableTxJournal currJournal = currRef.get();
                if ((currJournal != null) && (currJournal == targetJournal)) {
                    this.runningRotations.remove(currRef);
                    return;
                }
            }
        }
    }

}
