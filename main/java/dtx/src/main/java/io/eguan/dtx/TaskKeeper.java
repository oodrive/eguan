package io.eguan.dtx;

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

import io.eguan.dtx.DtxTaskApiAbstract.TaskKeeperParameters;
import io.eguan.dtx.DtxTaskApiAbstract.TaskLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeper of the {@link DtxTask}. Keeps the last known state and the extra datas of the {@link DtxTask}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * @author pwehrle
 * 
 */
final class TaskKeeper {

    private final class TaskPurger extends TimerTask {
        @Override
        public void run() {
            try {
                purgeTasks();
            }
            catch (final Throwable t) {
                // Ignore
                LOGGER.warn("Purge has failed " + t);
            }
        }
    }

    /**
     * Extra datas of a {@link DtxTask}. Name, description, task information and the last status of the task.
     * 
     * 
     */
    private static final class TaskExtraData {
        private String name;
        private String description;
        private UUID resourceId;
        private DtxTaskStatus lastStatus = DtxTaskStatus.UNKNOWN;
        private DtxTaskInfo taskInfo;
        private long timestamp = DtxConstants.DEFAULT_TIMESTAMP_VALUE;

        protected TaskExtraData(final long timestamp) {
            super();
            this.timestamp = timestamp;
        }

        final String getName() {
            return name;
        }

        final void setName(final String name) {
            this.name = name;
        }

        final String getDescription() {
            return description;
        }

        final void setDescription(final String description) {
            this.description = description;
        }

        final UUID getResourceId() {
            return resourceId;
        }

        final void setResourceId(final UUID resourceId) {
            this.resourceId = resourceId;
        }

        final DtxTaskStatus getLastStatus() {
            return lastStatus;
        }

        final void setLastStatus(final DtxTaskStatus lastStatus) {
            this.lastStatus = lastStatus;
        }

        final DtxTaskInfo getTaskInfo() {
            return taskInfo;
        }

        final void setTaskInfo(final DtxTaskInfo taskInfo) {
            this.taskInfo = taskInfo;
        }

        final long getTimestamp() {
            return timestamp;
        }

        final void setTimestamp(final long timestamp) {
            this.timestamp = timestamp;
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskKeeper.class);
    private static final DtxTaskAdm[] EMPTY_TASK_ADM_ARRAY = new DtxTaskAdm[0];

    /** Extra data of known tasks. */
    @GuardedBy("taskLock")
    private final Map<UUID, TaskExtraData> taskExtraDatas = new HashMap<>();

    /** Transaction identifier of known tasks. */
    @GuardedBy("taskLock")
    private final TreeMap<Long, UUID> transactionIds = new TreeMap<Long, UUID>();

    /** Transaction identifier of unknown tasks. */
    private final Map<UUID, TaskLoader> unknownTaskLoaders = new WeakHashMap<>();

    private final TaskKeeperParameters parameters;

    private TaskPurger purger;

    private final Timer timer = new Timer("taskKeeper");

    /**
     * Shared lock for task management with concurrent modification.
     */
    private final ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();

    /**
     * Constructs a new instance.
     */
    TaskKeeper(final TaskKeeperParameters parameters) {
        super();
        this.parameters = parameters;
    }

    /**
     * Get a {@link TaskLoader} from a given UUID
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     */
    private final TaskLoader getUnknownTaskLoader(final UUID taskUuid) {
        synchronized (unknownTaskLoaders) {
            return unknownTaskLoaders.get(taskUuid);
        }
    }

    /**
     * Set a new {@link TaskLoader} corresponding to a given task ID
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param task
     *            the non-<code>null</code> {@link TaskLoader}
     */
    private final TaskLoader setUnknownTaskLoader(final UUID taskUuid, final TaskLoader task) {
        synchronized (unknownTaskLoaders) {
            return unknownTaskLoaders.put(taskUuid, task);
        }
    }

    /**
     * Start the thread which purges the task cache.
     * 
     * @throws: IllegalArgumentException - if delay < 0, or delay + System.currentTimeMillis() < 0, or period <= 0
     */
    final void startPurge() {

        LOGGER.debug("Start tasks purge with absolute duration: " + parameters.getAbsoluteDuration()
                + " absolute size: " + parameters.getAbsoluteSize() + " max duration: " + parameters.getMaxDuration()
                + " max size: " + parameters.getMaxSize() + " delay: " + parameters.getDelay() + " period: "
                + parameters.getPeriod());
        // Check parameters
        if (parameters.getAbsoluteDuration() <= 0 || parameters.getAbsoluteSize() <= 0
                || parameters.getMaxDuration() <= 0 || parameters.getMaxSize() <= 0) {
            throw new IllegalArgumentException("Task Keeper parameters values must be all positive");

        }
        if (purger == null) {
            purger = new TaskPurger();
        }
        try {
            timer.scheduleAtFixedRate(purger, parameters.getDelay(), parameters.getPeriod());
        }
        catch (final IllegalStateException e) {
            LOGGER.warn("Purge already started");
        }
    }

    /**
     * Stop the thread which purges the task cache.
     */
    final void stopPurge() {
        if (purger != null) {
            LOGGER.debug("Stop purge");
            purger.cancel();
            purger = null;
        }
    }

    /**
     * Create a new object extraData.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @return {@link TaskExtraData} for that task.
     */
    private final TaskExtraData createTaskExtraData(final UUID taskId) {
        TaskExtraData extraData = taskExtraDatas.get(taskId);

        if (extraData == null) {
            extraData = new TaskExtraData(System.currentTimeMillis());
            taskExtraDatas.put(taskId, extraData);
        }
        return extraData;
    }

    /**
     * Create or update a task depending if already exists or not.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param txId
     *            the transaction identifier corresponding to the task
     * @param resourceId
     *            the non-<code>null</code> resource ID to set
     * @param status
     *            the non-<code>null</code> task status
     * @param dtxTaskInfo
     *            the information on the task.
     */
    final void setTask(@Nonnull final UUID taskId, final long txId, @Nonnull final UUID resourceId,
            @Nonnull final DtxTaskStatus status, final DtxTaskInfo dtxTaskInfo) {
        taskLock.writeLock().lock();
        try {

            final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));

            extraData.setResourceId(Objects.requireNonNull(resourceId));
            extraData.setLastStatus(Objects.requireNonNull(status));

            if (dtxTaskInfo != null) {
                extraData.setTaskInfo(dtxTaskInfo);
            }
            if (txId != DtxConstants.DEFAULT_LAST_TX_VALUE) {
                setTaskTransactionId(Objects.requireNonNull(taskId), txId);
            }
        }
        finally {
            taskLock.writeLock().unlock();
        }
    }

    /**
     * Purge the oldest tasks, depending on taskKeeper parameters.
     * 
     * The task Keeper keep only {@link TaskKeeperParameters#getMaxSize()} done tasks with a duration higher than
     * {@link TaskKeeperParameters#getMaxDuration()}. Only the done tasks with a duration smaller than
     * {@link TaskKeeperParameters#getAbsoluteDuration()} are kept. And only
     * {@link TaskKeeperParameters#getAbsoluteSize()} done tasks are kept. The task keeper keep all the not done tasks.
     * 
     */
    private final void purgeTasks() {

        LOGGER.debug("purge tasks");

        taskLock.writeLock().lock();
        try {
            final long currentTimestamp = System.currentTimeMillis();
            int countTotalDoneTasks = 0;
            int countOlderDoneTasks = 0;
            Long firstOlderTxId = null;

            final Iterator<Entry<Long, UUID>> txIdIterator = transactionIds.descendingMap().entrySet().iterator();
            while (txIdIterator.hasNext()) {
                final Entry<Long, UUID> entry = txIdIterator.next();
                final TaskExtraData extraData = taskExtraDatas.get(entry.getValue());

                // Task not ended must not be purged. Ignore it.
                if (!extraData.lastStatus.isDone()) {
                    continue;
                }
                else {
                    final long taskDuration = currentTimestamp - extraData.getTimestamp();

                    // If the task duration is higher than absolute duration, the followers will be too.
                    if (taskDuration > parameters.getAbsoluteDuration()) {
                        firstOlderTxId = entry.getKey();
                        break;
                    }
                    else {
                        // Check if the absolute size has not been achieved
                        if (countTotalDoneTasks < parameters.getAbsoluteSize()) {
                            countTotalDoneTasks++;

                            // Check if the max size with max duration has not been achieved.
                            if (taskDuration > parameters.getMaxDuration()) {
                                if (countOlderDoneTasks < parameters.getMaxSize()) {
                                    countOlderDoneTasks++;
                                }
                                else {
                                    firstOlderTxId = entry.getKey();
                                    break;
                                }
                            }
                        }
                        else {
                            firstOlderTxId = entry.getKey();
                            break;
                        }
                    }
                }
            }
            // Delete the entry up to this last entry
            if (firstOlderTxId != null) {
                final Iterator<Entry<Long, UUID>> iterator = transactionIds.headMap(firstOlderTxId).entrySet()
                        .iterator();
                while (iterator.hasNext()) {
                    final Entry<Long, UUID> entry = iterator.next();
                    final TaskExtraData extraData = taskExtraDatas.get(entry.getValue());

                    if (!extraData.lastStatus.isDone()) {
                        continue;
                    }
                    else {
                        // remove entry from taskExtraData map
                        taskExtraDatas.remove(entry.getValue());
                        // remove entry from transactionIds map
                        iterator.remove();
                    }
                }
                // remove the firstOlderTxId ;
                taskExtraDatas.remove(transactionIds.get(firstOlderTxId));
                transactionIds.remove(firstOlderTxId);
            }

        }
        finally {
            taskLock.writeLock().unlock();
        }
    }

    /**
     * Return the total number of tasks with a done status.
     * 
     */
    private final long getTotalDoneTasks() {
        long count = 0;
        for (final Entry<UUID, TaskExtraData> task : taskExtraDatas.entrySet()) {
            if (task.getValue().getLastStatus().isDone())
                count++;

        }
        return count;
    }

    /**
     * Return the number of tasks with a done status and a duration higher than max duration.
     * 
     */
    private final long getDoneTasksWithMaxDuration(final long currentTimestamp) {
        long count = 0;
        for (final Entry<UUID, TaskExtraData> task : taskExtraDatas.entrySet()) {
            if (task.getValue().getLastStatus().isDone()
                    && (currentTimestamp - task.getValue().getTimestamp()) > parameters.getMaxDuration()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Load or not a task. Remove the oldest task if necessary, depending on taskKeeper parameters
     * 
     * The task Keeper keep only {@link TaskKeeperParameters#getMaxSize()} done tasks with a duration higher than
     * {@link TaskKeeperParameters#getMaxDuration()}. Only the done tasks with a duration smaller than
     * {@link TaskKeeperParameters#getAbsoluteDuration()} are kept. And only
     * {@link TaskKeeperParameters#getAbsoluteSize()} done tasks are kept. The task keeper keep all the not done tasks.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param txId
     *            the transaction identifier corresponding to the task
     * @param resourceId
     *            the non-<code>null</code> resource ID to set
     * @param status
     *            the non-<code>null</code> task status
     * @param dtxTaskInfo
     *            the information on the task.
     */
    final void loadTask(@Nonnull final UUID taskId, final long txId, @Nonnull final UUID resourceId,
            @Nonnull final DtxTaskStatus status, final DtxTaskInfo dtxTaskInfo, final long timestamp) {

        LOGGER.debug("load task: " + taskId + " timestamp: " + timestamp);
        taskLock.writeLock().lock();
        try {

            final long currentTimestamp = System.currentTimeMillis();
            long duration = 0;
            boolean removeBeforeAdd = false;
            long countTotalDoneTasks = getTotalDoneTasks();
            long countDoneTasksWithMaxDuration = getDoneTasksWithMaxDuration(currentTimestamp);

            // Check if timestamp is valid.
            if (timestamp <= 0) {
                throw new IllegalArgumentException("Invalid timestamp");
            }

            // If status is not done add it directly.
            if (status.isDone()) {

                duration = currentTimestamp - timestamp;

                // If the duration is outside the absolute duration, do not load it.
                if (duration > parameters.getAbsoluteDuration()) {
                    return;
                }

                // Check if the task is outside the absolute size.
                if (countTotalDoneTasks < parameters.getAbsoluteSize()) {
                    countTotalDoneTasks++;
                }
                else {
                    // Absolute size is achieved, the older task must be removed.
                    removeBeforeAdd = true;
                }

                // The task is outside the max duration.
                if (duration > parameters.getMaxDuration()) {
                    if (countDoneTasksWithMaxDuration < parameters.getMaxSize()) {
                        countDoneTasksWithMaxDuration++;
                    }
                    else {
                        // The max Size limit is exceeded, the older task must be removed.
                        removeBeforeAdd = true;
                    }
                }

                // Remove first done entry.
                if (removeBeforeAdd) {
                    final Iterator<Entry<Long, UUID>> txIdIterator = transactionIds.entrySet().iterator();

                    while (txIdIterator.hasNext()) {
                        final Entry<Long, UUID> entry = txIdIterator.next();
                        final TaskExtraData extraData = taskExtraDatas.get(entry.getValue());

                        // Skip task with not done status.
                        if (!extraData.lastStatus.isDone()) {
                            continue;
                        }
                        else {
                            if (timestamp > extraData.getTimestamp()) {
                                taskExtraDatas.remove(entry.getValue());
                                txIdIterator.remove();
                                break;
                            }
                            else {
                                // the task is older than the oldest of the task keeper, do not load it
                                return;
                            }
                        }
                    }
                }
            }
            // Load the new task.
            final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));
            extraData.setResourceId(Objects.requireNonNull(resourceId));
            extraData.setLastStatus(Objects.requireNonNull(status));
            if (dtxTaskInfo != null) {
                extraData.setTaskInfo(dtxTaskInfo);
            }
            // Update the default timestamp with the new one.
            extraData.setTimestamp(timestamp);

            if (txId != DtxConstants.DEFAULT_LAST_TX_VALUE) {
                setTaskTransactionId(Objects.requireNonNull(taskId), txId);
            }
        }
        finally {
            taskLock.writeLock().unlock();
        }
    }

    /**
     * Set the name and the description of the task. One or both may be <code>null</code>.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param name
     *            the optional name
     * @param description
     *            the optional description
     */
    final void setTaskReadableId(@Nonnull final UUID taskId, final String name, final String description) {
        taskLock.writeLock().lock();
        try {
            final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));

            extraData.setName(name);
            extraData.setDescription(description);
        }
        finally {
            taskLock.writeLock().unlock();
        }
    }

    /**
     * Set the transaction ID corresponding to the task ID
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param txId
     *            the transaction ID to set
     */
    final void setTaskTransactionId(@Nonnull final UUID taskId, final long txId) {
        taskLock.writeLock().lock();
        try {
            if (txId != DtxConstants.DEFAULT_LAST_TX_VALUE) {

                TaskExtraData extraData = taskExtraDatas.get(taskId);

                if (extraData == null) {
                    extraData = new TaskExtraData(System.currentTimeMillis());
                    taskExtraDatas.put(taskId, extraData);
                }
                transactionIds.put(Long.valueOf(txId), Objects.requireNonNull(taskId));

            }
        }
        finally {
            taskLock.writeLock().unlock();
        }
    }

    /**
     * Set the status.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param status
     *            the status to set
     */
    final void setTaskStatus(@Nonnull final UUID taskId, @Nonnull final DtxTaskStatus status) {
        taskLock.writeLock().lock();
        try {
            final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));
            extraData.setLastStatus(Objects.requireNonNull(status));
        }
        finally {
            taskLock.writeLock().unlock();
        }

    }

    /**
     * Set the status.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param status
     *            the status to set
     */
    final void setTaskStatus(final long transactionId, @Nonnull final DtxTaskStatus status) {
        taskLock.writeLock().lock();
        try {
            final UUID taskId = transactionIds.get(Long.valueOf(transactionId));
            if (taskId != null) {
                final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));
                extraData.setLastStatus(Objects.requireNonNull(status));
            }
            else {
                LOGGER.info("Task can not be found in cache");
            }
        }
        finally {
            taskLock.writeLock().unlock();
        }

    }

    /**
     * Sets the dtx task info.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @param taskInfo
     *            some data coded in a {@link DtxTaskInfo}.
     */
    final void setDtxTaskInfo(@Nonnull final UUID taskId, final DtxTaskInfo taskInfo) {
        taskLock.writeLock().lock();
        try {
            final TaskExtraData extraData = createTaskExtraData(Objects.requireNonNull(taskId));
            extraData.setTaskInfo(taskInfo);
        }
        finally {
            taskLock.writeLock();
        }
    }

    /**
     * Return true if dtxTaskInfo is already set for a given task ID.
     * 
     * @param taskId
     *            the non-<code>null</code> target task's {@link UUID}
     * @return true is the dtxTaskInfo is not null, false otherwise.
     */
    final boolean isDtxTaskInfoSet(final UUID taskId) {
        // Search the task in the current hashmap.
        taskLock.readLock().lock();
        try {
            final TaskExtraData extraData = taskExtraDatas.get(taskId);
            if (extraData != null) {
                return (extraData.getTaskInfo() != null);
            }
            else {
                return false;
            }
        }
        finally {
            taskLock.readLock().unlock();
        }
    }

    /**
     * Gets the dtx task info.
     * 
     * @param taskId
     *            the target task's {@link UUID}
     * @return the DtxTaskInfo object, null if the task does not exist or its info does not exist.
     * */
    final DtxTaskInfo getDtxTaskInfo(final UUID taskId, final DtxTaskApiAbstract dtxTaskApiAbstract) {

        // Search the task in the current hashmap.
        taskLock.readLock().lock();
        try {
            final TaskExtraData extraData = taskExtraDatas.get(taskId);
            if (extraData != null) {
                return extraData.getTaskInfo();
            }
        }
        finally {
            taskLock.readLock().unlock();
        }
        // Search the task in the unknown tasks map.
        TaskLoader task = getUnknownTaskLoader(taskId);
        if (task != null) {
            return task.getInfo();
        }

        if (dtxTaskApiAbstract == null) {
            return null;
        }
        else {
            // Search the task in the journal.
            task = dtxTaskApiAbstract.readTask(taskId);
            setUnknownTaskLoader(taskId, task);
            return task.getInfo();
        }

    }

    /**
     * Gets the dtx task timestamp.
     * 
     * @param taskId
     *            the target task's {@link UUID}
     * @return the timestamp
     * */
    final long getTaskTimeStamp(final UUID taskId) {
        taskLock.readLock().lock();
        try {
            final TaskExtraData extraData = taskExtraDatas.get(taskId);
            if (extraData == null) {
                return DtxConstants.DEFAULT_TIMESTAMP_VALUE;
            }
            else {
                return extraData.getTimestamp();
            }
        }
        finally {
            taskLock.readLock().unlock();
        }
    }

    /**
     * Create the {@link DtxTaskAdm} corresponding to the given task ID.
     * 
     * @param taskId
     *            id of the task
     * @return the immutable task object.
     */
    final DtxTaskAdm getDtxTask(final UUID taskId, final DtxTaskApiAbstract dtxTaskApiAbstract) {
        taskLock.readLock().lock();
        try {
            final TaskExtraData extraData = taskExtraDatas.get(taskId);

            // If the task is not found, return an empty task with status UNKNOWN
            if (extraData != null) {
                return new DtxTaskAdm(taskId, extraData.getName(), extraData.getDescription(),
                        extraData.getResourceId(), extraData.getLastStatus());
            }
        }
        finally {
            taskLock.readLock().unlock();
        }
        // Search the task in the unknown tasks map.
        TaskLoader task = getUnknownTaskLoader(taskId);

        if (task != null) {
            return task.getDtxTaskAdm();
        }

        if (dtxTaskApiAbstract != null) {
            // Search the task in the journal.
            task = dtxTaskApiAbstract.readTask(taskId);
            setUnknownTaskLoader(taskId, task);
            synchronized (task) {
                return task.getDtxTaskAdm();
            }
        }
        else {
            return new DtxTaskAdm(taskId, null, null, null, DtxTaskStatus.UNKNOWN);
        }
    }

    /**
     * Gets the list of known tasks.
     * 
     * @param dtxTaskApi
     *            task API to be able to get the status of tasks.
     * @return the current status of every task.
     */
    final DtxTaskAdm[] getTasks() {
        taskLock.readLock().lock();
        try {

            final DtxTaskAdm[] result = new DtxTaskAdm[taskExtraDatas.size()];
            int i = 0;
            for (final Entry<UUID, TaskExtraData> task : taskExtraDatas.entrySet()) {
                final UUID taskId = task.getKey();
                final TaskExtraData extraData = task.getValue();

                result[i++] = new DtxTaskAdm(taskId, extraData.getName(), extraData.getDescription(),
                        extraData.getResourceId(), extraData.getLastStatus());
            }
            return result;
        }
        finally {
            taskLock.readLock().unlock();
        }
    }

    /**
     * Gets the list of known tasks for a given resource manager.
     * 
     * @param dtxTaskApi
     *            task API to be able to get the status of tasks.
     * @param resourceId
     *            id of the resource manager.
     * @return the current status of every task.
     */
    final DtxTaskAdm[] getResourceManagerTasks(final UUID resourceId) {
        taskLock.readLock().lock();
        try {
            final ArrayList<DtxTaskAdm> resultList = new ArrayList<DtxTaskAdm>(taskExtraDatas.size());
            for (final Entry<UUID, TaskExtraData> task : taskExtraDatas.entrySet()) {
                final UUID taskId = task.getKey();
                final TaskExtraData extraData = task.getValue();
                final UUID id = extraData.getResourceId();

                if (id.equals(resourceId)) {
                    resultList.add(new DtxTaskAdm(taskId, extraData.getName(), extraData.getDescription(), extraData
                            .getResourceId(), extraData.getLastStatus()));
                }
            }
            return resultList.toArray(EMPTY_TASK_ADM_ARRAY);
        }
        finally {
            taskLock.readLock().unlock();
        }
    }

}
