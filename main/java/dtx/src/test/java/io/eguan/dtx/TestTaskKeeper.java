package io.eguan.dtx;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.eguan.dtx.DtxConstants;
import io.eguan.dtx.DtxTaskAdm;
import io.eguan.dtx.DtxTaskApi;
import io.eguan.dtx.DtxTaskApiAbstract;
import io.eguan.dtx.DtxTaskInfo;
import io.eguan.dtx.DtxTaskInternal;
import io.eguan.dtx.DtxTaskStatus;
import io.eguan.dtx.TaskKeeper;
import io.eguan.dtx.DtxTaskApiAbstract.TaskKeeperParameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.junit.Test;

/**
 * Test for the API the {@link TaskKeeper} class.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */

public final class TestTaskKeeper {

    private static final class TestTaskInfoImpl extends DtxTaskInfo {

        protected TestTaskInfoImpl(final UUID source) {
            super(source);
        }
    }

    private static final class TestDtxTaskApiImpl extends DtxTaskApiAbstract {
        protected TestDtxTaskApiImpl(final TaskKeeperParameters parameters) {
            super(parameters);
        }

        @Override
        public UUID submit(final UUID resourceId, final byte[] payload) throws IllegalStateException {
            return null;
        }

        @Override
        public boolean cancel(final UUID taskId) throws IllegalStateException {
            return false;
        }

        @Override
        protected TaskLoader readTask(final UUID taskId) {
            return new TaskLoader(new DtxTaskAdm(taskId, null, null, null, DtxTaskStatus.COMMITTED),
                    new TestTaskInfoImpl(UUID.randomUUID()));
        }
    }

    private DtxTaskApi dtxTaskApi;
    private DtxTaskInternal dtxTaskInternal;

    private TaskKeeper taskKeeper;

    private static final long DEFAULT_ABSOLUTE_DURATION = 5000;
    private static final int DEFAULT_ABSOLUTE_SIZE = 5000;
    private static final long DEFAULT_MAX_DURATION = 2000;
    private static final int DEFAULT_MAX_SIZE = 2000;

    private static final long DEFAULT_PERIOD = 2000;
    private static final long DEFAULT_DELAY = 0;

    /**
     * Initialization method for {@link TaskKeeper} with default parameters
     */
    private final TaskKeeper setUpTaskKeeper() {

        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);
        return new TaskKeeper(parameters);
    }

    /**
     * Initialization method for dtxTaskApi with default parameters
     */
    private final void setUpDtxTaskApi() {

        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);
        final TestDtxTaskApiImpl dtxTaskApiImpl = new TestDtxTaskApiImpl(parameters);
        dtxTaskInternal = dtxTaskApiImpl.getDtxTaskInternal();
        dtxTaskApi = dtxTaskApiImpl;
    }

    /**
     * Start a purge
     */
    private final void startOnePurge() throws InterruptedException {
        taskKeeper.startPurge();
        // Wait a little to let the purge doing its job
        Thread.sleep(500);
        taskKeeper.stopPurge();
    }

    /**
     * Populate task keeper with committed and rolled-back tasks
     */
    private final void populateDoneTasks() {

        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE * 2; i++) {
            if (i % 2 == 0) {
                taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.COMMITTED, null);
            }
            else {
                taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.ROLLED_BACK, null);
            }
        }
    }

    /**
     * Tests the set of all the parameters for a task keeper.
     * 
     */
    @Test
    public void testSetTask() {

        final UUID taskId = UUID.randomUUID();
        final long txId = DtxTestHelper.nextTxId();
        final UUID resourceId = UUID.randomUUID();
        final UUID source = UUID.randomUUID();
        final DtxTaskInfo info = new TestTaskInfoImpl(source);

        taskKeeper = setUpTaskKeeper();

        // Add the first task.
        taskKeeper.setTask(taskId, txId, resourceId, DtxTaskStatus.STARTED, info);

        // Add 2 others.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), resourceId, DtxTaskStatus.COMMITTED, null);
        taskKeeper.setTask(UUID.randomUUID(), DtxConstants.DEFAULT_LAST_TX_VALUE, UUID.randomUUID(),
                DtxTaskStatus.PENDING, null);

        taskKeeper.setTaskReadableId(taskId, "task 1", "description 1");

        // Check access to DtxTaskInfo.
        assertEquals(info, taskKeeper.getDtxTaskInfo(taskId, null));

        // Check access with dtxTaskAdm.
        assertEquals("task 1", taskKeeper.getDtxTask(taskId, null).getName());
        assertEquals("description 1", taskKeeper.getDtxTask(taskId, null).getDescription());
        assertEquals(DtxTaskStatus.STARTED, taskKeeper.getDtxTask(taskId, null).getStatus());
        assertEquals(resourceId.toString(), taskKeeper.getDtxTask(taskId, null).getResourceId());
        assertEquals(taskId.toString(), taskKeeper.getDtxTask(taskId, null).getTaskId());
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == taskKeeper.getTaskTimeStamp(taskId));

        // Change status.
        taskKeeper.setTaskStatus(taskId, DtxTaskStatus.COMMITTED);
        assertEquals(DtxTaskStatus.COMMITTED, taskKeeper.getDtxTask(taskId, null).getStatus());
        taskKeeper.setTaskStatus(txId, DtxTaskStatus.ROLLED_BACK);
        assertEquals(DtxTaskStatus.ROLLED_BACK, taskKeeper.getDtxTask(taskId, null).getStatus());

        // Change info.
        final DtxTaskInfo newInfo = new TestTaskInfoImpl(UUID.randomUUID());
        taskKeeper.setDtxTaskInfo(taskId, newInfo);
        assertEquals(newInfo, taskKeeper.getDtxTaskInfo(taskId, null));

        // Check if the 2 tasks are returned.
        final DtxTaskAdm[] tasks = taskKeeper.getResourceManagerTasks(resourceId);
        assertEquals(2, tasks.length);
        for (int i = 0; i < 2; i++) {
            assertEquals(resourceId.toString(), tasks[i].getResourceId());
        }

        // Ask an unknown task.
        final UUID unknownTaskId = UUID.randomUUID();
        assertEquals(taskKeeper.getDtxTask(unknownTaskId, null).getStatus(), DtxTaskStatus.UNKNOWN);
        assertEquals(DtxConstants.DEFAULT_TIMESTAMP_VALUE, taskKeeper.getTaskTimeStamp(unknownTaskId));
        assertNull(taskKeeper.getDtxTaskInfo(unknownTaskId, null));

        // Set default transaction ID to a new task.
        final UUID newTaskId = UUID.randomUUID();
        taskKeeper.setTaskTransactionId(newTaskId, DtxConstants.DEFAULT_LAST_TX_VALUE);

        // Check that the task is no registered in the task keeper
        for (final DtxTaskAdm task : taskKeeper.getTasks()) {
            assertFalse(UUID.fromString(task.getTaskId()).equals(newTaskId));
        }
        // Set valid transaction ID to the new task.
        final long newTxId = DtxTestHelper.nextTxId();
        taskKeeper.setTaskTransactionId(newTaskId, newTxId);

        // Check if the task is not registered.
        boolean found = false;
        for (final DtxTaskAdm task : taskKeeper.getTasks()) {
            if (UUID.fromString(task.getTaskId()).equals(newTaskId)) {
                found = true;
            }
        }
        assertTrue(found);

        // Try to setStatus of a task which is not in cache : no error
        taskKeeper.setTaskStatus(DtxTestHelper.nextTxId(), DtxTaskStatus.ROLLED_BACK);
    }

    /**
     * Tests the set of all the parameters for a task keeper.
     * 
     */
    @Test
    public void testSetTaskViaDtxApi() {

        final UUID taskId = UUID.randomUUID();
        final long txId = DtxTestHelper.nextTxId();
        final UUID resourceId = UUID.randomUUID();
        final UUID source = UUID.randomUUID();
        final DtxTaskInfo info = new TestTaskInfoImpl(source);

        setUpDtxTaskApi();

        // Add the first task.
        dtxTaskApi.setTask(taskId, txId, resourceId, DtxTaskStatus.STARTED, info);

        // Add 2 others.
        dtxTaskApi.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), resourceId, DtxTaskStatus.COMMITTED, null);
        dtxTaskApi.setTask(UUID.randomUUID(), DtxConstants.DEFAULT_LAST_TX_VALUE, UUID.randomUUID(),
                DtxTaskStatus.PENDING, null);

        dtxTaskInternal.setTaskReadableId(taskId, "task 1", "description 1");

        // Check access to DtxTaskInfo.
        assertEquals(info, dtxTaskApi.getDtxTaskInfo(taskId));

        // Check access with dtxTaskAdm.
        assertEquals("task 1", dtxTaskApi.getTask(taskId).getName());
        assertEquals("description 1", dtxTaskApi.getTask(taskId).getDescription());
        assertEquals(DtxTaskStatus.STARTED, dtxTaskApi.getTask(taskId).getStatus());
        assertEquals(resourceId.toString(), dtxTaskApi.getTask(taskId).getResourceId());
        assertEquals(taskId.toString(), dtxTaskApi.getTask(taskId).getTaskId());
        assertFalse(DtxConstants.DEFAULT_TIMESTAMP_VALUE == dtxTaskInternal.getTaskTimestamp(taskId));

        // Change status.
        dtxTaskInternal.setTaskStatus(taskId, DtxTaskStatus.COMMITTED);
        assertEquals(DtxTaskStatus.COMMITTED, dtxTaskApi.getTask(taskId).getStatus());
        dtxTaskInternal.setTaskStatus(txId, DtxTaskStatus.ROLLED_BACK);
        assertEquals(DtxTaskStatus.ROLLED_BACK, dtxTaskApi.getTask(taskId).getStatus());

        // Change info.
        final DtxTaskInfo newInfo = new TestTaskInfoImpl(UUID.randomUUID());
        dtxTaskInternal.setDtxTaskInfo(taskId, newInfo);
        assertEquals(newInfo, dtxTaskApi.getDtxTaskInfo(taskId));

        // Check if the 2 tasks are returned.
        final DtxTaskAdm[] tasks = dtxTaskApi.getResourceManagerTasks(resourceId);
        assertEquals(2, tasks.length);
        for (int i = 0; i < 2; i++) {
            assertEquals(resourceId.toString(), tasks[i].getResourceId());
        }

        // Ask an unknown task.
        final UUID unknownTaskId = UUID.randomUUID();
        // The status is commited
        assertEquals(dtxTaskApi.getTask(unknownTaskId).getStatus(), DtxTaskStatus.COMMITTED);
        // There is an exising dtxTaskInfo
        assertNotNull(dtxTaskApi.getDtxTaskInfo(unknownTaskId));
        // But timestamp is default
        assertEquals(DtxConstants.DEFAULT_TIMESTAMP_VALUE, dtxTaskInternal.getTaskTimestamp(unknownTaskId));
        // And the task is not registered in the list
        for (final DtxTaskAdm task : dtxTaskApi.getTasks()) {
            assertFalse(UUID.fromString(task.getTaskId()).equals(unknownTaskId));
        }

        // Set default transaction ID to a new task.
        final UUID newTaskId = UUID.randomUUID();
        dtxTaskInternal.setTaskTransactionId(newTaskId, DtxConstants.DEFAULT_LAST_TX_VALUE);

        // Check that the task is no registered in the task keeper
        for (final DtxTaskAdm task : dtxTaskApi.getTasks()) {
            assertFalse(UUID.fromString(task.getTaskId()).equals(newTaskId));
        }
        // Set valid transaction ID to the new task.
        final long newTxId = DtxTestHelper.nextTxId();
        dtxTaskInternal.setTaskTransactionId(newTaskId, newTxId);

        // Check if the task is not registered.
        boolean found = false;
        for (final DtxTaskAdm task : dtxTaskApi.getTasks()) {
            if (UUID.fromString(task.getTaskId()).equals(newTaskId)) {
                found = true;
            }
        }
        assertTrue(found);

        // Try to setStatus of a task which is not in cache : no error
        dtxTaskInternal.setTaskStatus(DtxTestHelper.nextTxId(), DtxTaskStatus.ROLLED_BACK);
    }

    /**
     * Tests the {@link taskKeeper#setTaskReadableId} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public void testSetTaskReadableIdBadUUID() {
        taskKeeper = setUpTaskKeeper();
        // Set readable iD with null UUID.
        taskKeeper.setTaskReadableId(null, "name", "description");
    }

    /**
     * Tests the {@link taskKeeper#setTaskTransactionId} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test
     */
    @Test(expected = NullPointerException.class)
    public void testSetTaskTransactionIdBadUUID() {
        taskKeeper = setUpTaskKeeper();
        // Set transaction ID with null UUID.
        taskKeeper.setTaskTransactionId(null, DtxTestHelper.nextTxId());
    }

    /**
     * Tests the {@link taskKeeper#setDtxTaskInfo} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public void testSetDtxTaskInfoBadUUID() {
        taskKeeper = setUpTaskKeeper();
        // Set dtxTaskInfo with null UUID
        taskKeeper.setDtxTaskInfo(null, null);
    }

    /**
     * Tests the {@link taskKeeper#setTaskStatus} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public void testSetTaskStatusBadUUID() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.setTaskStatus(null, DtxTaskStatus.COMMITTED);
    }

    /**
     * Tests the {@link taskKeeper#setTask} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public void testSetTaskBadUUID() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.setTask(null, DtxConstants.DEFAULT_LAST_TX_VALUE, UUID.randomUUID(), DtxTaskStatus.PENDING, null);
    }

    /**
     * Tests the {@link taskKeeper#getDtxTask} method's failure due to an invalid UUID.
     * 
     * @throws NullPointerException
     *             if the {@link UUID} is null, expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public void testgetDtxTaskBadUUID() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.getDtxTask(null, null);
    }

    /**
     * Tests the start and restart of the purge.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, not part of this test.
     */
    @Test
    public void testPurgeStartRestart() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.startPurge();
        taskKeeper.startPurge();
    }

    /**
     * Tests the stop and restop of the purge.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, not part of this test.
     */
    @Test
    public void testPurgeStopRestop() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.startPurge();
        taskKeeper.stopPurge();
        taskKeeper.stopPurge();
    }

    /**
     * Tests the stop without started before.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, not part of this test.
     */
    @Test
    public void testPurgeStopNoStart() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.stopPurge();
    }

    /**
     * Tests the start purge with invalid period.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadPeriod() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, -1, DEFAULT_DELAY);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the start purge with invalid delay.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadDelay() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, -1);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the start purge with invalid absolute duration.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadAbsoluteDuration() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(-1, DEFAULT_ABSOLUTE_SIZE,
                DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the start purge with invalid max duration.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadAbsoluteSize() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION, -1,
                DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the start purge with invalid max size.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadMaxDuration() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, -1, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the start purge with invalid max size.
     * 
     * @throws IllegalArgumentException
     *             if the {@link TaskKeeperParameters} are not valid, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPurgeBadMaxSize() {
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, -1, DEFAULT_PERIOD, DEFAULT_DELAY);
        taskKeeper = new TaskKeeper(parameters);
        taskKeeper.startPurge();
    }

    /**
     * Tests the purge when max size is achieved.
     * 
     * @throws InterruptedException
     *             if any thread has interrupted the current thread, not part of this test.
     */
    @Test
    public void testPurgeKeepOnlyMaxSize() throws InterruptedException {

        // Create a task keeper with very long absolute duration.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add a not done task before.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null);

        populateDoneTasks();

        // Add a not done task after.
        taskKeeper
                .setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.PREPARED, null);

        // Wait the max duration.
        Thread.sleep(DEFAULT_MAX_DURATION + 100);

        // Purge.
        startOnePurge();

        final DtxTaskAdm[] endedTasks = taskKeeper.getTasks();

        // Keep only tasks with duration > max duration and the not done tasks.
        assertEquals(DEFAULT_MAX_SIZE + 2, endedTasks.length);

        // Add a not done task.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null);

        // Purge.
        startOnePurge();

        // Only tasks with duration > max duration and 1 not done task.
        assertEquals(DEFAULT_MAX_SIZE + 3, taskKeeper.getTasks().length);
    }

    /**
     * Tests the purge when absolute size is achieved.
     * 
     * @throws InterruptedException
     *             if any thread has interrupted the current thread, not part of this test.
     */
    @Test
    public void testPurgeKeepAbsoluteSize() throws InterruptedException {

        // Create a task keeper with high absolute duration and high max duration.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION * 100, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add a not done task before.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null);

        // Add tasks.
        populateDoneTasks();

        // Add a not done task after.
        taskKeeper
                .setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.PREPARED, null);

        // Purge.
        startOnePurge();

        // Total done task is absolute size.
        assertEquals(DEFAULT_ABSOLUTE_SIZE + 2, taskKeeper.getTasks().length);

        // Add a not done task.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null);

        // Purge.
        startOnePurge();

        // Only tasks with duration > max duration and 1 not done task.
        assertEquals(DEFAULT_ABSOLUTE_SIZE + 3, taskKeeper.getTasks().length);
    }

    /**
     * Tests the purge when absolute duration is achieved with not done tasks older and younger than done tasks.
     * 
     * @throws InterruptedException
     *             if any thread has interrupted the current thread, not part of this test.
     */
    @Test
    public void testPurgeAfterAbsoluteDuration() throws InterruptedException {

        // Default setup.
        taskKeeper = setUpTaskKeeper();

        // Add a not done task before.
        taskKeeper.setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null);

        populateDoneTasks();

        // Add a not done task after.
        taskKeeper
                .setTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.PREPARED, null);

        // Wait Absolute duration
        Thread.sleep(DEFAULT_ABSOLUTE_DURATION + 100);

        startOnePurge();

        // Only the not done tasks are still present
        assertEquals(2, taskKeeper.getTasks().length);

    }

    /**
     * Tests the task load with no timestamp.
     * 
     * @throws IllegalArgumentException
     *             if not timestamp is set for the task, expected for this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLoadTaskWithoutTimestamp() {
        taskKeeper = setUpTaskKeeper();
        taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED,
                null, 0);
    }

    /**
     * Tests the load of not done tasks when absolute size is achieved.
     * 
     */
    @Test
    public void testLoadMoreThanAbsoluteSizeWithNotDoneTask() {

        // Create a task keeper with high durations.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION * 100, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add DEFAULT_ABSOLUTE_SIZE not done tasks.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {
            taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.PENDING,
                    null, System.currentTimeMillis());
        }
        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        // Add one more.
        taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED,
                null, System.currentTimeMillis());

        assertEquals(DEFAULT_ABSOLUTE_SIZE + 1, taskKeeper.getTasks().length);

        // Add a done task.
        taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED,
                null, System.currentTimeMillis());

        assertEquals(DEFAULT_ABSOLUTE_SIZE + 2, taskKeeper.getTasks().length);
    }

    /**
     * Tests the load of done tasks when absolute size is achieved.
     * 
     */
    @Test
    public void testLoadMoreThanAbsoluteSizeWithDoneTasks() {

        // Create a task keeper with high durations.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION * 100, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        final ArrayList<UUID> taskIds = new ArrayList<UUID>();

        // Add DEFAULT_ABSOLUTE_SIZE done tasks
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {
            final UUID taskId = UUID.randomUUID();
            taskIds.add(taskId);

            if (i % 2 == 0) {
                taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED, null,
                        System.currentTimeMillis());
            }
            else {
                taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.ROLLED_BACK,
                        null, System.currentTimeMillis());
            }
        }

        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        // Add a new one
        final UUID taskId = UUID.randomUUID();
        taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED, null,
                System.currentTimeMillis());

        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        // Now check the tasks contained in the task keeper.
        final DtxTaskAdm[] dtxTaskAdm = taskKeeper.getTasks();
        boolean newTaskFound = false;

        for (final DtxTaskAdm task : dtxTaskAdm) {
            if (task.getTaskId().equals(taskId.toString())) {
                newTaskFound = true;
            }
            // The older must not be found.
            assertFalse(task.getTaskId().equals(taskIds.get(0)));
        }
        // The last loaded must be found instead.
        assertTrue(newTaskFound);

    }

    /**
     * Tests the load of done tasks with a duration higher than absolute duration.
     * 
     */
    @Test
    public void testLoadOlderThanAbsoluteDuration() {

        // Default setup.
        taskKeeper = setUpTaskKeeper();

        // Add DEFAULT_ABSOLUTE_SIZE tasks with a duration higher than DEFAULT_ABSOLUTE_DURATION.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {

            if (i % 2 == 0) {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.COMMITTED, null, System.currentTimeMillis() - 2 * DEFAULT_ABSOLUTE_DURATION);
            }
            else {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.ROLLED_BACK, null, System.currentTimeMillis() - 2 * DEFAULT_ABSOLUTE_DURATION);
            }
        }
        // None must be loaded.
        assertEquals(0, taskKeeper.getTasks().length);
    }

    /**
     * Tests the load of done tasks with a duration higher than max duration.
     * 
     */
    @Test
    public void testLoadOlderThanMaxDuration() {

        // Create a task keeper with a high absolute duration.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add DEFAULT_ABSOLUTE_SIZE tasks with duration higher than DEFAULT_MAX_DURATION.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {

            if (i % 2 == 0) {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.COMMITTED, null, System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);
            }
            else {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.ROLLED_BACK, null, System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);
            }
        }
        // Only DEFAULT_MAX_SIZE tasks must be loaded.
        assertEquals(DEFAULT_MAX_SIZE, taskKeeper.getTasks().length);
    }

    /**
     * Tests the load of done tasks with a duration higher than max duration with some not done tasks.
     * 
     */
    @Test
    public void testLoadOlderThanMaxDurationWithNotDoneTasks() {

        // Create a task keeper with high absolute duration.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add a task with started status and a duration higher than DEFAULT_MAX_DURATION.
        final UUID startedTask = UUID.randomUUID();
        taskKeeper.loadTask(startedTask, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null,
                System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);

        // Add tasks with done status and a duration higher than DEFAULT_MAX_DURATION.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {

            if (i % 2 == 0) {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.COMMITTED, null, System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);
            }
            else {
                taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                        DtxTaskStatus.ROLLED_BACK, null, System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);
            }
        }
        assertEquals(DEFAULT_MAX_SIZE + 1, taskKeeper.getTasks().length);

        // Add a task with started status and a duration higher than DEFAULT_MAX_DURATION.
        taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED,
                null, System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);

        assertEquals(DEFAULT_MAX_SIZE + 2, taskKeeper.getTasks().length);
    }

    /**
     * Tests the load of more than absolute done tasks with a duration higher than max duration.
     * 
     */
    @Test
    public void testLoadMoreThanAbsoluteSizeWithMoreThanMaxDuration() {

        // Create a task keeper with a high durations.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION * 100, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        final HashMap<Long, Long> oldTimestamps = new HashMap<>();
        final HashMap<Long, Long> newTimestamps = new HashMap<>();

        final HashMap<UUID, Long> oldTasks = new HashMap<>();
        final HashMap<UUID, Long> newTasks = new HashMap<>();

        // Create coherent couples (transactionID, timestamp).
        for (int i = 0; i < DEFAULT_MAX_SIZE; i++) {
            // Create timestamp with duration higher than max duration.
            oldTimestamps.put(Long.valueOf(DtxTestHelper.nextTxId()),
                    Long.valueOf(System.currentTimeMillis() - 3 * DEFAULT_MAX_DURATION));
        }

        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {
            // Create timestamp with duration smaller than max duration.
            newTimestamps.put(Long.valueOf(DtxTestHelper.nextTxId()), Long.valueOf(System.currentTimeMillis()));
        }

        // Add DEFAULT_ABSOLUTE_SIZE tasks with newTimestamps before
        for (final Entry<Long, Long> entry : newTimestamps.entrySet()) {
            final UUID taskId = UUID.randomUUID();
            taskKeeper.loadTask(taskId, entry.getKey().longValue(), UUID.randomUUID(), DtxTaskStatus.COMMITTED,
                    new TestTaskInfoImpl(UUID.randomUUID()), entry.getValue().longValue());
            newTasks.put(taskId, entry.getValue());

        }
        // Now cache must be full
        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        // Add DEFAULT_MAX_SIZE tasks with oldTimestamps after
        for (final Entry<Long, Long> entry : oldTimestamps.entrySet()) {
            final UUID taskId = UUID.randomUUID();
            taskKeeper.loadTask(taskId, entry.getKey().longValue(), UUID.randomUUID(), DtxTaskStatus.ROLLED_BACK, null,
                    entry.getValue().longValue());
            oldTasks.put(taskId, entry.getValue());
        }

        final DtxTaskAdm[] tasks = taskKeeper.getTasks();

        // Only DEFAULT_ABSOLUTE_SIZE must have been loaded.
        assertEquals(DEFAULT_ABSOLUTE_SIZE, tasks.length);

        // Only the new must be present
        for (int i = 0; i < tasks.length; i++) {
            assertNull(oldTasks.get(UUID.fromString(tasks[i].getTaskId())));
            assertNotNull(newTasks.get(UUID.fromString(tasks[i].getTaskId())));
        }

    }

    /**
     * Tests the load of not done tasks.
     * 
     */
    @Test
    public void testLoadNotDoneTasks() {

        // Create a task keeper with high durations.
        final TaskKeeperParameters parameters = new TaskKeeperParameters(DEFAULT_ABSOLUTE_DURATION * 100,
                DEFAULT_ABSOLUTE_SIZE, DEFAULT_MAX_DURATION * 100, DEFAULT_MAX_SIZE, DEFAULT_PERIOD, DEFAULT_DELAY);

        taskKeeper = new TaskKeeper(parameters);

        // Add some done tasks with small duration.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE / 2; i++) {

            taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                    DtxTaskStatus.ROLLED_BACK, null, System.currentTimeMillis());
        }
        // All should be present in the cache.
        assertEquals(DEFAULT_ABSOLUTE_SIZE / 2, taskKeeper.getTasks().length);

        // Add a task with started status
        final UUID startedTask = UUID.randomUUID();
        taskKeeper.loadTask(startedTask, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.STARTED, null,
                System.currentTimeMillis());

        // A new task should appear.
        assertEquals(DEFAULT_ABSOLUTE_SIZE / 2 + 1, taskKeeper.getTasks().length);

        // Add other done asks with small duration.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE / 2; i++) {

            taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(),
                    DtxTaskStatus.ROLLED_BACK, null, System.currentTimeMillis());
        }
        // the cache must be full
        assertEquals(DEFAULT_ABSOLUTE_SIZE + 1, taskKeeper.getTasks().length);

        // Add a pending task.
        final UUID pendingTask = UUID.randomUUID();
        taskKeeper.loadTask(pendingTask, DtxConstants.DEFAULT_LAST_TX_VALUE, UUID.randomUUID(), DtxTaskStatus.PENDING,
                null, System.currentTimeMillis());

        // A new task should appear.
        assertEquals(DEFAULT_ABSOLUTE_SIZE + 2, taskKeeper.getTasks().length);

        // Now check the cache, the 2 not done tasks must be present.
        final DtxTaskAdm[] dtxTaskAdm = taskKeeper.getTasks();
        boolean startedTaskFound = false;
        boolean pendingTaskFound = false;

        for (final DtxTaskAdm task : dtxTaskAdm) {
            if (task.getTaskId().equals(startedTask.toString())) {
                startedTaskFound = true;
            }
            else if (task.getTaskId().equals(pendingTask.toString())) {
                pendingTaskFound = true;
            }
        }
        assertTrue(startedTaskFound);
        assertTrue(pendingTaskFound);
    }

    /**
     * Tests the load of tasks with a duration negative when cache is full.
     * 
     */
    @Test
    public void testLoadTaskInTheFutureWithAbsoluteSize() {

        // Default setup is enough.
        taskKeeper = setUpTaskKeeper();

        // Add some done tasks.
        for (int i = 0; i < DEFAULT_ABSOLUTE_SIZE; i++) {
            final UUID taskId = UUID.randomUUID();
            taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED, null,
                    System.currentTimeMillis());
        }
        // The task keeper is full.
        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        // Add a new task with a time stamp in the future.
        final UUID taskId = UUID.randomUUID();
        taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED, null,
                System.currentTimeMillis() + DEFAULT_ABSOLUTE_DURATION);

        assertEquals(DEFAULT_ABSOLUTE_SIZE, taskKeeper.getTasks().length);

        final DtxTaskAdm[] dtxTaskAdm = taskKeeper.getTasks();
        boolean FutureTaskFound = false;
        for (final DtxTaskAdm task : dtxTaskAdm) {
            if (task.getTaskId().equals(taskId.toString())) {
                FutureTaskFound = true;
            }
        }
        // The task must be found in the task keeper cache.
        assertTrue(FutureTaskFound);
    }

    /**
     * Tests the load of tasks with a duration negative when max size is achieved.
     * 
     */
    @Test
    public void testLoadTaskInTheFutureWithMaxSize() {

        // Default setup is enough.
        taskKeeper = setUpTaskKeeper();

        for (int i = 0; i < DEFAULT_MAX_SIZE; i++) {
            final UUID taskId = UUID.randomUUID();
            // Add Task with Duration higher than DEFAULT_MAX_DURATION.
            taskKeeper.loadTask(taskId, DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED, null,
                    System.currentTimeMillis() - 2 * DEFAULT_MAX_DURATION);
        }
        assertEquals(DEFAULT_MAX_SIZE, taskKeeper.getTasks().length);

        // Load a task with negative duration.
        taskKeeper.loadTask(UUID.randomUUID(), DtxTestHelper.nextTxId(), UUID.randomUUID(), DtxTaskStatus.COMMITTED,
                null, System.currentTimeMillis() + DEFAULT_ABSOLUTE_DURATION);

        // This task has been added.
        assertEquals(DEFAULT_MAX_SIZE + 1, taskKeeper.getTasks().length);
    }

    /**
     * Tests the execution of the purge.
     * 
     */
    @Test
    public void testPurgeExecution() throws InterruptedException {

        // Default setup.
        taskKeeper = setUpTaskKeeper();

        // Start the purge.
        taskKeeper.startPurge();

        // Populate with done tasks.
        populateDoneTasks();

        // The cache is full
        assertEquals(DEFAULT_ABSOLUTE_SIZE * 2, taskKeeper.getTasks().length);

        // Wait at least MAX_DURATION
        Thread.sleep(DEFAULT_MAX_DURATION);

        // Wait for purge tasks with duration higher than DEFAULT_MAX_DURATION
        int retryCount = 2;
        int length = taskKeeper.getTasks().length;
        for (; retryCount > 0 && DEFAULT_MAX_SIZE != length; retryCount--) {
            Thread.sleep(DEFAULT_PERIOD);
            length = taskKeeper.getTasks().length;
        }
        assertTrue(retryCount > 0);

        // Wait at least the time left before ABSOLUTE_DURATION
        Thread.sleep(DEFAULT_ABSOLUTE_DURATION - DEFAULT_MAX_DURATION - (2 - retryCount) * DEFAULT_PERIOD);

        // Wait for purge tasks > ABSOLUTE_DURATION
        retryCount = 2;
        length = taskKeeper.getTasks().length;
        for (; retryCount > 0 && 0 != length; retryCount--) {
            Thread.sleep(DEFAULT_PERIOD);
            length = taskKeeper.getTasks().length;
        }
        assertTrue(retryCount > 0);
        taskKeeper.stopPurge();
    }

}
