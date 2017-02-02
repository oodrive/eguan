package io.eguan.dtx;

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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

/**
 * Class holding a task and the object associated to the completion of the task.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * @author ebredzinski
 * 
 * @param <V>
 *            Object created by the task or related to the completion of the task.
 */
public abstract class DtxTaskFuture<V> implements Future<V> {

    /** Task followed by this future. */
    private final UUID taskId;
    /** {@link DtxTaskApi} handling this task. */
    private final DtxTaskApi dtxTaskApi;
    /** true when the task is cancelled. */
    private boolean cancelled;

    /**
     * Internal constructor for implementing classses.
     * 
     * @param taskId
     *            the {@link UUID} of the task
     * @param dtxTaskApi
     *            the target {@link DtxTaskApi} instance
     */
    protected DtxTaskFuture(@Nonnull final UUID taskId, @Nonnull final DtxTaskApi dtxTaskApi) {
        super();
        this.taskId = Objects.requireNonNull(taskId);
        this.dtxTaskApi = Objects.requireNonNull(dtxTaskApi);
    }

    /**
     * Gets the UUID of the task held by this.
     * 
     * @return the UUID of the task.
     */
    public final UUID getTaskId() {
        return taskId;
    }

    @Override
    public final boolean cancel(final boolean mayInterruptIfRunning) {
        // Cannot cancel the task once again
        if (cancelled) {
            return false;
        }
        cancelled = dtxTaskApi.cancel(taskId);
        return cancelled;
    }

    @Override
    public final boolean isCancelled() {
        // TODO Should check if the task have been cancelled from elsewhere
        return cancelled;
    }

    @Override
    public final boolean isDone() {
        final DtxTaskAdm task = dtxTaskApi.getTask(taskId);
        final DtxTaskStatus status = task.getStatus();
        return status.isDone();
    }

    private static final long DTX_TASK_END_SLEEP_TIME = 100; // ms

    /**
     * Wait for the end of the task for the given duration.
     * 
     * @param duration
     *            maximum wait duration, in milliseconds. <code>0</code> to wait forever.
     * @return <code>true</code> if the task is done, <code>false</code> if a timeout occurred.
     * @throws InterruptedException
     *             if interrupted while waiting
     * @throws ExecutionException
     *             if the task fails
     */
    protected final boolean waitTaskEnd(final long duration) throws InterruptedException, ExecutionException {

        final long end = duration == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + duration;
        do {
            final DtxTaskStatus status = dtxTaskApi.getTask(taskId).getStatus();

            if (status == DtxTaskStatus.COMMITTED) {
                // Success
                return true;
            }
            else if (status == DtxTaskStatus.ROLLED_BACK || status == DtxTaskStatus.UNKNOWN) {
                // Failure TODO should get some exception from the transaction executor/resource manager
                throw new ExecutionException(new IllegalStateException("Task failed, status=" + status));
            }

            Thread.sleep(DTX_TASK_END_SLEEP_TIME);
        } while (end >= System.currentTimeMillis());

        // Timeout
        return false;
    }
}
