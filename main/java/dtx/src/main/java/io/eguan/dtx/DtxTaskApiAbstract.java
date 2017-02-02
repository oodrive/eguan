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

import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Utility class for the implementation of {@link DtxTaskApi}.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public abstract class DtxTaskApiAbstract implements DtxTaskApi {

    /**
     * Parameters for the task Keeper.
     * 
     * 
     */
    public static final class TaskKeeperParameters {
        private final long absoluteDuration;
        private final int absoluteSize;
        private final long maxDuration;
        private final int maxSize;

        private final long period;
        private final long delay;

        public TaskKeeperParameters(final long absoluteDuration, final int absoluteSize, final long maxDuration,
                final int maxSize, final long period, final long delay) {
            super();
            this.absoluteDuration = absoluteDuration;
            this.absoluteSize = absoluteSize;
            this.maxDuration = maxDuration;
            this.maxSize = maxSize;

            this.period = period;
            this.delay = delay;
        }

        public final long getAbsoluteDuration() {
            return absoluteDuration;
        }

        public final int getAbsoluteSize() {
            return absoluteSize;
        }

        public final long getMaxDuration() {
            return maxDuration;
        }

        public final int getMaxSize() {
            return maxSize;
        }

        public final long getPeriod() {
            return period;
        }

        public final long getDelay() {
            return delay;
        }

        @Override
        public final int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (absoluteDuration ^ (absoluteDuration >>> 32));
            result = prime * result + absoluteSize;
            result = prime * result + (int) (delay ^ (delay >>> 32));
            result = prime * result + (int) (maxDuration ^ (maxDuration >>> 32));
            result = prime * result + maxSize;
            result = prime * result + (int) (period ^ (period >>> 32));
            return result;
        }

        @Override
        public final boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof TaskKeeperParameters))
                return false;
            final TaskKeeperParameters other = (TaskKeeperParameters) obj;
            if (absoluteDuration != other.absoluteDuration)
                return false;
            if (absoluteSize != other.absoluteSize)
                return false;
            if (delay != other.delay)
                return false;
            if (maxDuration != other.maxDuration)
                return false;
            if (maxSize != other.maxSize)
                return false;
            return period != other.period;
        }

    }

    /**
     * Utility class to get all the parameters if a given task.
     * 
     * 
     */
    public static final class TaskLoader {
        private final DtxTaskAdm dtxTaskAdm;
        private final DtxTaskInfo info;

        public TaskLoader(final DtxTaskAdm dtxTaskAdm, final DtxTaskInfo info) {
            this.dtxTaskAdm = dtxTaskAdm;
            this.info = info;
        }

        public final DtxTaskAdm getDtxTaskAdm() {
            return dtxTaskAdm;
        }

        public final DtxTaskInfo getInfo() {
            return info;
        }

        public static TaskLoader createUnknownTask(final UUID taskId) {
            return new TaskLoader(new DtxTaskAdm(taskId, null, null, null, DtxTaskStatus.UNKNOWN), null);
        }

    }

    private final TaskKeeper taskKeeper;
    private final DtxTaskInternal taskInternal;

    protected DtxTaskApiAbstract(final TaskKeeperParameters parameters) {
        this.taskKeeper = new TaskKeeper(parameters);
        this.taskInternal = new DtxTaskInternal() {

            @Override
            public final void startPurgeTaskKeeper() {
                taskKeeper.startPurge();
            }

            @Override
            public final void stopPurgeTaskKeeper() {
                taskKeeper.stopPurge();
            }

            @Override
            public final void loadTask(@Nonnull final UUID taskId, final long txId, @Nonnull final UUID resourceId,
                    @Nonnull final DtxTaskStatus status, final DtxTaskInfo info, final long timestamp) {
                taskKeeper.loadTask(taskId, txId, resourceId, status, info, timestamp);
            }

            @Override
            public final void setTaskReadableId(@Nonnull final UUID taskId, final String name, final String description) {
                taskKeeper.setTaskReadableId(taskId, name, description);
            }

            @Override
            public final void setTaskTransactionId(@Nonnull final UUID taskId, final long txId) {
                taskKeeper.setTaskTransactionId(taskId, txId);
            }

            @Override
            public final void setTaskStatus(@Nonnull final UUID taskId, final DtxTaskStatus status) {
                taskKeeper.setTaskStatus(taskId, status);
            }

            @Override
            public final void setTaskStatus(final long transactionId, final DtxTaskStatus status) {
                taskKeeper.setTaskStatus(transactionId, status);
            }

            @Override
            public final void setDtxTaskInfo(@Nonnull final UUID taskId, final DtxTaskInfo taskInfo) {
                taskKeeper.setDtxTaskInfo(taskId, taskInfo);
            }

            @Override
            public final boolean isDtxTaskInfoSet(final UUID taskId) {
                return taskKeeper.isDtxTaskInfoSet(taskId);
            }

            @Override
            public final long getTaskTimestamp(final UUID taskId) {
                return taskKeeper.getTaskTimeStamp(taskId);
            }

        };
    }

    DtxTaskInternal getDtxTaskInternal() {
        return taskInternal;
    }

    @Override
    public final void setTask(@Nonnull final UUID taskId, final long txId, @Nonnull final UUID resourceId,
            @Nonnull final DtxTaskStatus status, final DtxTaskInfo info) {
        taskKeeper.setTask(taskId, txId, resourceId, status, info);
    }

    @Override
    public final DtxTaskInfo getDtxTaskInfo(final UUID taskId) {
        return taskKeeper.getDtxTaskInfo(taskId, this);
    }

    @Override
    public final DtxTaskAdm getTask(final UUID taskId) {
        return taskKeeper.getDtxTask(taskId, this);
    }

    @Override
    public final DtxTaskAdm[] getTasks() {
        return taskKeeper.getTasks();
    }

    @Override
    public final DtxTaskAdm[] getResourceManagerTasks(final UUID resourceId) {
        return taskKeeper.getResourceManagerTasks(resourceId);
    }

    /**
     * Get the task data corresponding to a give task ID. Null is returned if the task is not found
     * 
     * @param taskId
     *            the requested task's ID
     * @return the non-<code>null</code> {@link TaskLoader}
     */

    protected abstract TaskLoader readTask(UUID taskId);

}
