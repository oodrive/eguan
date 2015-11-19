package io.eguan.vvr.repository.core.api;

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

import io.eguan.dtx.DtxTaskApi;
import io.eguan.dtx.DtxTaskFutureVoid;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future for VVR operations that does not create any new object.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class FutureVoid implements Future<Void> {

    private final DtxTaskFutureVoid dtxTaskFutureVoid;

    /**
     * @param repository
     * @param taskId
     *            a valid task {@link UUID} associated to <code>repository</code> or <code>null</code> when there is not
     *            any task created for the given operation.
     * @param object
     *            object related to the task
     */
    protected FutureVoid(final AbstractRepositoryImpl repository, final UUID taskId, final UUID object) {
        super();
        if (taskId == null) {
            dtxTaskFutureVoid = null;
        }
        else {
            final DtxTaskApi dtxTaskApi = repository.getDtxTaskApi();
            dtxTaskFutureVoid = new DtxTaskFutureVoid(taskId, dtxTaskApi);
        }
    }

    /**
     * The {@link UUID} of the related task.
     * 
     * @return the {@link UUID} of a task or <code>null</code> if no task is involved
     */
    public final UUID getTaskId() {
        return dtxTaskFutureVoid == null ? null : dtxTaskFutureVoid.getTaskId();
    }

    @Override
    public final Void get() throws InterruptedException, ExecutionException {
        if (dtxTaskFutureVoid == null) {
            return null;
        }
        return dtxTaskFutureVoid.get();
    }

    @Override
    public final Void get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        if (dtxTaskFutureVoid == null) {
            return null;
        }
        return dtxTaskFutureVoid.get(timeout, unit);
    }

    @Override
    public final boolean cancel(final boolean mayInterruptIfRunning) {
        if (dtxTaskFutureVoid == null) {
            return false;
        }
        return dtxTaskFutureVoid.cancel(mayInterruptIfRunning);
    }

    @Override
    public final boolean isCancelled() {
        if (dtxTaskFutureVoid == null) {
            return false;
        }
        return dtxTaskFutureVoid.isCancelled();
    }

    @Override
    public final boolean isDone() {
        if (dtxTaskFutureVoid == null) {
            return true;
        }
        return dtxTaskFutureVoid.isDone();
    }

}
