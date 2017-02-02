package io.eguan.vvr.repository.core.api;

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

import io.eguan.dtx.DtxTaskFuture;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class FutureSnapshot extends DtxTaskFuture<Snapshot> {

    private final UUID snapshotUuid;
    private final AbstractRepositoryImpl repository;

    protected FutureSnapshot(final UUID snapshotUuid, final AbstractRepositoryImpl repository, final UUID taskId) {
        super(taskId, repository.getDtxTaskApi());
        this.snapshotUuid = snapshotUuid;
        this.repository = repository;
    }

    @Override
    public final Snapshot get() throws InterruptedException, ExecutionException {
        // Wait forever for the end of the task
        waitTaskEnd(0);
        return repository.getSnapshot(snapshotUuid);
    }

    @Override
    public final Snapshot get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        // Wait for the task to end
        if (!waitTaskEnd(unit.toMillis(timeout))) {
            throw new TimeoutException();
        }
        return repository.getSnapshot(snapshotUuid);
    }

}
