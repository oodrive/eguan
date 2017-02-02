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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link DtxTaskFuture} not related to any object.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 * 
 */
public final class DtxTaskFutureVoid extends DtxTaskFuture<Void> {

    /**
     * Constructs an instance for a given task ID and {@link DtxTaskApi} instance.
     * 
     * @param taskId
     *            the {@link UUID} of the task
     * @param dtxTaskApi
     *            the target {@link DtxTaskApi} instance
     */
    public DtxTaskFutureVoid(final UUID taskId, final DtxTaskApi dtxTaskApi) {
        super(taskId, dtxTaskApi);
    }

    @Override
    public final Void get() throws InterruptedException, ExecutionException {
        // Wait forever for the end of the task
        waitTaskEnd(0);
        // Nothing to return
        return null;
    }

    @Override
    public final Void get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        // Wait for the task to end
        if (!waitTaskEnd(unit.toMillis(timeout))) {
            throw new TimeoutException();
        }
        // Nothing to return
        return null;
    }

}
