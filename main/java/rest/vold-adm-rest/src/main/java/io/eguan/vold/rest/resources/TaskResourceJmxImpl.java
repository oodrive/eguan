package io.eguan.vold.rest.resources;

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

import io.eguan.dtx.DtxManagerMXBean;
import io.eguan.vold.rest.errors.ClientErrorFactory;
import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.generated.model.ExecState;
import io.eguan.vold.rest.generated.model.Task;
import io.eguan.vold.rest.generated.resources.CancelTaskResource;
import io.eguan.vold.rest.generated.resources.TaskResource;

import java.net.URI;

import javax.management.MXBean;
import javax.ws.rs.core.UriBuilder;

/**
 * {@link TaskResource} implementation for JMX backend.
 * 
 * TODO: The current implementation is not based on any {@link MXBean} proxy of a persistent server-side object (for
 * lack thereof).
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class TaskResourceJmxImpl extends AbstractResource implements TaskResource {

    public final class CancelTaskResourceJmxImpl implements CancelTaskResource {

        @Override
        public final Task cancelTask(final String ownerId) throws CustomResourceException {

            if (dtxProxy.cancelTask(uuid)) {
                final Task task = getTask(ownerId); // task can not be null here
                task.setState(ExecState.CANCELED);
                return task;
            }
            else
                throw ClientErrorFactory.newForbiddenException("Invalid task state", "Task " + uuid
                        + " can not be canceled");
        }
    }

    private final String uuid;
    private final ExecState status;
    private final String resultref;
    private final AbstractTasksResource parent;
    private final DtxManagerMXBean dtxProxy;

    TaskResourceJmxImpl(final String uuid, final ExecState status, final String resultref,
            final AbstractTasksResource parent, final DtxManagerMXBean dtxProxy) {
        this.uuid = uuid;
        this.status = status;
        this.parent = parent;
        this.resultref = resultref;
        this.dtxProxy = dtxProxy;
    }

    @Override
    public final Task getTask(final String ownerId) {
        return AbstractTasksResource.getTaskPojoFromMbeanProxy(uuid, status, resultref);
    }

    @Override
    public final void deleteTask(final String ownerId) {
        getCancelTaskResource(ownerId).cancelTask(ownerId);
    }

    @Override
    public final CancelTaskResource getCancelTaskResource(final String ownerId) {
        return new CancelTaskResourceJmxImpl();
    }

    @Override
    protected final URI getResourceUri() {
        return UriBuilder.fromUri(parent.getResourceUri()).path(uuid).build();
    }

}
