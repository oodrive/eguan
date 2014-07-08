package com.oodrive.nuage.vold.rest.resources;

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

import java.net.URI;
import java.util.List;

import javax.ws.rs.core.UriBuilder;

import com.oodrive.nuage.dtx.DtxManagerMXBean;
import com.oodrive.nuage.vold.model.VvrMXBean;
import com.oodrive.nuage.vold.model.VvrTask;
import com.oodrive.nuage.vold.rest.generated.model.ExecState;
import com.oodrive.nuage.vold.rest.generated.model.Task;
import com.oodrive.nuage.vold.rest.generated.model.TaskList;
import com.oodrive.nuage.vold.rest.generated.resources.DevicesResource;
import com.oodrive.nuage.vold.rest.generated.resources.SnapshotsResource;
import com.oodrive.nuage.vold.rest.generated.resources.TaskResource;
import com.oodrive.nuage.vold.rest.generated.resources.VvrTasksResource;
import com.oodrive.nuage.vold.rest.generated.resources.VvrsResource;
import com.oodrive.nuage.vold.rest.util.ResourcePath;
import com.oodrive.nuage.vvr.persistence.repository.VvrTaskInfo;

/**
 * {@link VvrTasksResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author pwehrle
 * 
 */
public final class VvrTasksResourceJmxImpl extends AbstractTasksResource implements VvrTasksResource {

    private final VvrMXBean vvrProxy;

    VvrTasksResourceJmxImpl(final URI tasksResUri, final VvrsResourceJmxImpl rootResource, final URI resourceUri,
            final VvrMXBean vvrProxy) {
        super(tasksResUri, rootResource, resourceUri);
        this.vvrProxy = vvrProxy;
    }

    @Override
    public final TaskList getAllTasks(final String ownerId) {
        final TaskList result = getObjectFactory().createTaskList();
        final List<Task> taskList = result.getTasks();

        for (final VvrTask task : vvrProxy.getVvrTasks()) {
            taskList.add(getTaskPojoFromMbeanProxy(task.getTaskId(), constructStatus(task),
                    constructResultReference(task)));
        }
        return result;
    }

    @Override
    public final TaskResource getTaskResource(final String ownerId, final String taskId) {

        final VvrTask task = vvrProxy.getVvrTask(taskId);

        if (task == null) // Jersey will generate a 404 not found error
            return null;

        final DtxManagerMXBean dtxManager = createDtxProxy(ownerId);
        final TaskResourceJmxImpl result = new TaskResourceJmxImpl(task.getTaskId(), constructStatus(task),
                constructResultReference(task), this, dtxManager);

        ResourcePath.injectUriInfoContext(uriInfo, result);
        return result;
    }

    /**
     * Construct a resource path
     * 
     * @param targetType
     *            the type of the target
     * @return a string with the resources path
     */
    static String constructResourcesPath(final VvrTask task) {

        final VvrTaskInfo vvrTaskInfo = task.getInfo();
        if (vvrTaskInfo == null) {
            return "";
        }
        Class<?> resourceClass;
        switch (vvrTaskInfo.getTargetType()) {
        case DEVICE:
            resourceClass = DevicesResource.class;
            break;
        case SNAPSHOT:
            resourceClass = SnapshotsResource.class;
            break;
        case VVR:
        default:
            resourceClass = VvrsResource.class;
            break;

        }
        return ResourcePath.extractPathForSubResourceLocator(VvrResourceJmxImpl.class, resourceClass);
    }

    private String constructResultReference(final VvrTask task) {
        final VvrTaskInfo vvrTaskInfo = task.getInfo();
        if (vvrTaskInfo == null) {
            return "";
        }
        final String id = vvrTaskInfo.getTargetId();
        final String resourcesPath = constructResourcesPath(task);
        // construct /storage/vvrs/<uuid vvr>/<target_type>s/<uiid target>
        return UriBuilder.fromUri(getParentUri()).path(resourcesPath).path(id).build().toString();
    }

    /**
     * Construct a resource path
     * 
     * @param targetType
     *            the type of the target
     * @return a string with the resources path
     */
    static ExecState constructStatus(final VvrTask task) {
        switch (task.getStatus()) {
        case PENDING:
            return ExecState.PENDING;
        case STARTED:
            return ExecState.IN_PROGRESS;
        case PREPARED:
            return ExecState.IN_PROGRESS;
        case COMMITTED:
            return ExecState.DONE;
        case ROLLED_BACK:
        case UNKNOWN: // TODO: error 40x or 50x?
        default:
            return ExecState.FAILED;

        }
    }
}
