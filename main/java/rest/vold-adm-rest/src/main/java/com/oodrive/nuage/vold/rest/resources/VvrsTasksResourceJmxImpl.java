package com.oodrive.nuage.vold.rest.resources;

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

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.ObjectName;
import javax.ws.rs.core.UriBuilder;

import com.oodrive.nuage.dtx.DtxManagerMXBean;
import com.oodrive.nuage.vold.model.VvrMXBean;
import com.oodrive.nuage.vold.model.VvrManagerMXBean;
import com.oodrive.nuage.vold.model.VvrManagerTask;
import com.oodrive.nuage.vold.model.VvrTask;
import com.oodrive.nuage.vold.rest.generated.model.ExecState;
import com.oodrive.nuage.vold.rest.generated.model.Task;
import com.oodrive.nuage.vold.rest.generated.model.TaskList;
import com.oodrive.nuage.vold.rest.generated.resources.TaskResource;
import com.oodrive.nuage.vold.rest.generated.resources.VvrsTasksResource;
import com.oodrive.nuage.vold.rest.util.InputValidation;
import com.oodrive.nuage.vold.rest.util.ResourcePath;

/**
 * {@link VvrsTasksResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author pwehrle
 * 
 */
public final class VvrsTasksResourceJmxImpl extends AbstractTasksResource implements VvrsTasksResource {

    VvrsTasksResourceJmxImpl(final URI tasksResUri, final VvrsResourceJmxImpl rootResource, final URI parentUri) {
        super(tasksResUri, rootResource, parentUri);
    }

    @Override
    public final TaskList getAllTasks(final String ownerId) {

        final TaskList result = getObjectFactory().createTaskList();
        final List<Task> taskList = result.getTasks();

        final VvrManagerMXBean vvrManager = getRootResource().newVvrManagerProxy(
                InputValidation.getUuidFromString(ownerId));

        for (final VvrManagerTask task : vvrManager.getVvrManagerTasks()) {
            taskList.add(getTaskPojoFromMbeanProxy(task.getTaskId(), constructStatus(task),
                    constructResultReference(task)));
        }
        // get task for all the vvr
        final Set<ObjectName> vvrInstances = getRootResource().getAllVvrInstances(ownerId);

        for (final ObjectName currObjName : vvrInstances) {
            final VvrMXBean vvr = JMX.newMXBeanProxy(getRootResource().getConnection(), currObjName, VvrMXBean.class);
            for (final VvrTask task : vvr.getVvrTasks()) {
                final String resourcesPath = VvrTasksResourceJmxImpl.constructResourcesPath(task);
                // construct manually the result reference
                final String resultRef = UriBuilder.fromUri(getParentUri()).path(vvr.getUuid()).path(resourcesPath)
                        .path(task.getInfo().getTargetId()).build().toString();

                taskList.add(getTaskPojoFromMbeanProxy(task.getTaskId(), VvrTasksResourceJmxImpl.constructStatus(task),
                        resultRef));
            }
        }
        return result;
    }

    @Override
    public final TaskResource getTaskResource(final String ownerId, final String taskId) {
        final UUID taskUuid = InputValidation.getUuidFromString(taskId);

        final VvrManagerMXBean vvrManagerProxy = getRootResource().newVvrManagerProxy(
                InputValidation.getUuidFromString(ownerId));

        final VvrManagerTask task = vvrManagerProxy.getVvrManagerTask(taskUuid.toString());

        // Jersey will generate a 404 not found error
        if (task == null) {
            return null;
        }
        final DtxManagerMXBean dtxManager = createDtxProxy(ownerId);
        final TaskResourceJmxImpl result = new TaskResourceJmxImpl(task.getTaskId(), constructStatus(task),
                constructResultReference(task), this, dtxManager);

        ResourcePath.injectUriInfoContext(uriInfo, result);
        return result;
    }

    private String constructResultReference(final VvrManagerTask task) {

        if (task == null) {
            return "";
        }
        final String id = task.getInfo().getTargetId();

        // construct /storage/vvrs/<uuid vvr>
        if (id != null)
            return UriBuilder.fromUri(getParentUri()).path(id).build().toString();
        else {
            return "";
        }

    }

    private ExecState constructStatus(final VvrManagerTask task) {
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
