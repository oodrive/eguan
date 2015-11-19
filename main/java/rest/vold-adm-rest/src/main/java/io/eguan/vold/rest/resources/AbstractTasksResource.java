package io.eguan.vold.rest.resources;

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

import io.eguan.dtx.DtxManagerMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.vold.rest.generated.model.ExecState;
import io.eguan.vold.rest.generated.model.Task;
import io.eguan.vold.rest.util.InputValidation;

import java.net.URI;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.ws.rs.core.UriBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of functionality common to all Resources containing {@link Task}s.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public abstract class AbstractTasksResource extends AbstractResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTasksResource.class);

    /**
     * The base URI of this resource,
     */
    private final URI tasksResourceUri;

    /**
     * The base URI to construct the result URI
     */
    private final URI parentUri;

    /**
     * The VVrs resource (root)
     */
    private final VvrsResourceJmxImpl rootResource;

    /**
     * Common constructor.
     * 
     * @param tasksResourceUri
     *            the absolute URI of this resource.
     * @throws IllegalStateException
     *             if the given URI is not absolute
     * @throws NullPointerException
     *             if the given URI is <code>null</code>
     */
    protected AbstractTasksResource(final URI tasksResourceUri, final VvrsResourceJmxImpl rootResource,
            final URI parentUri) throws IllegalStateException, NullPointerException {
        if (!tasksResourceUri.isAbsolute()) {
            throw new IllegalStateException("Resource URI is not absolute");
        }
        this.tasksResourceUri = tasksResourceUri;
        this.rootResource = rootResource;
        this.parentUri = parentUri;
    }

    public URI getParentUri() {
        return parentUri;
    }

    public VvrsResourceJmxImpl getRootResource() {
        return rootResource;
    }

    @Override
    protected final URI getResourceUri() {
        return tasksResourceUri;
    }

    protected final DtxManagerMXBean createDtxProxy(final String ownerId) {
        final UUID ownerUuid = InputValidation.getUuidFromString(ownerId);
        final MBeanServerConnection jmxConnection = rootResource.getConnection();

        final DtxManagerMXBean dtxManager = JMX.newMXBeanProxy(jmxConnection,
                VvrObjectNameFactory.newDtxManagerObjectName(ownerUuid), DtxManagerMXBean.class);

        return dtxManager;
    }

    /**
     * construct URI from the provided taskId.
     * 
     * @param taskId
     *            a functional TaskId instance
     * @return the absolute {@link URI} of the new task handling the job
     */
    final URI constructTaskUri(final String TaskId) {
        final URI result = UriBuilder.fromUri(tasksResourceUri).path(TaskId).build();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Created new task; task URI=" + result);
        }
        return result;
    }

    /**
     * Construct a Task with a DtxTaskAdm
     * 
     * @param currTaskAdm
     *            a functional currTaskAdm instance
     * @param resultref
     *            contains the object ID result
     * @return the task which is created
     */
    static final Task getTaskPojoFromMbeanProxy(final String uuid, final ExecState state, final String resultref) {
        final Task result = getObjectFactory().createTask();
        result.setState(state);
        result.setResultRef(resultref);
        result.setUuid(uuid);
        return result;
    }
}
