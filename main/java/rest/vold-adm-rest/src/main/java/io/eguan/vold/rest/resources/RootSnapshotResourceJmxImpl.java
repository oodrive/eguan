package io.eguan.vold.rest.resources;

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

import io.eguan.vold.model.SnapshotMXBean;
import io.eguan.vold.rest.generated.resources.RootSnapshotResource;

import java.net.URI;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * {@link RootSnapshotResource} implementation for JMX backend.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class RootSnapshotResourceJmxImpl extends SnapshotResourceJmxImpl implements RootSnapshotResource {

    private final URI rootSnapResourceUri;

    public RootSnapshotResourceJmxImpl(final SnapshotMXBean snapshotProxy, final VvrResourceJmxImpl parentResource,
            final URI resourceUri) {
        super(snapshotProxy, parentResource, resourceUri);
        this.rootSnapResourceUri = resourceUri;
    }

    @Override
    public final Response deleteSnapshot(final String ownerId) {
        return Response.status(Status.FORBIDDEN).build();
    }

    @Override
    protected final URI getResourceUri() {
        return rootSnapResourceUri;
    }

}
