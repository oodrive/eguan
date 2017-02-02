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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import io.eguan.vold.rest.generated.model.Snapshot;
import io.eguan.vold.rest.generated.model.SnapshotList;

import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.MediaType;

import org.junit.Test;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public final class TestSnapshotsResourceL extends AbstractVvrResourceTest {

    private static final int NUMBER_OF_SNAPSHOTS_TO_CREATE = 10;

    public TestSnapshotsResourceL(final MediaType runContentType, final MediaType runAcceptType) {
        super(runContentType, runAcceptType);
    }

    @Test
    public final void testGetSnapshots() throws TimeoutException {

        final WebResource vvrResource = getVvrResource();

        final WebResource target = vvrResource.path("snapshots");

        // gets initial list
        final SnapshotList snapshotList = prebuildRequest(target, null).get(SnapshotList.class);
        assertNotNull(snapshotList);

        final List<Snapshot> snapshots = snapshotList.getSnapshots();
        assertNotNull(snapshots);
        assertFalse(snapshots.isEmpty());
        assertEquals(1, snapshots.size());

        final MultivaluedMapImpl createDevParams = new MultivaluedMapImpl();
        createDevParams.add("name", "testSnapDevice");
        createDevParams.add("size", DEFAULT_DEVICE_SIZE);

        final WebResource targetDevRes = createDevice(vvrResource.path("root"), createDevParams);

        for (int i = 1; i < NUMBER_OF_SNAPSHOTS_TO_CREATE; i++) {

            final MultivaluedMapImpl createQueryParams = new MultivaluedMapImpl();
            createQueryParams.add("name", TestSnapshotsResourceL.class.getSimpleName() + " " + i);

            assertNotNull(createSnapshot(targetDevRes, createQueryParams));

            final SnapshotList modifiedResult = prebuildRequest(target, null).get(SnapshotList.class);
            assertNotNull(modifiedResult);

            final List<Snapshot> modSnapList = modifiedResult.getSnapshots();
            assertNotNull(modSnapList);

            assertEquals(i + 1, modSnapList.size());

        }

    }
}
