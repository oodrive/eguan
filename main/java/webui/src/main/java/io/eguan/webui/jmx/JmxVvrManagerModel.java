package io.eguan.webui.jmx;

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

import io.eguan.vold.model.VvrManagerMXBean;
import io.eguan.vold.model.VvrObjectNameFactory;
import io.eguan.webui.model.VvrManagerModel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JmxVvrManagerModel implements VvrManagerModel {

    private final VvrManagerMXBean vvrManagerMxBean;
    static final String MB_VVR_KEY = ",vvr=";
    private final MBeanServerConnection jmxConnection;
    private final UUID ownerUuid;

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxVvrManagerModel.class);

    JmxVvrManagerModel(final MBeanServerConnection jmxConnection, final UUID ownerUuid) {
        final ObjectName vvrManagerObjectName = VvrObjectNameFactory.newVvrManagerObjectName(ownerUuid);
        this.vvrManagerMxBean = JMX.newMXBeanProxy(jmxConnection, vvrManagerObjectName, VvrManagerMXBean.class, false);
        this.jmxConnection = jmxConnection;
        this.ownerUuid = ownerUuid;
    }

    @Override
    public final UUID getItemUuid() {
        return null;
    }

    @Override
    public final Set<UUID> getVvrs() {

        Set<ObjectName> vvrInstances;
        final HashSet<UUID> vvrUuid = new HashSet<>();
        try {
            vvrInstances = jmxConnection.queryNames(VvrObjectNameFactory.newVvrQueryListObjectName(ownerUuid),
                    Query.eq(Query.attr("OwnerUuid"), Query.value(ownerUuid.toString())));
        }

        catch (final IOException e) {
            LOGGER.error("Exception querying VVRs", e);
            throw new IllegalArgumentException();
        }
        if (vvrInstances.isEmpty()) {
            LOGGER.debug("No VVR");
        }
        for (final ObjectName vvrObjectName : vvrInstances) {
            vvrUuid.add(JmxHandler.getVvrUuid(vvrObjectName));
        }
        return vvrUuid;
    }

    @Override
    public final void createVvr(final String name, final String description) throws Exception {
        vvrManagerMxBean.createVvrNoWait(name, description);
    }

    @Override
    public final void deleteVvr(final UUID uuid) {
        vvrManagerMxBean.delete(uuid.toString());
    }
}
