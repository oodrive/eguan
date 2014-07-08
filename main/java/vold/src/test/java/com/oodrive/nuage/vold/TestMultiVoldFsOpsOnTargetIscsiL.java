package com.oodrive.nuage.vold;

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

import java.io.IOException;

import com.oodrive.nuage.utils.unix.UnixIScsiTarget;
import com.oodrive.nuage.utils.unix.UnixTarget;
import com.oodrive.nuage.vold.model.DeviceMXBean;

public class TestMultiVoldFsOpsOnTargetIscsiL extends TestMultiVoldFsOpsOnTargetAbstract {

    @Override
    protected final UnixTarget createTarget(final DeviceMXBean d, final int serverIndex, final int deviceIndex)
            throws IOException {
        return discoverTarget("127.0.0.1", getIscsiServerPort(serverIndex), d);
    }

    /**
     * Discover targets on a given address and port and build the Iscsi Target corresponding to a given device
     * 
     * @return UnixIScsiTarget which is created
     */
    private UnixIScsiTarget discoverTarget(final String address, final int port, final DeviceMXBean d)
            throws IOException {
        UnixIScsiTarget.sendTarget(address + ":" + port);
        return new UnixIScsiTarget(address, port, d.getIqn());
    }
}
