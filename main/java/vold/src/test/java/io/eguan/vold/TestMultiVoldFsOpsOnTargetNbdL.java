package io.eguan.vold;

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

import io.eguan.utils.unix.UnixNbdTarget;
import io.eguan.vold.model.DeviceMXBean;

import java.io.IOException;

import org.junit.AfterClass;

public class TestMultiVoldFsOpsOnTargetNbdL extends TestMultiVoldFsOpsOnTargetAbstract {

    @AfterClass
    public static final void killNbdClient() throws IOException {
        UnixNbdTarget.killAll();
    }

    @Override
    public UnixNbdTarget createTarget(final DeviceMXBean d1, final int serverIndex, final int deviceIndex)
            throws IOException {
        return new UnixNbdTarget("127.0.0.1", getNbdServerPort(serverIndex), d1.getName(), deviceIndex);
    }
}
