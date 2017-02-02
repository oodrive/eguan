package io.eguan.vvr.remote;

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

import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;

import java.util.UUID;

/**
 * Resource manager context for the VVR.
 * 
 * @author oodrive
 * @author jmcaba
 * 
 */
public class VvrDtxRmContext extends DtxResourceManagerContext {

    private final RemoteOperation operation;

    protected VvrDtxRmContext(final UUID resourceManagerId, final RemoteOperation operation)
            throws NullPointerException {
        super(resourceManagerId);
        this.operation = operation;
    }

    public final RemoteOperation getOperation() {
        return operation;
    }
}
