package io.eguan.dtx;

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

import io.eguan.dtx.DtxResourceManager;

import javax.transaction.xa.XAException;

/**
 * Extends {@link TestDtxResourceManager} on a mock implementation behaving exactly the way a complete implementation
 * has to.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestMockDtxResourceManager extends TestDtxResourceManager {

    @Override
    protected final DtxResourceManager getResourceManagerInstance() throws XAException {
        return DtxDummyRmFactory.newResMgrThatDoesEverythingRight(null);
    }

    @Override
    protected final byte[] getPayload() {
        return DtxDummyRmFactory.DEFAULT_PAYLOAD;
    }

}
