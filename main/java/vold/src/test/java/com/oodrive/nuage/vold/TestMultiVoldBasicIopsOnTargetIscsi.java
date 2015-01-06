package com.oodrive.nuage.vold;

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

import com.oodrive.nuage.iscsisrv.InitiatorClientBasicIops;

public class TestMultiVoldBasicIopsOnTargetIscsi extends TestMultiVoldBasicIopsOnTargetAbstract {

    public TestMultiVoldBasicIopsOnTargetIscsi(final int blockSize, final int numBlocks) {
        super(blockSize, numBlocks);
    }

    @Override
    public InitiatorClientBasicIops createClient(final int serverIndex) {
        // jSCSI
        return new InitiatorClientBasicIops("/jscsi.xsd",
                TestMultiVoldBasicIopsOnTargetIscsi.class.getResource("/jscsi" + (serverIndex + 1) + ".xml"));
    }
}
