package io.eguan.vvr.persistence.repository;

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

import io.eguan.nrs.NrsFile;
import io.eguan.vvr.persistence.repository.NrsDevice;
import io.eguan.vvr.repository.core.api.TestDeviceIoAbstract;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Unit tests to read and write in {@link NrsFile} with NrsFileBlock.
 *
 * @author oodrive
 * @author llambert
 *
 */
public final class TestDeviceIoBlocks extends TestDeviceIoAbstract {

    public TestDeviceIoBlocks() {
        super(true);
    }

    @BeforeClass
    public static final void enableNrsFileBlock() {
        NrsDevice.NRS_BLOCK_FILE_ENABLED = true;
    }

    @AfterClass
    public static final void disableNrsFileBlock() {
        NrsDevice.NRS_BLOCK_FILE_ENABLED = false;
    }
}
