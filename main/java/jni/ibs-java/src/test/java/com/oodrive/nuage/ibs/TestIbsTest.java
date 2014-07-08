package com.oodrive.nuage.ibs;

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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;

/**
 * Create a new IBS for each test.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public abstract class TestIbsTest extends TestIbs {
    private IbsInitHelper ibsInitHelper;

    File tempFileConfig = null;
    /** IBPGEN directory */
    Path tempDirIbpgen = null;
    /** IBP directory */
    Path tempDirIbp = null;
    /** IBS identifier */
    Ibs ibs = null;

    /** <code>true</code> to enable ibsType Ibs */
    protected final IbsType ibsType;
    /** Compression mode (no, back, front) or <code>null</code> */
    private final String compression;

    protected TestIbsTest() {
        this(IbsFactory.DEFAULT_IBS_TYPE, null);
    }

    protected TestIbsTest(final IbsType ibsType, final String compression) {
        super();
        this.ibsType = ibsType;
        this.compression = compression;
    }

    @Before
    public void initIbs() throws IOException {
        ibsInitHelper = new IbsInitHelper();
        ibsInitHelper.initIbs(ibsType, compression);
        tempFileConfig = ibsInitHelper.getTempFileConfig();
        tempDirIbpgen = ibsInitHelper.getTempDirIbpgen();
        tempDirIbp = ibsInitHelper.getTempDirIbp();
        ibs = ibsInitHelper.getIbs();
    }

    @After
    public void finiIbs() throws Exception {
        try {
            ibsInitHelper.finiIbs();
        }
        finally {
            ibsInitHelper = null;
            tempFileConfig = null;
            tempDirIbpgen = null;
            tempDirIbp = null;
            ibs = null;
        }
    }
}
