package com.oodrive.nuage.ibs;

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

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Create a IBS for all the tests of the class. Created to speed up tests that does not need to start with an empty IBS.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public abstract class TestIbsClass extends TestIbs {

    private static IbsInitHelper ibsInitHelperLevelDB;
    private static IbsInitHelper ibsInitHelperFs;
    private static IbsInitHelper ibsInitHelperFake;

    /** IBS identifier for the current test */
    static Ibs ibs = null;

    private final IbsType ibsType;

    public TestIbsClass(final IbsType ibsType) {
        super();
        this.ibsType = ibsType;
    }

    @BeforeClass
    public static void initIbs() throws IOException {
        ibsInitHelperLevelDB = new IbsInitHelper();
        ibsInitHelperLevelDB.initIbs(IbsType.LEVELDB, null);

        ibsInitHelperFs = new IbsInitHelper();
        ibsInitHelperFs.initIbs(IbsType.FS, null);

        ibsInitHelperFake = new IbsInitHelper();
        ibsInitHelperFake.initIbs(IbsType.FAKE, null);

        // Use LevelDB by default
        ibs = ibsInitHelperLevelDB.getIbs();
    }

    @AfterClass
    public static void finiIbs() throws Exception {
        ibs = null;

        try {
            ibsInitHelperLevelDB.finiIbs();
        }
        finally {
            ibsInitHelperLevelDB = null;
        }
        try {
            ibsInitHelperFs.finiIbs();
        }
        finally {
            ibsInitHelperFs = null;
        }
        try {
            ibsInitHelperFake.finiIbs();
        }
        finally {
            ibsInitHelperFake = null;
        }
    }

    /**
     * Starts all Ibs.
     */
    protected final static void doStartIbs() {
        ibsInitHelperLevelDB.getIbs().start();
        ibsInitHelperFs.getIbs().start();
        ibsInitHelperFake.getIbs().start();
    }

    /**
     * Stops all Ibs.
     */
    protected final static void doStopIbs() {
        try {
            try {
                ibsInitHelperLevelDB.getIbs().stop();
            }
            finally {
                ibsInitHelperFs.getIbs().stop();
            }
        }
        finally {
            ibsInitHelperFake.getIbs().stop();
        }
    }

    @Before
    public void selectIbs() {
        if (ibsType == IbsType.LEVELDB) {
            ibs = ibsInitHelperLevelDB.getIbs();
        }
        else if (ibsType == IbsType.FS) {
            ibs = ibsInitHelperFs.getIbs();
        }
        else if (ibsType == IbsType.FAKE) {
            ibs = ibsInitHelperFake.getIbs();
        }
        else {
            throw new AssertionError(ibsType);
        }
    }
}
