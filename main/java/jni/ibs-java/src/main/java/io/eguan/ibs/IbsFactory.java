package io.eguan.ibs;

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

import java.io.File;

/**
 * Builds and opens {@link Ibs}.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
public final class IbsFactory {

    private static final IbsType CURRENT_DEFAULT_IBS_TYPE = IbsType.LEVELDB;

    /** Default implementation of the {@link Ibs} */
    public static final IbsType DEFAULT_IBS_TYPE;

    private static final String DEFAULT_IBS_IMPL_PROP = "io.eguan.ibs.default";

    static {
        final String typeStr = System.getProperty(DEFAULT_IBS_IMPL_PROP, CURRENT_DEFAULT_IBS_TYPE.name());
        IbsType typeSelected = CURRENT_DEFAULT_IBS_TYPE;
        for (final IbsType type : IbsType.values()) {
            if (typeStr.equalsIgnoreCase(type.name())) {
                typeSelected = type;
            }
        }
        DEFAULT_IBS_TYPE = typeSelected;
    }

    /**
     * No instance.
     */
    private IbsFactory() {
        throw new AssertionError();
    }

    /**
     * Create a new IBS.
     * 
     * @param path
     *            path leading to the IBS configuration file.
     * @return a new opened IBS
     * @throws IbsIOException
     *             if the IBS initialization fails
     */
    public static final Ibs createIbs(final File path) throws IbsException {
        return createIbs(path, path.getName().startsWith(Ibs.UNIT_TEST_IBS_HEADER) ? IbsType.FAKE : DEFAULT_IBS_TYPE);
    }

    /**
     * Create a new IBS.
     * 
     * @param path
     *            path leading to the IBS configuration file.
     * @param ibsType
     *            type of {@link Ibs}
     * @return a new opened IBS
     * @throws IbsIOException
     *             if the IBS initialization fails
     */
    public static final Ibs createIbs(final File path, final IbsType ibsType) throws IbsException {
        switch (ibsType) {
        case LEVELDB: {
            final String ibsPathAbs = path.getAbsolutePath();
            final int retval = IbsLevelDB.ibsCreate(ibsPathAbs);
            if (retval < 0) {
                throw new IbsException(ibsPathAbs, IbsErrorCode.valueOf(retval));
            }
            return new IbsLevelDB(ibsPathAbs, retval);
        }
        case FS:
            return IbsFilesDB.createIbs(path);
        case FAKE:
            return IbsFake.createIbs(path.getName());

        default:
            throw new AssertionError(ibsType);
        }
    }

    /**
     * Opens an existing IBS.
     * 
     * @param path
     *            path leading to the IBS configuration file.
     * @return a new opened IBS
     * @throws IbsIOException
     *             if the IBS initialization fails
     */
    public static final Ibs openIbs(final File path) throws IbsException {
        return openIbs(path, path.getName().startsWith(Ibs.UNIT_TEST_IBS_HEADER) ? IbsType.FAKE : DEFAULT_IBS_TYPE);
    }

    /**
     * Opens an existing IBS.
     * 
     * @param path
     *            path leading to the IBS configuration file.
     * @param ibsType
     *            type of {@link Ibs}
     * @return a new opened IBS
     * @throws IbsIOException
     *             if the IBS initialization fails
     */
    public static final Ibs openIbs(final File path, final IbsType ibsType) throws IbsException {
        switch (ibsType) {
        case LEVELDB: {
            final String ibsPathAbs = path.getAbsolutePath();
            final int retval = IbsLevelDB.ibsInit(ibsPathAbs);
            if (retval < 0) {
                throw new IbsException(ibsPathAbs, IbsErrorCode.valueOf(retval));
            }
            return new IbsLevelDB(ibsPathAbs, retval);
        }
        case FS:
            return IbsFilesDB.openIbs(path);
        case FAKE:
            return IbsFake.openIbs(path.getName());

        default:
            throw new AssertionError(ibsType);
        }
    }

}
