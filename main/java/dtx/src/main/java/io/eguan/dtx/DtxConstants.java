package io.eguan.dtx;

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

/**
 * Class holding constants common to all DTX classes.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class DtxConstants {

    /**
     * The default value for last transaction counters if no transaction has been registered with them.
     */
    public static final long DEFAULT_LAST_TX_VALUE = -1;

    /**
     * The default value for the task timestamp, if no timestamp has been registered.
     */
    public static final long DEFAULT_TIMESTAMP_VALUE = -1;

    /**
     * Extension for journal files.
     */
    public static final String JOURNAL_FILE_EXTENSION = ".jrnl";

    /**
     * Default prefix applied to any journal file.
     */
    public static final String DEFAULT_JOURNAL_FILE_PREFIX = "dtx_";

    private DtxConstants() {
        throw new AssertionError("Not instantiable");
    }
}
