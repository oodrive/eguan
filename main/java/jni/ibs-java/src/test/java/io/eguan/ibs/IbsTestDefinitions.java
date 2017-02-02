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

/**
 * Definitions for the IBS tests.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class IbsTestDefinitions {

    /** Temporary files and directories prefix */
    static final String TEMP_PREFIX = "ibs-java-tst";
    /** Temporary files suffix */
    static final String TEMP_SUFFIX = ".tmp";

    /** Key in config file for the IBPGEN directory */
    static final String CONF_IBPGEN = "ibpgen_path=";
    /** Key in config file for the IBP directory */
    static final String CONF_IBP = "ibp_path=";
    /** Enable hot data support */
    static final String CONF_HOTDATA = "hotdata=yes";
    /** Disable hot data support */
    static final String CONF_HOTDATA_OFF = "hotdata=no";
    /** No compression */
    static final String COMPRESSION_OFF = "compression=no";
    /** Compression in foreground (before input in IbpGen */
    static final String COMPRESSION_FRONT = "compression=front";
    /** Compression in background (before input in Ibp */
    static final String COMPRESSION_BACK = "compression=back";
    /** Keyword for the compression */
    static final String COMPRESSION_KEYWORD = "compression=";
    /** IBS UUID */
    static final String CONF_UUID = "uuid=bd2bc1d3-4288-4c13-9641-bba7fcce3358";
    /** IBS alternate UUID */
    static final String CONF_UUID_ALT = "uuid=bd2bc1d3-4288-4c13-9641-bba7fcce3354";
    /** IBS owner UUID */
    static final String CONF_OWNER = "owner=f58791ef-d88f-47b1-a184-e6188351ba19";
    /** IBS alternate owner UUID */
    static final String CONF_OWNER_ALT = "owner=f58791ef-d88f-47b1-a184-e6188351ba18";
    /** IBS Debug level */
    static final String CONF_DEBUG_LEVEL = "loglevel=off";
}
