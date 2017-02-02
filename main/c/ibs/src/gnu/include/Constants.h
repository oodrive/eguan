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
#ifndef CONSTANTS_H_
#define CONSTANTS_H_

/* Configuration parameters
 * TODO: don't forget to authorize new key by adding to allowedKeys
 * in Configurator::isKnownConfigurationKey method.
 */
#define HOT_DATA                        "hotdata"
#define IBP_GEN_PATH                    "ibpgen_path"
#define IBP_PATH                        "ibp_path"
#define IBS_UUID                        "uuid"
#define IBS_OWNER                       "owner"
#define IBP_ID                          "id"
#define IBP_NB                          "ibp_number"
#define LOG_LEVEL                       "loglevel"
#define SYSLOG                          "syslog"
#define BUFFER_ROTATION_THRESHOLD       "buffer_rotation_threshold"
#define BUFFER_ROTATION_DELAY           "buffer_rotation_delay"
#define COMPRESSION                     "compression"
#define RECORD                          "record_execution"
#define DUMP_AT_STOP_BEST_EFFORT_DELAY  "dump_at_stop_best_effort_delay"
#define INDICATED_RAM_SIZE              "indicated_ram_size"

/* IbpGen ldb buffer write delay parameters */
#define BUFFER_WRITE_DELAY_THRESHOLD    "buffer_write_delay_threshold"
#define BUFFER_WRITE_DELAY_LEVEL_SIZE   "buffer_write_delay_level_size"
#define BUFFER_WRITE_DELAY_INCR_MS      "buffer_write_delay_increment_ms"

/* compression value */
#define FRONT_COMPRESSION               "front"
#define BACK_COMPRESSION                "back"

/* levelDB advanced parameters, be careful !!!  */
#define LDB_BLOCK_SIZE                   "ldb_block_size"
#define LDB_BLOCK_RESTART_INVERVAL       "ldb_block_restart_interval"
#define LDB_DISABLE_BACKGROUND_COMPACTION_FOR_IBPGEN "ldb_disable_background_compaction_for_ibpgen"
/* Note: default values are those define within leveldb itself */

/* These constant are set "arbitrary" based on performance bench and may change */

/* Number of minimum default databases used for hot data directory to keep */
#define PERSIST_LIMIT                   4

/* Number of pass to check and persist data at dump */
#define PERSIST_RETRY                   2

#define SECOND_TO_MICROSECOND           1000000L

// This delay is the delay the buffering persist thread waits before
// to continue it's watch of hot data for persisting data in cold data.
// If the delay is too short for example 1000 micro second
// RestartHotData unit test has shown to fail.
// In the ideal the thread should wake after
// a new buffer is created.
#define PERSIST_WAIT_STEP               2000L /* 2 milliseconds (converted in micro seconds) */

#define DUMP_AT_STOP_BEST_EFFORT_DELAY_DEF    5   /* default delay in seconds to dump the bases at stop */

#define LDB_BLOCK_RESTART_INVERVAL_DEF  16
#define BUFFER_ROTATION_THRESHOLD_DEF   65536                 /* in number of writes */
#define BUFFER_ROTATION_DELAY_DEF       30                    /* in seconds */
#define BUFFER_ROTATION_WAIT_STEP       SECOND_TO_MICROSECOND /* in micro seconds */
#define BLOCK_SIZE_DEF                  4096                  /* 4 KiB block size */

#define RAM_FRACTION_DEF                 0.65
#define CACHE_MAX_VALUE                  1<<30
#define CACHE_MIN_VALUE                  32<<20
#define WRITE_BUFFER_SIZE_FOR_IBP_DEF    128<<20
#define READ_CACHE_SIZE_FOR_IBP_DEF      64<<20

#define BUFFER_WRITE_DELAY_THRESHOLD_DEF      15
#define BUFFER_WRITE_DELAY_LEVEL_DEF          5
#define BUFFER_WRITE_DELAY_INCR_DEF           10000

/* Miscellaneous */
#define YES                             "yes"
#define CONFIG_FILE                     "ibs.conf"
#define SIGNATURE_FILE                  "ibpid.conf"

#endif /* CONSTANTS_H_ */
