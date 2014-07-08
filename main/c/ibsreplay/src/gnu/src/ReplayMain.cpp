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
/**
 * @file ReplayMain.cpp
 * @brief IBS Execution Replay main
 * @author j. caba
 */
#include "ReplayMain.h"
#include "FileTools.h"
#include <getopt.h>

using namespace ibs;
using namespace ibs::replay;

void printWelcome() {
    for (const std::string& txt : welcome) {
        std::cout << txt << std::endl;
    }
}

void printUsage(const std::string& progname) {
    std::cout << "Usage: " << progname;
    std::cout << " --recordFile <FILE1> [ --verbose <off|error|warn|info|debug|trace> ] [ --configFile <FILE2> ]";
    std::cout << std::endl;
    std::cout << "The indicated FILE1 and FILE2 must exist and be valid" << std::endl;
}

bool parseArgs(const int argc, const char* argv[], std::string& recordFile, std::string& configFile) {
    bool isValid = false;

    int c;
    bool loop = true;
    while (loop) {
        static struct option long_options[] = { { "recordFile", required_argument, 0, 'r' }, { "configFile",
        required_argument, 0, 'c' }, { "verbose", required_argument, 0, 'v' }, { 0, 0, 0, 0 } };
        int option_index = 0;
        c = getopt_long(argc, static_cast<char* const *>(static_cast<void*>(argv)), "rcv:", long_options,
                &option_index);

        /* Detect the end of the options. */
        if (c == -1)
            break;

        switch (c) {
            case 'r': {
                recordFile = optarg;
                isValid = ibs::FileTools::isFile(recordFile) && ibs::FileTools::isReadable(recordFile);
                if (!isValid) {
                    loop = false;
                }
                continue;
                break; // break for switch only to silent compiler
            }
            case 'c': {
                configFile = optarg;
                isValid = FileTools::isFile(configFile) && FileTools::isReadable(configFile);
                if (!isValid) {
                    loop = false;
                }
                continue;
                break; // break for switch only to silent compiler
            }
            case 'v': {
                std::string strOptarg(optarg);
                if ((strOptarg == std::string("off"))   || (strOptarg == std::string("error"))
                 || (strOptarg == std::string("warn"))  || (strOptarg == std::string("info"))
                 || (strOptarg == std::string("debug")) || (strOptarg == std::string("trace"))) {
                    REPLAY_LOG_LEVEL = strOptarg;
                }
                else {
                    REPLAY_LOG_LEVEL = "debug";
                }
                continue;
                break; // break for switch only to silent compiler
            }
            case '?':
                /* usage will be printed when isValid=false */
                break;

            default:
                break;
        }
    }
    return isValid;
}

static Logger_t mainLogger = Logger::getLogger("mainLogger");

int ibsreplay_main_implementation(const int argc, const char* argv[]) {
    std::string recordFile;
    std::string configFile;
    printWelcome();
    if (!parseArgs(argc, argv, recordFile, configFile)) {
        return EXIT_FAILURE;
    }
    ibs::Logger::setLevel (REPLAY_LOG_LEVEL);
    LOG4IBS_TRACE(mainLogger, "LOG LEVEL [on]");
    LOG4IBS_WARN(mainLogger, "LOG LEVEL [on]");
    LOG4IBS_ERROR(mainLogger, "LOG LEVEL [on]");
    LOG4IBS_INFO(mainLogger, "LOG LEVEL [on]");
    LOG4IBS_DEBUG(mainLogger, "LOG LEVEL [on]");
    RecordFileReader reader(recordFile);
    return reader.replay(configFile);
}

int ibsreplay_main(const int argc, const char* argv[]) {
    int resultCode = ibsreplay_main_implementation(argc, argv);
    ibs::Logger::setLevel("trace");
    if (resultCode == EXIT_SUCCESS) {
        LOG4IBS_INFO(mainLogger, "Replay ended successfully.");
    }
    else if (resultCode == EXIT_FAILURE) {
        LOG4IBS_ERROR(mainLogger, "Replay ended with error.");
        printUsage(argv[0]);
    }
    else {
        LOG4IBS_ERROR(mainLogger, "Replay ended with unknown error.");
        printUsage(argv[0]);
    }
    ibs::Logger::setLevel("off");
    return resultCode;
}

int main(const int argc, const char* argv[]) {
    return ibsreplay_main(argc, argv);
}
