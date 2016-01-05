/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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
 * @file testsTools.cpp
 * @brief Tools for unit testings/source
 * @author j. caba
 */
#include "testsTools.h"
#include "testsMemoryTools.h"
#include "Logger.h"
#include "FileTools.h"
#include "Constants.h"
#include <vector>
#include <fstream>

static std::string parentDir;

void randomString(std::string& str, const int size, const int numberOfzeroToInsert) {
    char* pt = new char[size];
    for (int i = 0; i < size; i++) {
        pt[i] = (char) (::rand() % 256);
    }
    str.assign(pt, size);
    delete[] pt;
    pt = NULL;
    // insert zero randomly
    for (int i = 0; i < numberOfzeroToInsert; i++) {
        size_t index = ::rand() % size;
        str[index] = 0;
    }
}

std::string randomName(const size_t nbRamdomChar) {
    std::vector<char> valid = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    valid.shrink_to_fit();
    std::string randomPart;
    for (size_t i = 0; i < nbRamdomChar; i++) {
        randomPart += valid[rand() % valid.size()];
    }
    return randomPart;
}

void setParentDir(const std::string& dir) {
    parentDir = dir;
}

std::string getParentDir() {
    return parentDir;
}

std::string MKTMPNAME(const size_t nbRamdomChar) {
    std::string randomPart = randomName(nbRamdomChar);
    std::string dir(parentDir);
    std::string prefix(TEST_PREFIX);
    std::string name = dir + prefix + randomPart;
    return name;
}

namespace ibs {

using namespace std;

namespace errors {

SignalTranslator<SegmentationFaultException> g_objSegmentationFaultTranslator;
SignalTranslator<BusErrorException> g_objBusErrorTranslator;
SignalTranslator<AbortException> g_objAbortTranslator;
SignalTranslator<FloatingPointException> g_objFPETranslator;

ExceptionHandler g_objHandler;

static Logger_t rootLogger = ibs::Logger::getRootLogger();

bool ExceptionTracer::redTrace = true;

void ExceptionHandler::Handler() {
    try {
        // re-throw
        throw;
    }
    catch (SegmentationFaultException &) {
        LOG4IBS_ERROR(rootLogger, "Segmentation fault not treated in tests");
    }
    catch (AbortException &) {
        LOG4IBS_ERROR(rootLogger, "Abort not treated in tests");
    }
    LOG4IBS_ERROR(rootLogger, "EXIT PROGRAM");
    exit(-1);
}

}/* namespace errors */

std::string testsTools::InitConfig(const int n_ibp, const Type type, const bool doFalseRecordOption,
        const bool doRealRecordOption, const bool doEnableSyslog) {
    std::map<std::string, std::string> cfg;
    std::vector<string> ibpDirs;
    stringstream ssForPath;
    string cfgFile = MKTMPNAME() + "_CONFIG";

    // clear previous test run by cleaning parent dir ...
    FileTools::removeDirectory(getParentDir()); //ensure the directory is deleted first
    if (FileTools::createDirectory(getParentDir()) == false) {
        /* TODO: handle exception */
    }

    for (int i = 0; i < n_ibp; i++) {
        ibpDirs.emplace_back(MKTMPNAME() + "_IBP");
    }
    for (int i = 0; i < n_ibp; i++) {
        FileTools::removeDirectory(ibpDirs[i]); //ensure the directory is deleted first
        if (FileTools::createDirectory(ibpDirs[i]) == false) {
            /* TODO: handle exception */
        }
        ssForPath << ibpDirs[i];
        if (i + 1 != n_ibp)
            ssForPath << ",";
    }
    cfg[IBP_PATH] = ssForPath.str();
    cfg[IBP_GEN_PATH] = MKTMPNAME() + "_IBPGEN";
    FileTools::removeDirectory(cfg[IBP_GEN_PATH]); //ensure the directory is deleted first
    if (FileTools::createDirectory(cfg[IBP_GEN_PATH]) == false) {
        /* TODO: handle exception */
    }
    switch (type) {
        case WithHotData:
            cfg[HOT_DATA] = "yes";
            break;
        case OnlyColdData:
        default:
            cfg[HOT_DATA] = "no";
            break;
    }
    cfg[LOG_LEVEL] = "warn";
    cfg[IBS_UUID] = TESTUUID;
    cfg[IBS_OWNER] = OWNERUUID;
    if (doFalseRecordOption) {
        std::string recordFile(MKTMPNAME() + "_this_file_does_not_exist");
        cfg[RECORD] = recordFile;
    }
    if (doRealRecordOption) {
        std::string recordFile(MKTMPNAME() + "_this_is_a_valid_recorder_file");
        std::ofstream f(recordFile);
        f.close();
        cfg[RECORD] = recordFile;
    }
    if (doEnableSyslog) {
        cfg[LOG_LEVEL] = YES;
    }
    writeTestConfig(cfgFile, cfg);
    cfg.clear();
    ibpDirs.clear();
    return cfgFile;
}

void testsTools::writeTestConfig(const std::string& fname, const std::map<std::string, std::string>& _cfg) {
    std::ofstream f(fname);
    auto ite = _cfg.begin();
    while (ite != _cfg.end()) {
        f << ite->first << "=" << ite->second << endl;
        ++ite;
    }
    f.close();
}

} /* namespace ibs */
