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
 * @file testsIbsC.cpp
 */
#include "gtest/gtest.h"
#include "libibsc.h"
#include "FileTools.h"
#include "Logger.h"
#include "Constants.h"
#include <string>
#include <map>
#include <iostream>
#include <fstream>

#include <execinfo.h>
#include <signal.h>
#include <exception>

namespace ibs {

#define TEST_PREFIX "IBSC_"
#define TEST_SUFFIX "_TEST"

std::string MKTMPNAME() {
    char* toFree = tempnam(NULL, TEST_PREFIX); // tempnam does malloc memory
    std::string name;
    name.assign(toFree);
    ::free(toFree); // release memory and avoid leak
    toFree = NULL;
    return name + TEST_SUFFIX;
}

// ensure configuration of the logger is done once
static Logger_t logger = ibs::Logger::getLogger("testsIbsC");

// The exception handling back_trace like the following IBM article :
// http://www.ibm.com/developerworks/library/l-cppexcep/index.html

/* Class to register a signal handler and translate the signal to an C++ exception */
template<class SignalExceptionClass> class SignalTranslator {
    public:
        SignalTranslator() {
            signal(SignalExceptionClass::GetSignalNumber(), SignalHandler);
        }

        static void SignalHandler(int) {
            throw SignalExceptionClass();
        }
};

class ExceptionTracer {
    public:
        ExceptionTracer() {
            void * array[25];
            int nSize = backtrace(array, 25);
            char ** symbols = backtrace_symbols(array, nSize);

            for (int i = 0; i < nSize; i++) {
                std::cerr << symbols[i] << std::endl;
            }

            free(symbols);
        }
};

class SegmentationFaultException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGSEGV;
        }
};

class AbortException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGABRT;
        }
};

SignalTranslator<SegmentationFaultException> g_objSegmentationFaultTranslator;
SignalTranslator<AbortException> g_objAbortTranslator;

class ExceptionHandler {
    public:
        ExceptionHandler() {
            std::set_terminate(Handler);
        }

        static void Handler() {
            try {
                // re-throw
                throw;
            }
            catch (SegmentationFaultException &) {
                LOG4IBS_ERROR(logger, "Segmentation fault not treated in tests");
            }
            catch (AbortException &) {
                LOG4IBS_ERROR(logger, "Abort not treated in tests");
            }

            exit(-1);
        }
};

ExceptionHandler g_objHandler;

/* used to simplify multi thread test */
struct config {
        std::map<std::string, std::string> configMap;
        std::string cfgFile;
        std::string ibpGenPath;
        std::vector<std::string> ibpDirs;
        int nbDirs;
};

/* to synchronize threads before the start of useful work */
pthread_barrier_t threadBarrierSynchro;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

class testsIbsC: public ::testing::Test {
    protected:
        std::string ibpGenPath;
        std::string cfgFile;
        std::vector<std::string> ibpDirs;
        std::map<std::string, std::string> cfg;
        int n;
        char* cCfg;
        static const int dropWait = 2;
        static const int multiThreadTestWait = 15;

        static std::vector<std::string> possibleHotDataParams;
        static std::vector<std::string> possibleCompressionParams;

        testsIbsC() :
                ibpGenPath(), cfgFile(), ibpDirs(), cfg(), n(0), cCfg(NULL) {
        }

        virtual ~testsIbsC() {
        }

        static char* createConfigDirs(struct config& conf, const std::string& hotDataCfg,
                const std::string& compressionCfg) {
            return createConfigDirs(conf.configMap, conf.cfgFile, conf.ibpGenPath, conf.ibpDirs, conf.nbDirs,
                    hotDataCfg, compressionCfg);
        }

        static char* createConfigDirs(std::map<std::string, std::string>& configMap, std::string& newCfgFile,
                std::string& newIbpGenPath, std::vector<std::string>& newIbpDirs, int& nbDirs,
                const std::string& hotDataCfg, const std::string& compressionCfg) {
            char* configFileNameBuffer;
            std::stringstream ss;
            nbDirs = 5;
            newCfgFile = std::string(MKTMPNAME()) + "_CONFIG";
            newIbpGenPath = std::string(MKTMPNAME()) + "_IBPGEN";
            configMap.clear();
            configMap[IBS_UUID] = "ffd93fa8-5952-4943-a474-84bed12b0c9f";
            configMap[IBS_OWNER] = "ffd93fa8-5952-a474-4943-84bed12b0c9f";
            configMap[IBP_GEN_PATH] = newIbpGenPath;
            configMap[LOG_LEVEL] = "warn";
            configMap[HOT_DATA] = hotDataCfg;
            configMap[COMPRESSION] = compressionCfg;
            newIbpDirs.clear();
            for (int i = 0; i < nbDirs; i++) {
                newIbpDirs.push_back(std::string(MKTMPNAME()) + "_IBP");
            }
            mkdir(newIbpGenPath.c_str(), 0755);
            for (int i = 0; i < nbDirs; i++) {
                /* TODO: replace by an ASSERT to handle it */
                if (mkdir(newIbpDirs[i].c_str(), 0755) != 0) {
                    /* TODO: handle exception */
                }
                ss << newIbpDirs[i];
                if (i + 1 != nbDirs)
                    ss << ",";
            }
            configMap[IBP_PATH] = ss.str();
            writeTestConfig(newCfgFile, configMap);
            configFileNameBuffer = new char[newCfgFile.size() + 1];
            strcpy(configFileNameBuffer, newCfgFile.c_str());
            return configFileNameBuffer;
        }

        static void cleanConfigDirs(std::string& aCfgFile, std::vector<std::string>& theIbpDirs,
                std::string& theIbpGenPath, const int nbDirs) {
            if (remove(aCfgFile.c_str()) != 0) {
                FileTools::removeDirectory(aCfgFile);
            }
            for (int i = 0; i < nbDirs; i++) {
                if (remove(theIbpDirs[i].c_str()) != 0) {
                    FileTools::removeDirectory(theIbpDirs[i]);
                }
            }
            if (remove(theIbpGenPath.c_str()) != 0) {
                FileTools::removeDirectory(theIbpGenPath);
            }
        }

        virtual void SetUp() {
        }

        virtual void TearDown() {
        }

        static void writeTestConfig(std::string fname, std::map<std::string, std::string>& _cfg) {
            std::map<std::string, std::string>::iterator ite;
            std::ofstream f;
            f.open(fname.c_str());
            ite = _cfg.begin();
            while (ite != _cfg.end()) {
                f << ite->first << "=" << ite->second << std::endl;
                ite++;
            }
            f.close();
        }
        static void randomString(std::string& str, size_t size) {
            char* pt = new char[size];
            for (size_t i = 0; i < size; i++) {
                pt[i] = (char) 32 + (rand() % 256);
            }
            str.assign(pt, size);
            delete[] pt;
        }

        static void testClose(const int instance) {
            EXPECT_EQ(ibsDestroy(instance), 0);
            EXPECT_EQ(ibsDelete(instance), 0);
        }
        typedef void*(*threadFunction_t)(void*);

        struct threadParameters {
                char* configFileName;bool goOn;
        };
        typedef struct threadParameters threadParameters_t;

        static void setGoOn(threadParameters_t& params, bool value) {
            pthread_mutex_lock(&lock);
            params.goOn = value;
            pthread_mutex_unlock(&lock);
        }

        static bool getGoOn(const threadParameters_t& params) {
            bool goOn;
            pthread_mutex_lock(&lock);
            goOn = params.goOn;
            pthread_mutex_unlock(&lock);
            return goOn;
        }

        static void handleException(const int instance) {
            /* close */
            if (instance != 0)
                EXPECT_EQ(ibsDelete(instance), 0);
            pthread_exit((void*) -1);
        }

        static void ibsOpenClose(void* arg) {
            /* synchronize all threads */
            pthread_barrier_wait(&threadBarrierSynchro);
            /* and start work at the "same time" for all threads */
            threadParameters_t* param = reinterpret_cast<threadParameters_t*>(arg);
            ASSERT_TRUE(param != NULL);
            const char* cfgFileName = param->configFileName;
            ASSERT_TRUE(cfgFileName != NULL);
            bool continueLoop = getGoOn(*param);
            while (continueLoop) {
                int instance = 0;
                try {
                    /* open */
                    instance = ibsInit(cfgFileName);
                    EXPECT_GT(instance, 0);
                    EXPECT_EQ(ibsStart(instance), 0);
                    EXPECT_EQ(ibsStop(instance), 0);
                }
                catch (SegmentationFaultException& e) {
                    pthread_mutex_lock(&lock);
                    LOG4IBS_WARN(logger, "SegmentationFaultException");
                    pthread_mutex_unlock(&lock);
                    handleException(instance);
                }
                catch (AbortException& e) {
                    pthread_mutex_lock(&lock);
                    LOG4IBS_WARN(logger, "AbortException");
                    pthread_mutex_unlock(&lock);
                    handleException(instance);
                }
                /* close */
                EXPECT_EQ(ibsDelete(instance), 0);
                continueLoop = getGoOn(*param);
            }
        }

        static void* runInThread(void* arg) {
            ibsOpenClose(arg);
            /* not used, only to silent compiler */
            return NULL;
        }
};

std::vector<std::string> testsIbsC::possibleHotDataParams = { "no", "yes" };
std::vector<std::string> testsIbsC::possibleCompressionParams = { FRONT_COMPRESSION, BACK_COMPRESSION, "none" };

//FIXME divide hot and cold data tests
#if 0
TEST_F(testsIbsC, BasicTransaction) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            EXPECT_EQ(ibsStart(instance), 0);

            std::unique_ptr<std::string> key1(new std::string("key1"));
            std::string value1 = "value1";
            std::string fetchedValue;
            std::string key2 = "key2";
            std::string value2 = "value2";
            std::string key3 = "key3";
            std::string value3 = "value3";

            int transactionId1 = ibsCreateTransaction(instance);
            EXPECT_TRUE(transactionId1 > 0);
            int put1Res = ibsPutTransaction(instance, transactionId1, key1->data(), key1->size(), value1.data(),
                    value1.size());
            EXPECT_TRUE(put1Res == 0);
            // assign a different memory block for the key
            // to test errors issues due to release of memory block key of the key
            key1.reset(new std::string("key1"));
            int put2Res = ibsPutTransaction(instance, transactionId1, key2.data(), key2.size(), value2.data(),
                    value2.size());
            EXPECT_TRUE(put2Res == 0);
            int replace1Res = ibsReplaceTransaction(instance, transactionId1, key2.data(), key2.size(), key3.data(),
                    key3.size(), value3.data(), value3.size());
            EXPECT_TRUE(replace1Res == 0);

            size_t size = 4096;
            char* buffer = new char[size];
            size_t written = 0;
            // nothing applied until commit
            EXPECT_LT(ibsGet(instance, key1->data(), key1->size(), buffer, size, &written), 0);
            EXPECT_LT(ibsGet(instance, key2.data(), key2.size(), buffer, size, &written), 0);
            EXPECT_LT(ibsGet(instance, key3.data(), key3.size(), buffer, size, &written), 0);

            EXPECT_EQ(ibsCommitTransaction(instance, transactionId1), 0);

            // after commit only key and key3 shall exist
            EXPECT_GT(ibsGet(instance, key1->data(), key1->size(), buffer, size, &written), 0);
            EXPECT_EQ(written, value1.size());
            EXPECT_EQ(0, strncmp(buffer, value1.data(), value1.size()));
            EXPECT_LT(ibsGet(instance, key2.data(), key2.size(), buffer, size, &written), 0);

            EXPECT_GT(ibsGet(instance, key3.data(), key3.size(), buffer, size, &written), 0);
            EXPECT_EQ(written, value3.size());
            EXPECT_EQ(0, strncmp(buffer, value3.data(), value3.size()));

            // test creation of another transaction and rollback
            int transactionId2 = ibsCreateTransaction(instance);
            EXPECT_TRUE(transactionId2 > 0);
            bool wasPresentBefore = (ibsGet(instance, key2.data(), key2.size(), buffer, size, &written) == 0);
            EXPECT_FALSE(wasPresentBefore);
            put2Res = ibsPutTransaction(instance, transactionId1, key2.data(), key2.size(), value2.data(),
                    value2.size());

            EXPECT_TRUE(put2Res == 0);

            int put3Res = ibsPutTransaction(instance, transactionId1, key3.data(), key3.size(), value3.data(),
                    value3.size());
            EXPECT_TRUE(put3Res == IBS__KEY_ALREADY_ADDED);
            // nothing applied until commit
            EXPECT_LT(ibsGet(instance, key2.data(), key2.size(), buffer, size, &written), 0);

            EXPECT_EQ(ibsRollbackTransaction(instance, transactionId2), 0);
            // after rollback nothing shall change
            EXPECT_GT(ibsGet(instance, key1->data(), key1->size(), buffer, size, &written), 0);
            EXPECT_EQ(written, value1.size());
            EXPECT_EQ(0, strncmp(buffer, value1.data(), value1.size()));
            EXPECT_LT(ibsGet(instance, key2.data(), key2.size(), buffer, size, &written), 0);
            EXPECT_GT(ibsGet(instance, key3.data(), key3.size(), buffer, size, &written), 0);
            EXPECT_EQ(written, value3.size());
            EXPECT_EQ(0, strncmp(buffer, value3.data(), value3.size()));

            // a commit of a transaction is not possible after a rollback off this transaction
            EXPECT_LT(ibsCommitTransaction(instance, transactionId2), 0);

            EXPECT_EQ(ibsStop(instance), 0);
            testClose(instance);
            delete[] buffer;
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}
#endif

TEST_F(testsIbsC, InitAndDestroy) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            testClose(instance);
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, InitNull) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int instance = ibsCreate(NULL);
            EXPECT_EQ(instance, IBS__CONFIG_ERROR);
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, InitInEmpty) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int instance = ibsInit(cCfg);
            EXPECT_EQ(IBS__INIT_FROM_EMPTY_DIR, instance);
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, CreateInExisting) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            EXPECT_EQ(ibsDelete(instance), 0);
            instance = ibsCreate(cCfg);
            EXPECT_EQ(IBS__CREATE_IN_NON_EMPTY_DIR, instance);

            // Cleaning the directories content for TearDown() to delete them afterwards
            instance = ibsInit(cCfg);
            EXPECT_GT(instance, 0);
            testClose(instance);
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, putGetColdData) {
    auto hotDataParam = possibleHotDataParams[0];
    for (auto compressionParam : possibleCompressionParams) {
        LOG4IBS_WARN(logger,
                "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
        cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
        size_t size = 4096;
        int i;
        std::string randomData;
        std::string fetchedValue;
        char* buffer = new char[size];
        int instance = ibsCreate(cCfg);
        EXPECT_GT(instance, 0);

        int n = ibsStart(instance);
        EXPECT_EQ(n, 0);
        for (i = 0; i < 1000; i++) {
            size_t written = 0;
            std::stringstream key;
            randomString(randomData, size);
            key << "key" << i;
            EXPECT_EQ(ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size()), 0);
            EXPECT_GT(ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written), 0);
            EXPECT_EQ(written, size);
            EXPECT_EQ(0, strncmp(buffer, randomData.data(), size));
        }
        EXPECT_EQ(ibsStop(instance), 0);
        testClose(instance);
        delete[] buffer;
        cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
    }
}

TEST_F(testsIbsC, putGetHotDataFrontCompression) {
    auto hotDataParam = possibleHotDataParams[1];
    auto compressionParam = possibleCompressionParams[0];
    LOG4IBS_WARN(logger,
            "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
    cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
    size_t size = 4096;
    int i;
    std::string randomData;
    std::string fetchedValue;
    char* buffer = new char[size];
    int instance = ibsCreate(cCfg);
    EXPECT_GT(instance, 0);

    int n = ibsStart(instance);
    EXPECT_EQ(n, 0);
    for (i = 0; i < 1; i++) {
        size_t written = 0;
        std::stringstream key;
        randomString(randomData, size);
        key << "key" << i;

        int putRes = ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
        EXPECT_EQ(putRes, 0);
        int getRes = ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written);
        EXPECT_GT(getRes, 0);
        EXPECT_EQ(written, size);
        EXPECT_EQ(0, strncmp(buffer, randomData.data(), size));
    }
    EXPECT_EQ(ibsStop(instance), 0);
    testClose(instance);
    delete[] buffer;
    cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
}

TEST_F(testsIbsC, putGetHotDataBackCompression) {
    auto hotDataParam = possibleHotDataParams[1];
    auto compressionParam = possibleCompressionParams[1];
    LOG4IBS_WARN(logger,
            "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
    cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
    size_t size = 4096;
    int i;
    std::string randomData;
    std::string fetchedValue;
    char* buffer = new char[size];
    int instance = ibsCreate(cCfg);
    EXPECT_GT(instance, 0);

    int n = ibsStart(instance);
    EXPECT_EQ(n, 0);
    for (i = 0; i < 1; i++) {
        size_t written = 0;
        std::stringstream key;
        randomString(randomData, size);
        key << "key" << i;

        int putRes = ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
        EXPECT_EQ(putRes, 0);
        int getRes = ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written);
        EXPECT_GT(getRes, 0);
        EXPECT_EQ(written, size);
        EXPECT_EQ(0, strncmp(buffer, randomData.data(), size));
    }
    EXPECT_EQ(ibsStop(instance), 0);
    testClose(instance);
    delete[] buffer;
    cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
}

TEST_F(testsIbsC, putGetHotDataNoCompression) {
    auto hotDataParam = possibleHotDataParams[1];
    auto compressionParam = possibleCompressionParams[2];
    LOG4IBS_WARN(logger,
            "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
    cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
    size_t size = 4096;
    int i;
    std::string randomData;
    std::string fetchedValue;
    char* buffer = new char[size];
    int instance = ibsCreate(cCfg);
    EXPECT_GT(instance, 0);

    int n = ibsStart(instance);
    EXPECT_EQ(n, 0);
    for (i = 0; i < 1; i++) {
        size_t written = 0;
        std::stringstream key;
        randomString(randomData, size);
        key << "key" << i;

        int putRes = ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
        EXPECT_EQ(putRes, 0);
        int getRes = ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written);
        EXPECT_GT(getRes, 0);
        EXPECT_EQ(written, size);
        EXPECT_EQ(0, strncmp(buffer, randomData.data(), size));
    }
    EXPECT_EQ(ibsStop(instance), 0);
    testClose(instance);
    delete[] buffer;
    cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
}

TEST_F(testsIbsC, testError) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            int i;
            size_t size = 4096;
            char* buffer = new char[size];
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            EXPECT_EQ(ibsStart(instance), 0);
            for (i = 0; i < 5; i++) {
                size_t written;
                std::stringstream key;
                key << "key" << i;
                EXPECT_LT(ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written), 0);
            }
            EXPECT_EQ(ibsStop(instance), 0);
            testClose(instance);
            delete[] buffer;
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, simpleReplaceL) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            cfg[HOT_DATA] = "yes";
            writeTestConfig(cfgFile, cfg);
            int i;
            int n = 5;
            size_t size = 4096;
            srandom(time(NULL));
            std::string randomData;
            char* buffer = new char[size];
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            EXPECT_EQ(ibsStart(instance), 0);
            for (i = 0; i < n; i++) {
                size_t written;
                std::stringstream key1;
                std::stringstream key2;
                key1 << "key" << i;
                key2 << "key" << n + i;
                randomString(randomData, size);
                EXPECT_EQ(ibsPut(instance, key1.str().data(), key1.str().size(), randomData.data(), randomData.size()),
                        0);
                randomString(randomData, size);
                ibsReplace(instance, key1.str().data(), key1.str().size(), key2.str().data(), key2.str().size(),
                        randomData.data(), randomData.size());
                sleep(2);
                EXPECT_EQ(ibsGet(instance, key1.str().data(), key1.str().size(), buffer, size, &written),
                        IBS__KEY_NOT_FOUND);
                EXPECT_EQ(ibsGet(instance, key2.str().data(), key2.str().size(), buffer, size, &written), (int )size);
                EXPECT_EQ(strncmp(randomData.data(), buffer, size), 0);
            }
            EXPECT_EQ(ibsStop(instance), 0);
            testClose(instance);
            cfg[HOT_DATA] = "no";
            delete[] buffer;
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, testReplaceL) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            bool errorDetected = false;
            try {
                LOG4IBS_WARN(logger,
                        "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
                cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
                cfg[HOT_DATA] = "yes";
                writeTestConfig(cfgFile, cfg);
                size_t size = 4096;
                int i;
                std::string randomData;
                std::string fetchedValue;
                char* buffer = new char[size];
                int instance = ibsCreate(cCfg);
                EXPECT_GT(instance, 0);
                EXPECT_EQ(ibsStart(instance), 0);
                for (i = 0; i < 100; i++) {
                    std::stringstream key;
                    std::stringstream newkey;
                    randomString(randomData, size);
                    key << "key" << i;
                    ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
                    randomString(randomData, size);
                    newkey << "newkey" << i;
                    ibsReplace(instance, key.str().data(), key.str().size(), newkey.str().data(), newkey.str().size(),
                            randomData.data(), randomData.size());
                }
                sleep(dropWait);
                for (i = 0; i < 100; i++) {
                    size_t written;
                    std::stringstream key;
                    std::stringstream newkey;
                    key << "key" << i;
                    newkey << "newkey" << i;
                    EXPECT_GT(0, ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written));
                    EXPECT_LT(0, ibsGet(instance, newkey.str().data(), newkey.str().size(), buffer, size, &written));
                    EXPECT_EQ(written, size);
                    written = 0;
                }
                EXPECT_EQ(ibsStop(instance), 0);
                testClose(instance);
                cfg[HOT_DATA] = "no";
                delete[] buffer;
                cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
            }
            catch (SegmentationFaultException& e) {
                LOG4IBS_WARN(logger, "SegmentationFaultException");
                errorDetected = true;
            }
            EXPECT_EQ(errorDetected, false);
        }
    }
}

// FIXME: delete test failed surely because of delete in cold data not really delete data ...
#if 0
TEST_F(testsIbsC, simpleDeleteL) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='"
                    << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            cfg[HOT_DATA] = "yes";
            writeTestConfig(cfgFile, cfg);
            int i;
            int n = 5;
            size_t size = 4096;
            srandom(time(NULL));
            std::string randomData;
            char* buffer = new char[size];
            int instance = ibsCreate(cCfg);
            EXPECT_GT(instance, 0);
            EXPECT_EQ(ibsStart(instance), 0);
            for (i = 0; i < n; i++) {
                size_t written;
                std::stringstream key;
                key << "key" << i;
                randomString(randomData, size);
                ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
                ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
                ibsPut(instance, key.str().data(), key.str().size(), randomData.data(), randomData.size());
                EXPECT_EQ(ibsDel(instance, key.str().data(), key.str().size()), 0);
                sleep(dropWait);
                EXPECT_EQ(ibsGet(instance, key.str().data(), key.str().size(), buffer, size, &written),
                        IBS__KEY_NOT_FOUND);
            }
            EXPECT_EQ(ibsStop(instance), 0);
            testClose(instance);
            cfg[HOT_DATA] = "no";
            delete[] buffer;
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}
#endif

TEST_F(testsIbsC, OpenIbsMultiThreadL) {
    for (auto hotDataParam : possibleHotDataParams) {
        for (auto compressionParam : possibleCompressionParams) {
            LOG4IBS_WARN(logger,
                    "Test with " << HOT_DATA << "='" << hotDataParam << "' and " << COMPRESSION << "='" << compressionParam << "'");
            cCfg = createConfigDirs(cfg, cfgFile, ibpGenPath, ibpDirs, n, hotDataParam, compressionParam);
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";

            static const int NB_THREAD = sysconf(_SC_NPROCESSORS_ONLN);
            pthread_t thread[NB_THREAD];
            threadParameters_t threadParameters[NB_THREAD];
            struct config config[NB_THREAD];

            /* first create the configuration and directories */
            for (int i = 0; i < NB_THREAD; i++) {
                char* conf = createConfigDirs(config[i], "yes", compressionParam);
                ASSERT_TRUE(conf != NULL);
                int instance = ibsCreate(conf);
                EXPECT_GT(instance, 0);
                EXPECT_EQ(ibsDelete(instance), 0);
                threadParameters[i].configFileName = conf;
                threadParameters[i].goOn = true;
            }
            /* initialize the barrier */
            ASSERT_EQ(pthread_barrier_init(&threadBarrierSynchro, NULL, NB_THREAD), 0);
            /* then create the thread with the function passed as parameter */
            for (int i = 0; i < NB_THREAD; i++) {
                ASSERT_EQ(pthread_create(&thread[i], NULL, runInThread, &threadParameters[i]), 0);
            }
            sleep(multiThreadTestWait);
            /* stop thread loops using goOn flag
             * WE DON'T use pthread_cancel
             * as "libibs" use locks
             * and if a thread is canceled while a lock is hold
             * it will result in an infinite loop !!! */
            for (int i = 0; i < NB_THREAD; i++) {
                setGoOn(threadParameters[i], false);
            }
            /* before waiting for thread termination */
            for (int i = 0; i < NB_THREAD; i++) {
                pthread_join(thread[i], NULL);
            }
            /* then clean directories and check tasks run */
            for (int i = 0; i < NB_THREAD; i++) {
                struct config& conf = config[i];
                ASSERT_FALSE(threadParameters[i].goOn);
                int instance = ibsInit(conf.cfgFile.c_str());
                testClose(instance);
                cleanConfigDirs(conf.cfgFile, conf.ibpDirs, conf.ibpGenPath, conf.nbDirs);
            }
            cleanConfigDirs(cfgFile, ibpDirs, ibpGenPath, n);
        }
    }
}

TEST_F(testsIbsC, DummyTest) {
// empty test, to detect a unexpected crash
// while multi thread access on IBS <-> ID map test (OpenIbsMultiThread)
}

} /* namespace ibs */
