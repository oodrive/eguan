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
 * @file testsIbsPut.cpp
 * @author j. caba
 */
#include "testsTools.h"
#include "Controller.h"
#include "IbsHandler.h"
#include "ConfigFileReader.h"
#include "Logger.h"
#include <chrono>

using namespace std;

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("testsIbsPut");

class testsIbsPut: public ::testing::Test {
    protected:
        static const int nbIbp = 5;
        static constexpr int c4K = 4 << 10; // 4KiB in bytes.
        static constexpr int blockSize = c4K;
        static const int nbGetCheckIteration = 2;
        static constexpr uint64_t maxPutTime = 2 * 1000; //  maximum time (in microsecond) allowed for a Put operation (2 ms)

        testsIbsPut() {
        }

        virtual ~testsIbsPut() {
        }

        virtual void SetUp() {
        }

        virtual void TearDown() {
        }

        static void putGetData(const uint64_t nbBlockToPut, bool printStats);
        static void putGetDataTx(const uint64_t nbBlockToPut, bool printStats);

        template<typename FunctionType>
        static void putGetDataTemplate(const ulong nbBlockToPut, FunctionType putFunction, bool printStats);
};

StatusCode putTest(AbstractController* ibs, const string& key, const string& value) {
    return ibs->put(key, value);
}

StatusCode putTestTx(AbstractController* ibs, const string& key, const string& value) {
    std::string transactionId = ibs->createTransaction();
    ibs->put(transactionId, key, value);
    return ibs->commitTransaction(transactionId);
}

template<typename FunctionType>
void testsIbsPut::putGetDataTemplate(const ulong nbBlockToPut, FunctionType putFunction, bool printStats) {
    std::string cfgFile = testsTools::InitConfig(nbIbp, WithHotData);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(NULL != ctrl);
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    std::string cfgFileName = ctrl->getConfigFile();
    std::unordered_map<string, string> testSet;

    ctrl->start();

    testSet.clear();

    double writeTime = 0.0;

    // generate data to put
    for (ulong i = 0; i < nbBlockToPut; i++) {
        std::stringstream key;
        std::string randomData;
        randomString(randomData, blockSize);
        key << "key" << i;
        testSet[key.str()] = randomData;
    }
    // put the data in database
    for (auto ite = testSet.begin(); ite != testSet.end(); ite++) {
        std::chrono::time_point<std::chrono::system_clock> start, end;
        start = chrono::system_clock::now();
        // create transaction, put commit
        StatusCode status = putFunction(ctrl, ite->first, ite->second);
        end = std::chrono::system_clock::now();
        if (!status.ok()) {
            LOG4IBS_ERROR(logger, "Could not commit data, status=" << status.ToString());
        }
        uint64_t elapsedPutTime = std::chrono::duration_cast < std::chrono::microseconds > (end - start).count();
        writeTime += (double) elapsedPutTime;
        if (elapsedPutTime > maxPutTime) {
            LOG4IBS_WARN(logger,
                    "Put operation is too slow (" << elapsedPutTime << "), should be less than " << maxPutTime << " microseconds");
        }
    }
    double writenInMib = ((double) testSet.size()) * ((double) blockSize) / (1024.0 * 1024.0);
    double writeTimeInSeconds = (double) (writeTime) / (1000000.0);
    double writeRatio = writenInMib / writeTimeInSeconds;
    std::cout << "Writen(MiB): " << writenInMib << " Ratio: " << writeRatio << " MiB/s" << std::endl;

    // restart
    ctrl->stop();
    ctrl->start();

    // check data was fetched correctly with "normal" leveldb get API
    LOG4IBS_WARN(logger, "Checking 'normal' get API");
    for (int i = 0; i < nbGetCheckIteration; i++) {
        for (auto pairKV : testSet) {
            const std::string& key = pairKV.first;
            const std::string& putValue = pairKV.second;

            std::string buffer(blockSize, 'A');
            DataChunk fetched(buffer);
            size_t count;
            StatusCode status = ctrl->fetch(key, std::move(fetched), count);
            if (!status.ok()) {
                LOG4IBS_ERROR(logger,
                        "Could not get (with 'normal' leveldb get API) data, status=" << status.ToString());
            }
            ASSERT_TRUE(status.ok());
            if (fetched.toString() != putValue) {
                LOG4IBS_ERROR(logger, "fetched='" << fetched.toString() << "'");
                LOG4IBS_ERROR(logger, "putValue='" << putValue << "'");
            }
            ASSERT_EQ(fetched, putValue);
        }
    }
    testSet.clear();

    ctrl->stop();

    ctrl->destroy();
    delete ctrl;
}

void testsIbsPut::putGetData(const ulong nbBlockToPut, bool printStats) {
    putGetDataTemplate(nbBlockToPut, putTest, printStats);
}

void testsIbsPut::putGetDataTx(const ulong nbBlockToPut, bool printStats) {
    putGetDataTemplate(nbBlockToPut, putTestTx, printStats);
}

struct CompareData {
        uint64_t sizeInM;
        uint64_t timeWithoutTx;
        uint64_t timeWithTx;
        double ratio;
};

TEST_F(testsIbsPut, CompareWithAndWithoutTxPutGetDataL) {
    const uint nbStep = 5;
    std::vector<struct CompareData> data;
    std::cout << "Generating timing data ..." << std::endl;
    for (uint step = 1; step <= nbStep; step++) {
        std::cout << "step " << step << " of " << nbStep << std::endl;
        std::chrono::time_point<std::chrono::system_clock> start, end, startTx, endTx;
        struct CompareData dataToPrint;
        uint64_t sizeM = (1024 * 1024) * step;
        uint64_t calculatedNumberOfBlocks = sizeM / blockSize;
        dataToPrint.sizeInM = step;

        start = chrono::system_clock::now();
        putGetData(calculatedNumberOfBlocks, false);
        end = std::chrono::system_clock::now();
        dataToPrint.timeWithoutTx = std::chrono::duration_cast < std::chrono::microseconds > (end - start).count();

        startTx = chrono::system_clock::now();
        putGetDataTx(calculatedNumberOfBlocks, false);
        endTx = chrono::system_clock::now();
        dataToPrint.timeWithTx = std::chrono::duration_cast < chrono::microseconds > (endTx - startTx).count();
        dataToPrint.ratio = (double) dataToPrint.timeWithTx / (double) dataToPrint.timeWithoutTx;
        data.emplace_back(dataToPrint);
    }

    std::cout << "Size\t|\tTime without Tx\t|\tTime with Tx\t|\tSlowdown" << std::endl;
    for (auto& dataToPrint : data) {
        std::cout << dataToPrint.sizeInM << "\t|\t";
        std::cout << dataToPrint.timeWithoutTx << " ms\t|\t";
        std::cout << dataToPrint.timeWithTx << " ms\t|\t";
        std::cout << dataToPrint.ratio << "x" << std::endl;
    }
}

TEST_F(testsIbsPut, LevelDbPutGet1MData) {
    constexpr uint64_t size1M = (1024 * 1024);
    constexpr uint64_t calculatedNumberOfBlocks = size1M / blockSize;
    LOG4IBS_WARN(logger, "Checking without transactions.");
    putGetData(calculatedNumberOfBlocks, false);
}

TEST_F(testsIbsPut, LevelDbPutGet10MDataL) {
    constexpr uint64_t size10M = (10 * 1024 * 1024);
    constexpr uint64_t calculatedNumberOfBlocks = size10M / blockSize;
    LOG4IBS_WARN(logger, "Checking without transactions.");
    putGetData(calculatedNumberOfBlocks, false);
}

TEST_F(testsIbsPut, LevelDbPutGet100MDataL) {
    constexpr uint64_t size100M = (100 * 1024 * 1024);
    constexpr uint64_t calculatedNumberOfBlocks = size100M / blockSize;
    LOG4IBS_WARN(logger, "Checking without transactions.");
    putGetData(calculatedNumberOfBlocks, true);
}

} /* namespace ibs */

