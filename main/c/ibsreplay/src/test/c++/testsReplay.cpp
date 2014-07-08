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
 * @file testsReplay.cpp
 * @author j. caba
 */
#include "gtest/gtest.h"
#include "Constants.h"
#include "FileTools.h"
#include "Logger.h"
#include "Replay.h"
#include "ReplayMain.h"

#include <signal.h>
#include <exception>

using namespace std;

namespace ibs {
namespace replay {

// ensure configuration of the logger is done once
static Logger_t logger = ibs::Logger::getLogger("testsReplay");

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

class SegmentationFaultException: public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGSEGV;
        }
};

class AbortException: public std::exception {
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

class testsReplay: public ::testing::Test {
    protected:
        testsReplay() {
        }

        virtual ~testsReplay() {
        }

        virtual void SetUp() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        }

        virtual void TearDown() {
        }
};

TEST_F(testsReplay, TestDataGenerationFromKey) {
    const size_t defaultSize = 4096; // 4K
    string key("key");
    const int nbTry = 1000;
    for (int i = 0; i < nbTry; i++) {
        string data1;
        RecordFileReader::GenerateDataForKey(key, data1);
        string data2;
        RecordFileReader::GenerateDataForKey(key, data2);
        EXPECT_EQ(data1.size(), defaultSize);
        EXPECT_EQ(data2.size(), defaultSize);
        EXPECT_EQ(data1, data2);
    }
}

TEST_F(testsReplay, TestBasicMethods) {
    std::string number = RecordFileReader::IntToString(42);
    EXPECT_EQ(number, "42");

    std::string randomName = RecordFileReader::GenerateRandomTemporaryFileName();
    EXPECT_EQ(randomName.size(), 35);
}

TEST_F(testsReplay, TestMainBadArgs) {
    const int argc = 2;
    const char* argv[argc] = { "progname", "arg1" };
    int rescode = ibsreplay_main(2, argv);
    EXPECT_EQ(rescode, EXIT_FAILURE);
}

} /* namespace replay */
} /* namespace ibs */
