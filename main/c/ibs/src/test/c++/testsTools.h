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
 * @file testsTools.h
 * @brief Tools for unit testings/header
 * @author j. caba
 */
#ifndef TESTSTOOLS_H_
#define TESTSTOOLS_H_

#include "gtest/gtest.h"

#include <sys/types.h>

#include <execinfo.h>
#include <signal.h>
#include <exception>
#include <iostream>
#include <map>
#include <string>

#define TEST_PREFIX "IBS_"
#define TESTUUID  "ffd93fa8-5952-4943-a474-84bed12b0c9f"
#define OWNERUUID "aed12fbb-e76b-ae7d-4ff3-2b0c9f84bed1"

extern std::string randomName(const size_t nbRamdomChar = 10);
extern void setParentDir(const std::string& parentDir);
extern std::string getParentDir();
extern std::string MKTMPNAME(const size_t nbRamdomChar = 64);
extern void randomString(std::string& str, const int size, const int numberOfzeroToInsert = 10);

namespace ibs {

#define IBSTEST_ASSERT_FALSE(cond, msg) \
if ( cond ) {                          \
    std::cerr << msg << std::endl;     \
    ASSERT_TRUE(false);                \
}

using namespace std;

namespace errors {
// The exception handling back_trace like the following IBM article :
// http://www.ibm.com/developerworks/library/l-cppexcep/index.html

/* Class to register a signal handler and translate the signal to a C++ exception */
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
            void* array[25];
            int nSize = backtrace(array, 25);
            char** symbols = backtrace_symbols(array, nSize);

            for (int i = 0; i < nSize; i++) {
                if (redTrace) {
                    std::cerr << symbols[i] << std::endl;
                }
                else {
                    std::cout << symbols[i] << std::endl;
                }

            }

            free(symbols);
        }

        static void enableRedTrace() {
            redTrace = true;
        }
        static void disableRedTrace() {
            redTrace = false;
        }
    private:
        static bool redTrace;
};

class SegmentationFaultException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGSEGV;
        }
};

class BusErrorException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGBUS;
        }
};

class AbortException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGABRT;
        }
};

class FloatingPointException: public ExceptionTracer, public std::exception {
    public:
        static int GetSignalNumber() {
            return SIGFPE;
        }
};

extern SignalTranslator<SegmentationFaultException> g_objSegmentationFaultTranslator;
extern SignalTranslator<AbortException> g_objAbortTranslator;
extern SignalTranslator<FloatingPointException> g_objFPETranslator;

class ExceptionHandler {
    public:
        ExceptionHandler() {
            std::set_terminate(Handler);
        }

        static void Handler();
};

extern ExceptionHandler g_objHandler;

}/* namespace errors */

enum Type {
    OnlyColdData = 1, WithHotData = 2
};

class testsTools {
    public:
        static string InitConfig(const int n_ibp, const Type type, const bool doFalseRecordOption = false,
                const bool doRealRecordOption = false, const bool doEnableSyslog = false);

        static void writeTestConfig(const std::string& fname, const std::map<std::string, std::string>& _cfg);
};

} /* namespace ibs */

#endif /* TESTSTOOLS_H_ */
