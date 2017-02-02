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
#include <chrono>
#include <algorithm>
#include "gtest/gtest.h"
#include "tap.h"
#include "testsTools.h"
#include "FileTools.h"

using namespace std;

#define GTEST_COLOR

class EguanPrinter: public ::testing::EmptyTestEventListener {
        virtual void OnTestProgramStart(const ::testing::UnitTest& test) {
            setParentDir(std::string("/tmp/") + "IBS_TESTS_" + randomName() + "/");
            parentDir = getParentDir();
            cout << "-------------------------------------------------------" << endl;
            cout << " T E S T S configuration files in '" << parentDir << "'" << endl;
            cout << "-------------------------------------------------------" << endl;
            ibs::FileTools::createDirectory(parentDir);
        }
        virtual void OnTestCaseStart(const ::testing::TestCase& test) {
            cout << "Running test case: " << test.name() << endl;
        }

        virtual void OnTestStart(const ::testing::TestInfo& test) {
            cout << "Running test: " << test.name() << endl;
        }

        virtual void OnTestEnd(const ::testing::TestInfo& test) {
            if (test.result() && test.result()->Failed()) {
                cerr << "Test " << test.name() << " failed" << endl;
                _failedTests.emplace_back(test.name());
            }
            else {
                // no errors can safely remove all left configuration files if any
                // by deleting parent directory
                ibs::FileTools::removeDirectory(parentDir);
                // and recreate it
                ibs::FileTools::createDirectory(parentDir);
            }
        }

        virtual void OnTestCaseEnd(const ::testing::TestCase& test) {
            cout << "Tests run: " << test.total_test_count();
            cout << ", Failures: " << test.failed_test_count();
            cout << ", Skipped: " << test.disabled_test_count();
            cout << ", Time elapsed: " << test.elapsed_time() / 1000.0 << " sec" << endl;
        }

        virtual void OnTestProgramEnd(const ::testing::UnitTest& test) {
            cout << endl;
            cout << "Results :" << endl << endl;
            cout << "Tests run: " << test.total_test_count();
            cout << ", Failures: " << test.failed_test_count();
            cout << ", Skipped: " << test.disabled_test_count() << endl << endl;

            std::unique(_failedTests.begin(), _failedTests.end());
            if (!_failedTests.empty()) {
                cerr << "List of failed test(s) : " << endl;
                for (auto& failedTest : _failedTests) {
                    cerr << "Test " << failedTest << " failed" << endl;
                }
            }
            else {
                // no errors can safely remove all left configuration files if any
                // by deleting parent directory
                ibs::FileTools::removeDirectory(parentDir);
            }
        }

    private:
        std::vector<std::string> _failedTests;
        std::string parentDir;
};

GTEST_API_ int main(int argc, char **argv) {
    std::chrono::time_point<std::chrono::system_clock> startTime, endTime;
    int ret;
    testing::InitGoogleTest(&argc, argv);
    testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();

    bool keepDefaultPrinter = ((argc > 1) && (std::string(argv[1]) == std::string("--cdt-unit-tests-runner")));

    if (keepDefaultPrinter == false) {
        delete listeners.Release(listeners.default_result_printer());
        listeners.Append(new EguanPrinter);
        listeners.Append(new tap::TapListener());
    }
    startTime = std::chrono::system_clock::now();
    ret = RUN_ALL_TESTS();
    endTime = std::chrono::system_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count() / 1000.0;

    if (keepDefaultPrinter == false) {
        if (ret != 0) {
            cerr << "Some tests failed." << endl;
        }
        else {
            cout << "All test ran successfully." << endl;
        }
    }
    cout << "Total time elapsed: " << elapsed << " sec" << endl;
    return ret;
}
