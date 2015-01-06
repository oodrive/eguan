/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2015 Oodrive
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
#include "gtest/gtest.h"
#include "tap.h"

using namespace std;

#define GTEST_COLOR

class NuagePrinter: public ::testing::EmptyTestEventListener {
        virtual void OnTestProgramStart(const ::testing::UnitTest& test) {
            cout << "-------------------------------------------------------" << endl;
            cout << " T E S T S" << endl;
            cout << "-------------------------------------------------------" << endl;
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
        }

};

GTEST_API_ int main(int argc, char **argv) {
    int ret;
    testing::InitGoogleTest(&argc, argv);
    testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();

    bool keepDefaultPrinter = ((argc > 1) && (std::string(argv[1]) == std::string("--cdt-unit-tests-runner")));

    if (keepDefaultPrinter == false) {
        delete listeners.Release(listeners.default_result_printer());
        listeners.Append(new NuagePrinter);
        listeners.Append(new tap::TapListener());
    }
    ret = RUN_ALL_TESTS();

    if (keepDefaultPrinter == false) {
        if (ret != 0) {
            cerr << "Some tests failed." << endl;
        }
        else {
            cout << "All test ran successfully." << endl;
        }
    }
    return ret;
}
