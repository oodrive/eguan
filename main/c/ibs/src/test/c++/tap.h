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
/*
 * The MIT License
 *
 * Copyright (c) 2011 Bruno P. Kinoshita <http://www.kinoshita.eti.br>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * ORIGINAL AUTHORS:
 * @author Bruno P. Kinoshita <http://www.kinoshita.eti.br>
 */
#ifndef TAP_H_
#define TAP_H_

#include <vector>
#include <iostream>
#include <unordered_map>
#include <fstream>
#include <stdlib.h>
#include "testsMemoryTools.h"

namespace tap {

typedef std::string String;
typedef std::stringstream StringStream;
typedef std::ofstream OutputFileStream;

class TestResult;
class TestSet;

typedef std::vector<TestResult> TestResultList;
typedef std::unordered_map<String, TestSet> TestResultMap;

class TestResult {
    public:
        TestResult() {

        }

        ~TestResult() {

        }

        String getComment() const {
            StringStream ss;
            if (this->skip) {
                ss << "# SKIP " << this->comment;
            }
            else if (!this->comment.empty()) {
                ss << "# " << this->comment;
            }
            return ss.str();
        }

        const String& getName() const {
            return name;
        }

        int getNumber() const {
            return number;
        }

        const String& getStatus() const {
            return status;
        }

        bool getSkip() const {
            return skip;
        }

        void setComment(const String& comment) {
            this->comment = comment;
        }

        void setName(const String& name) {
            this->name = name;
        }

        void setNumber(int number) {
            this->number = number;
        }

        void setStatus(const String& status) {
            this->status = status;
        }

        void setSkip(bool skip) {
            this->skip = skip;
        }

        String toString() const {
            StringStream ss;
            ss << this->status << " " << this->number << " " << this->name << " " << this->getComment();
            return ss.str();
        }

    private:
        int number;
        String status;
        String name;
        String comment;
        bool skip;
};

class TestSet {
    private:
        TestResultList testResults;

    public:

        const TestResultList& getTestResults() const {
            return testResults;
        }

        void addTestResult(TestResult& testResult) {
            testResult.setNumber((this->getNumberOfTests() + 1));
            this->testResults.emplace_back(testResult);
        }

        int getNumberOfTests() const {
            return this->testResults.size();
        }

        String toString() const {
            StringStream ss;
            ss << "1.." << this->getNumberOfTests() << std::endl;
            for (auto testResult : this->testResults) {
                ss << testResult.toString() << std::endl;
            }
            return ss.str();
        }
};

class TapListener: public ::testing::EmptyTestEventListener {

    private:
        TestResultMap testCaseTestResultMap;
        String testDir;

        const void addTapTestResult(const testing::TestInfo& testInfo) {
            String testCaseName = testInfo.test_case_name();

            tap::TestResult tapResult;
            tapResult.setName(testInfo.name());
            tapResult.setSkip(!testInfo.should_run());

            const testing::TestResult* testResult = testInfo.result();

            if (testResult->HasFatalFailure()) {
                tapResult.setStatus("Bail out!");
            }
            else if (testResult->Failed()) {
                tapResult.setStatus("not ok");
            }
            else {
                tapResult.setStatus("ok");
            }

            this->addNewOrUpdate(testCaseName, tapResult);
        }

        void addNewOrUpdate(const String& testCaseName, tap::TestResult testResult) {
            auto it = this->testCaseTestResultMap.find(testCaseName);
            if (it != this->testCaseTestResultMap.end()) {
                tap::TestSet testSet = it->second;
                testSet.addTestResult(testResult);
                this->testCaseTestResultMap[testCaseName] = testSet;
            }
            else {
                tap::TestSet testSet;
                testSet.addTestResult(testResult);
                this->testCaseTestResultMap[testCaseName] = testSet;
            }
            writeToTap(testCaseName, this->testCaseTestResultMap[testCaseName].toString());
            if (testDir.empty()) {
                testDir = get_current_dir_name();
            }
        }

        void writeToTap(const String& testCaseName, const String& tapStream) {
            const String tapFileName = testCaseName + ".tap";
            OutputFileStream tapFile(tapFileName);
            tapFile << tapStream;
            tapFile.flush();
            tapFile.close();
        }
    public:

        virtual void OnTestEnd(const testing::TestInfo& testInfo) {
            this->addTapTestResult(testInfo);
        }

        virtual void OnTestProgramEnd(const ::testing::UnitTest& /* test */) {
            std::cout << std::endl;
            if (testDir.empty()) {
                std::cout << "Tests report should be in target/test-nar/test-reports";
            }
            else {
                std::cout << "Test report located : ";
                std::cout << testDir;
            }
            std::cout << std::endl;
        }
};

}

#endif /* TAP_H_ */
