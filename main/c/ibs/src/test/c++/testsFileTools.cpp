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
/**
 * @file testsFileTools.h
 * @author j. caba
 */
#include "testsTools.h"
#include "Logger.h"
#include "FileTools.h"
#include <iostream>
#include <string>
#include <regex>

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("testsFileTools");

class testsFileTools: public ::testing::Test {
    protected:
        testsFileTools() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        }

        virtual ~testsFileTools() {
        }

        virtual void SetUp() {
        }

        virtual void TearDown() {
        }
};

TEST_F(testsFileTools, CreateDirectoryTest) {
    std::string tmp = "/tmp/testFileTools";
    EXPECT_FALSE(FileTools::exists(tmp));
    EXPECT_TRUE(FileTools::createDirectory(tmp));
    EXPECT_TRUE(FileTools::exists(tmp));
    EXPECT_TRUE(FileTools::isDirectory(tmp));
    EXPECT_TRUE(FileTools::isDirectoryEmpty(tmp));
    EXPECT_TRUE(FileTools::dirOk(tmp));
    EXPECT_TRUE(FileTools::isReadable(tmp));
    EXPECT_TRUE(FileTools::isWritable(tmp));
    EXPECT_FALSE(FileTools::isFile(tmp));
    FileTools::removeDirectory(tmp);
    EXPECT_FALSE(FileTools::exists(tmp));
}

TEST_F(testsFileTools, IbsExtensionBasicTest) {
    EXPECT_FALSE(FileTools::hasIbsExtension("subject"));
    EXPECT_TRUE(FileTools::hasIbsExtension("subject.ibs"));
}

} /* namespace ibs */

