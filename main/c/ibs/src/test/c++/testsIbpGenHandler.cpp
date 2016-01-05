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
 * @file testsIbpGenHandler.cpp
 * @author j. caba
 */
#include "testsMemoryTools.h"
#include "testsTools.h"
#include "LevelDbFacade.h"
#include "FileTools.h"
#include "ConfigFileReader.h"
#include "IbpGenHandler.h"
#include "Controller.h"
#include "CreationTask.h"

using namespace std;

namespace ibs {

class testsIbpGenHandler: public ::testing::Test {
    protected:
        size_t before;
        size_t after;

        virtual void SetUp() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
            before = getAllocatedBytes();
        }

        virtual void TearDown() {
            after = getAllocatedBytes();
            int64_t leaked = after - before;
            EXPECT_LE(leaked, 0);
            EXPECT_LE(after, before);
            if (leaked > 0) {
                cerr << "Leaked " << leaked << "\t bytes \t=\t " << leaked / 1024.0 << "\t Kb \t=\t "
                        << leaked / (1024.0 * 1024.0) << "\t Mb" << endl;
            }
        }
};

TEST_F(testsIbpGenHandler, BasicIbpGenHandlerPutGetMemCheck) {
    std::string cfgFile = testsTools::InitConfig(2, WithHotData);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(NULL != ctrl);
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    std::string cfgFileName = ctrl->getConfigFile();

    ctrl->start();

    StatusCode stPut = ctrl->put("key", "data");
    EXPECT_TRUE(stPut.ok());

    std::string buffer(80, 'A');
    DataChunk fetched(buffer);
    size_t count;
    StatusCode stGet = ctrl->fetch("key", std::move(fetched), count);
    EXPECT_TRUE(stGet.ok());
    EXPECT_TRUE(fetched == "data");

    ctrl->stop();

    ctrl->destroy();
    delete ctrl;
}

TEST_F(testsIbpGenHandler, CreationConditionsBasicTests) {
    CreationConditions conditions;
    EXPECT_FALSE(conditions.isCreationPending());
    conditions.requestCreation();
    EXPECT_TRUE(conditions.isCreationPending());
}

} /* namespace ibs */
