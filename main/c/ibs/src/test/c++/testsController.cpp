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
#include "AbstractController.h"
#include "Controller.h"
#include "Recorder.h"
#include "IbpHandler.h"
#include "testsTools.h"

using namespace std;

namespace ibs {

class testsIbsController: public ::testing::Test {
    protected:
        string cfgFile;
        static const int nbIbp = 5;

        virtual void SetUp() {

        }

        virtual void TearDown() {
            ibs::Logger::getRootLogger().setLogLevel(log4cplus::WARN_LOG_LEVEL);
        }
};

TEST_F(testsIbsController, Constructor) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData);
    std::string fname(tmpnam(NULL));
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(fname, ctrl);
    ASSERT_TRUE(st.IsConfigError());
    EXPECT_EQ(NULL, ctrl);
}

TEST_F(testsIbsController, ConstructorAndWrongRecordExecutionOption) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData, true, false);
    AbstractController* ctrl = NULL;
    ibs::Logger::getRootLogger().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.IsConfigError());
    ASSERT_TRUE(NULL == ctrl);
}

TEST_F(testsIbsController, ConstructorAndGoodRecordExecutionOption) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData, false, true);
    AbstractController* ctrl = NULL;
    ibs::Logger::getRootLogger().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_FALSE(st.IsConfigError());
    ASSERT_TRUE(NULL != ctrl);
    Recorder* recorder = dynamic_cast<Recorder*>(ctrl);
    ASSERT_TRUE(NULL != recorder);
    recorder->destroy();
    delete ctrl;
}

TEST_F(testsIbsController, ConstructorWithSyslogEnabled) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData, false, false, true);
    AbstractController* ctrl = NULL;
    ibs::Logger::getRootLogger().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    StatusCode st1 = Controller::create(cfgFile, ctrl);
    ASSERT_FALSE(st1.IsConfigError());
    ASSERT_TRUE(NULL != ctrl);
    StatusCode st2 = Controller::init(cfgFile, ctrl);
    ASSERT_TRUE(st2.ok());
    ctrl->destroy();
    delete ctrl;
}

TEST_F(testsIbsController, ContructorAndUuid) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(NULL != ctrl);
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    ctrl->destroy();
    delete ctrl;
}

TEST_F(testsIbsController, InitOverNonExisting) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::init(cfgFile, ctrl);
    ASSERT_TRUE(st.IsInitInEmptyDirectory());
    EXPECT_EQ(NULL, ctrl);
}

TEST_F(testsIbsController, InitOverExisting) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(NULL != ctrl);
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    delete ctrl;

    st = Controller::init(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    ctrl->destroy();
    delete ctrl;
}

TEST_F(testsIbsController, CreateInExisting) {
    cfgFile = testsTools::InitConfig(nbIbp, OnlyColdData);

    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.ok());
    ASSERT_TRUE(NULL != ctrl);
    EXPECT_EQ(cfgFile, ctrl->getConfigFile());
    EXPECT_EQ(TESTUUID, ctrl->getUuid());
    delete ctrl;

    st = Controller::create(cfgFile, ctrl);
    ASSERT_TRUE(st.IsCreateInExistingIBS());
    EXPECT_EQ(NULL, ctrl);
}

} /* namespace ibs */
