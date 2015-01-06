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
#include "testsMemoryTools.h"
#include "StatusCode.h"

namespace ibs {

class testsStatusCode: public ::testing::Test {
    protected:

        testsStatusCode() {
        }

        virtual ~testsStatusCode() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        }

        virtual void SetUp() {
            resetAllocationCounter();
        }

        virtual void TearDown() {
            EXPECT_EQ(0, getAllocationCount());
        }
};

TEST_F(testsStatusCode, Constructors) {
    StatusCode* stp;
    StatusCode status2;
    StatusCode status;
    status = StatusCode();
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::OK();
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::KeyNotFound();
    EXPECT_TRUE(status.IsKeyNotFound());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::SliceTooSmall();
    EXPECT_TRUE(status.IsSliceTooSmall());
    EXPECT_TRUE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::KeyAreadyAdded();
    EXPECT_TRUE(status.IsKeyAlreadyAdded());
    EXPECT_TRUE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::TransactionNotFound();
    EXPECT_TRUE(status.IsTransactionNotFound());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::IOError();
    EXPECT_TRUE(status.IsIOError());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::Corruption();
    EXPECT_TRUE(status.IsCorruption());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::InvalidArgument();
    EXPECT_NE("", status.ToString());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_FALSE(status.ok());

    status = StatusCode::NotSupported();
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_TRUE(status.IsNotSupported());
    EXPECT_NE("", status.ToString());

    status = StatusCode::ConfigError();
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::CreateInExistingIBS();
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    status = StatusCode::InitInEmptyDirectory();
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(status.IsKeyFound());
    EXPECT_NE("", status.ToString());

    /* operator= test */
    status2 = status;
    EXPECT_FALSE(status2.ok());
    EXPECT_FALSE(status2.IsKeyFound());

    /* ptr/new/delete basic test */
    stp = new StatusCode();
    EXPECT_TRUE(stp->ok());
    EXPECT_TRUE(stp->IsKeyFound());
    delete stp;

    stp = new StatusCode(StatusCode::K_UNKOWN);
    EXPECT_TRUE(!stp->ok());
    EXPECT_NE("", stp->ToString());
    EXPECT_EQ(StatusCode::K_UNKOWN, stp->getCode());
    EXPECT_FALSE(stp->IsKeyFound());
    delete stp;

    stp = new StatusCode(StatusCode::K_DUMP_ERROR);
    EXPECT_TRUE(!stp->ok());
    EXPECT_NE("", stp->ToString());
    EXPECT_EQ(StatusCode::K_DUMP_ERROR, stp->getCode());
    delete stp;
}

TEST_F(testsStatusCode, ExposedToCErrors) {
    StatusCode status;
    EXPECT_TRUE(status.ok());
    status = StatusCode::ConfigError();
    EXPECT_TRUE(status.IsConfigError());
    status = StatusCode::CreateInExistingIBS();
    EXPECT_TRUE(status.IsCreateInExistingIBS());
    status = StatusCode::InitInEmptyDirectory();
    EXPECT_TRUE(status.IsInitInEmptyDirectory());
}

} /* namespace ibs */

