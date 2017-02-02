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
 * @file testsReplace.cpp
 * @author j. caba
 */
#include "testsTools.h"
#include "FileTools.h"
#include "ConfigFileReader.h"
#include "IbpGenHandler.h"
#include "Controller.h"

namespace ibs {

class testsReplace: public ::testing::Test {
    protected:
        string cfgFile;
        AbstractController* ctrl;
        static constexpr int dropWait = 5;

        testsReplace() {
            srand(time(NULL));
        }

        virtual ~testsReplace() {
        }

        virtual void SetUp() {
            ctrl = NULL;
            cfgFile = testsTools::InitConfig(5, WithHotData);
            StatusCode st = Controller::create(cfgFile, ctrl);
            ASSERT_TRUE(st.ok());
            ASSERT_TRUE(NULL != ctrl);
            EXPECT_EQ(cfgFile, ctrl->getConfigFile());
            EXPECT_EQ(TESTUUID, ctrl->getUuid());
            std::string cfgFileName = ctrl->getConfigFile();

            ctrl->start();
        }

        virtual void TearDown() {
            Logger::setLevel("warn");
            cfgFile.clear();
            ctrl->stop();

            ctrl->destroy();
            delete ctrl;
            ctrl = NULL;
        }

        static void* runInThread(void *arg) {
            AbstractController* ibp = reinterpret_cast<AbstractController*>(arg);
            int size = 65536;
            int i;
            int a = rand();
            string randomData;
            for (i = 0; i < 100; i++) {
                StatusCode status;
                stringstream key;
                randomString(randomData, size);
                key << "key" << a + i;
                ibp->put("old", randomData);
                ibp->replace("old", key.str(), randomData);

                std::string buffer(size, 'A');
                DataChunk fetched(buffer);
                size_t count;
                status = ibp->fetch(key.str(), std::move(fetched), count);
                EXPECT_TRUE(status.ok());
                EXPECT_TRUE(fetched == randomData);
            }
            sleep(dropWait);
            std::string buffer(size, 'A');
            DataChunk fetched(buffer);
            size_t count;
            StatusCode status = ibp->fetch("old", std::move(fetched), count);
            EXPECT_FALSE(status.ok());
            return NULL;
        }
};

TEST_F(testsReplace, BasicReplaceAndGet) {
    Logger::setLevel("debug");
    string old_key = "old";
    string key = "new";
    string value = "value";
    StatusCode status;
    ctrl->put(old_key, value);
    ctrl->replace(old_key, key, value);

    Logger::setLevel("info");
    std::string buffer(80, 'A');
    DataChunk fetched(buffer);
    size_t count;
    ctrl->fetch(key, std::move(fetched), count);
    EXPECT_TRUE(fetched == value);
    sleep(dropWait);
    status = ctrl->fetch(old_key, std::move(fetched), count);
    EXPECT_FALSE(status.ok());
    Logger::setLevel("error");
}

TEST_F(testsReplace, Advanced_2Threads_5Ibp_64Kx100records) {
    Logger::setLevel("off");
    pthread_t t1, t2;
    Logger::setLevel("trace");
    pthread_create(&t1, NULL, &runInThread, (void*) ctrl);
    pthread_create(&t2, NULL, &runInThread, (void*) ctrl);
    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    Logger::setLevel("fatal");
}

} //namespace ibs
