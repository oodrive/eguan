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
 * @file testsLevelDbFacade.cpp
 * @author j. caba
 */
#include "testsMemoryTools.h"
#include "testsTools.h"
#include "LevelDbFacade.h"
#include "FileTools.h"

using namespace std;

namespace ibs {

class testsLevelDbFacade: public ::testing::Test {
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

TEST_F(testsLevelDbFacade, BasicLevelDbFacadePutGetMemCheck) {
    std::vector<bool> falseTrue = { false, true };
    for (bool param1 : falseTrue) {
        for (bool param2 : falseTrue) {
            for (bool param3 : falseTrue) {
                std::string dbPath = std::string(MKTMPNAME()) + "_IBP";
                FileTools::createDirectory(dbPath);

                LevelDbFacade db(dbPath, param1, param2, param3);
                EXPECT_TRUE(db.isClosed());
                db.open();
                EXPECT_FALSE(db.isClosed());

                StatusCode stPut = db.put("key", "data");
                EXPECT_TRUE(stPut.ok());

                std::string fetched;
                StatusCode stGet = db.get("key", &fetched);
                EXPECT_TRUE(stGet.ok());
                EXPECT_TRUE(fetched == "data");

                EXPECT_FALSE(db.isClosed());
                db.close();
                EXPECT_TRUE(db.isClosed());
                std::string output;
                db.getStats(output);

                FileTools::removeDirectory(dbPath);
            }
        }
    }
}

} /* namespace ibs */
