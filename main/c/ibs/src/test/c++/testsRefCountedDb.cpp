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
/**
 * @file testsRefCountedDb.cpp
 * @author j. caba
 */
#include "testsMemoryTools.h"
#include "testsTools.h"
#include "LevelDbFacade.h"
#include "FileTools.h"
#include "RefCountedDb.h"

using namespace std;

namespace ibs {

class testsRefCountedDb: public ::testing::Test {
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

TEST_F(testsRefCountedDb, BasicSetPutGetMemCheck) {
    std::string parentDir = std::string(MKTMPNAME()) + "_RefDb";
    FileTools::createDirectory(parentDir);
    std::string dbName = parentDir + "/" + "refcounted.db";

    std::unique_ptr<RefCountedDb> db(new RefCountedDb(dbName));
    ASSERT_TRUE(db.get() != NULL);

    EXPECT_TRUE(db->isClosed());
    db->open();
    EXPECT_FALSE(db->isClosed());

    StatusCode stPut = db->put("key", "data");
    EXPECT_TRUE(stPut.ok());

    std::string fetched;
    StatusCode stGet = db->get("key", &fetched);
    EXPECT_TRUE(stGet.ok());
    EXPECT_TRUE(fetched == "data");

    std::string fetched2;
    StatusCode stPut2 = db->put("key1", "data1");
    EXPECT_TRUE(stPut2.ok());
    StatusCode stGet2 = db->get("key1", &fetched2);
    EXPECT_TRUE(stGet2.ok());
    EXPECT_TRUE(fetched2 == "data1");
    std::string fetched3;
    StatusCode stGet3 = db->get("key", &fetched3);
    EXPECT_TRUE(stGet3.ok());
    EXPECT_TRUE(fetched3 == "data");

    EXPECT_FALSE(db->isClosed());
    db->close();
    EXPECT_TRUE(db->isClosed());

    EXPECT_TRUE(FileTools::exists(dbName));
    EXPECT_TRUE(RefCountedDbFile::hasValidHeader(db->getStoragePath()));
    db.reset();// delete db and destroy file
    EXPECT_FALSE(FileTools::exists(dbName));

    FileTools::removeDirectory(parentDir);
}

} /* namespace ibs */
