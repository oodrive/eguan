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
 * @file testsFIFO.cpp
 * @author j. caba
 */
#include "testsTools.h"
#include "testsMemoryTools.h"
#include "Logger.h"
#include "FileTools.h"
#include "ConcurrentFIFO.h"
#include "LevelDbFacade.h"

using namespace std;

namespace ibs {

class testsIbsFIFO: public ::testing::Test {
    protected:
        size_t before;
        size_t after;

        virtual void SetUp() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
            leveldb::Options firstOptions; // to allocate default environment.
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

void addNewDb(const std::string& dbPath, ConcurrentFIFO<std::string>& dbPaths, ConcurrentFIFO<leveldb::DB>& dbHandle,
        const int64_t index) {
    leveldb::Options options;
    options.block_cache = NULL;
    options.create_if_missing = true; //to create the database
    options.error_if_exists = true; // don't create a new database if already created

    stringstream ss;
    ss << index;
    const std::string fullpath = dbPath + "/" + ss.str();
    FileTools::createDirectory(fullpath);

    leveldb::DB* dbToAllocate = NULL;
    leveldb::DB::Open(options, fullpath, &dbToAllocate);
    ASSERT_TRUE(dbToAllocate != NULL);

    dbHandle.insertNewest(dbToAllocate);
    ASSERT_FALSE(dbHandle.getNewest().expired());
    ASSERT_TRUE(dbHandle.getNewest().lock().get() == dbToAllocate);
    dbToAllocate = NULL;
    ASSERT_TRUE(dbHandle.getNewest().lock().get() != NULL);

    dbPaths.insertNewest(new std::string(fullpath));

    ASSERT_TRUE(dbPaths.count() == dbHandle.count());
}

TEST_F(testsIbsFIFO, CheckFIFORotationWithLeveldb) {
    int64_t index = -1;

    std::cout << "Testing levedb databases rotation" << std::endl;

    const std::string dbPath = std::string(MKTMPNAME()) + "_IBPGEN_DIR";
    FileTools::removeDirectory(dbPath);
    FileTools::createDirectory(dbPath);

    ConcurrentFIFO<std::string> dbPaths; //only to keep track of directory
    ConcurrentFIFO<leveldb::DB> dbHandle;

    addNewDb(dbPath, dbPaths, dbHandle, ++index);
    addNewDb(dbPath, dbPaths, dbHandle, ++index);
    addNewDb(dbPath, dbPaths, dbHandle, ++index);
    addNewDb(dbPath, dbPaths, dbHandle, ++index);

    // check directory created
    std::vector<std::string> dirs1;
    FileTools::list(dbPath, dirs1, FileTools::directory);
    EXPECT_TRUE(dirs1.size() == 4);
    std::sort(dirs1.begin(), dirs1.end());
    const std::vector<std::string> test1 = { "0", "1", "2", "3" };
    EXPECT_TRUE(dirs1 == test1);

    const size_t testLimit = 20;
    for (size_t i = 5; i < testLimit; i++) {
        addNewDb(dbPath, dbPaths, dbHandle, ++index);
        dbHandle.removeOldest();

        std::vector<std::string> dirs2;
        FileTools::list(dbPath, dirs2, FileTools::directory);
        size_t nbDirBefore = dirs2.size();

        // remove directory too
        EXPECT_FALSE(dbPaths.getOldest().expired());
        FileTools::removeDirectory(*dbPaths.getOldest().lock());
        dbPaths.removeOldest();

        // and check a directory was deleted
        std::vector<std::string> dirs3;
        FileTools::list(dbPath, dirs3, FileTools::directory);
        size_t nbDirAfter = dirs3.size();

        EXPECT_TRUE(nbDirAfter == (nbDirBefore - 1));
    }

    FileTools::removeDirectory(dbPath);
}

void addNewDbWithFacade(const std::string& dbPath, ConcurrentFIFO<LevelDbFacade>& dbHandle, const int64_t index) {
    leveldb::Options options;
    options.block_cache = NULL;
    options.create_if_missing = true; //to create the database
    options.error_if_exists = true; //don't create a new database if already created

    stringstream ss;
    ss << index;
    const std::string fullpath = dbPath + "/" + ss.str();
    FileTools::createDirectory(fullpath);

    // simulate "ibpgen" database
    dbHandle.insertNewest(new LevelDbFacade(fullpath, true, true, false));
    ASSERT_FALSE(dbHandle.getNewest().expired());
    ASSERT_TRUE(dbHandle.getNewest().lock().get() != NULL);
    ASSERT_TRUE(dbHandle.getNewest().lock()->getStoragePath() == fullpath);
}

TEST_F(testsIbsFIFO, CheckFIFORotationWithLeveldbFacade) {
    int64_t index = -1;

    std::cout << "Testing levedb databases rotation" << std::endl;

    const std::string dbPath = std::string(MKTMPNAME()) + "_IBPGEN_DIR";
    FileTools::removeDirectory(dbPath);
    FileTools::createDirectory(dbPath);

    ConcurrentFIFO<LevelDbFacade> dbHandle;

    addNewDbWithFacade(dbPath, dbHandle, ++index);
    addNewDbWithFacade(dbPath, dbHandle, ++index);
    addNewDbWithFacade(dbPath, dbHandle, ++index);
    addNewDbWithFacade(dbPath, dbHandle, ++index);

    // check directory created
    std::vector<std::string> dirs1;
    FileTools::list(dbPath, dirs1, FileTools::directory);
    EXPECT_TRUE(dirs1.size() == 4);
    const std::vector<std::string> test1 = { "0", "1", "2", "3" };
    std::sort(dirs1.begin(), dirs1.end());
    EXPECT_TRUE(dirs1 == test1);

    const size_t testLimit = 20;
    for (size_t i = 5; i < testLimit; i++) {
        addNewDbWithFacade(dbPath, dbHandle, ++index);

        std::vector<std::string> dirsBefore;
        FileTools::list(dbPath, dirsBefore, FileTools::directory);
        std::sort(dirsBefore.begin(), dirsBefore.end());
        size_t nbDirBefore = dirsBefore.size();

        // remove directory too
        EXPECT_FALSE(dbHandle.getOldest().expired());
        dbHandle.getOldest().lock()->close();
        EXPECT_TRUE(dbHandle.getOldest().lock()->isClosed());

        dbHandle.getOldest().lock()->destroy();

        // and check the good directory was deleted
        std::vector<std::string> dirsAfter;
        FileTools::list(dbPath, dirsAfter, FileTools::directory);
        std::sort(dirsAfter.begin(), dirsAfter.end());
        size_t nbDirAfter = dirsAfter.size();
        EXPECT_TRUE(nbDirAfter == (nbDirBefore - 1));

        const std::string dirThatShouldBeDeleted = dbHandle.getOldest().lock()->getStoragePath();
        std::vector<std::string> removedDirs;
        std::set_difference(dirsBefore.begin(), dirsBefore.end(), dirsAfter.begin(), dirsAfter.end(),
                std::inserter(removedDirs, removedDirs.begin()));
        EXPECT_TRUE(removedDirs.size() == 1);

        EXPECT_EQ(removedDirs.front(), FileTools::getBasename(dirThatShouldBeDeleted));
        EXPECT_EQ(dbPath + "/" + removedDirs.front(), dirThatShouldBeDeleted);
        EXPECT_EQ(FileTools::getBasename(removedDirs.front()), FileTools::getBasename(dirThatShouldBeDeleted));

        // effectively remove and delete database
        dbHandle.removeOldest();
    }

    FileTools::removeDirectory(dbPath);
}

TEST_F(testsIbsFIFO, CheckEmptyFIFO) {
    ConcurrentFIFO<std::string> dbHandle;
    EXPECT_TRUE(dbHandle.isEmpty());
    dbHandle.insertNewest(new std::string("test"));
    EXPECT_FALSE(dbHandle.isEmpty());
    dbHandle.reborn();
    EXPECT_TRUE(dbHandle.isEmpty());
    auto testOldest = dbHandle.getOldest();
    auto testNewest = dbHandle.getNewest();

    EXPECT_TRUE(testOldest.expired());
    EXPECT_TRUE(testNewest.expired());
}

} /* namespace ibs */
