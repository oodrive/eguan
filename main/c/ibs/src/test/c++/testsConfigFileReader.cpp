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
#include "Constants.h"
#include "ConfigFileReader.h"
#include "FileTools.h"
#include "testsTools.h"
#include <memory>
#include <string>
#include <fstream>

namespace ibs {

class testsConfigFileReader: public ::testing::Test {
    protected:
        string cfgFile;
        ofstream f;
        std::unique_ptr<ConfigFileReader> config;

        testsConfigFileReader() :
                cfgFile(std::string(MKTMPNAME()) + "_CONFIG"), f(), config() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
            f.open(cfgFile.c_str());
            f << "key1=val1" << endl;
            f << "key2=val2#testcomz1" << endl;
            f << "#key3=val3" << endl;
            // test with allowed keys
            f << HOT_DATA << "=val4" << endl;
            f << LOG_LEVEL << "=val5#foobar" << endl;
            f.close();
            config.reset(new ConfigFileReader(cfgFile));
            config->read();
        }

        virtual ~testsConfigFileReader() {
            remove(cfgFile.c_str());
        }

        virtual void SetUp() {
        }

        virtual void TearDown() {
        }
};

TEST_F(testsConfigFileReader, ReadValue) {
    EXPECT_EQ(string("val1"), config->getString("key1"));
    EXPECT_EQ(string("val4"), config->getString(HOT_DATA));
}

TEST_F(testsConfigFileReader, ReadValueWithComments) {
    EXPECT_EQ(string("val2"), config->getString("key2"));
    EXPECT_EQ(string("val5"), config->getString(LOG_LEVEL));
}

TEST_F(testsConfigFileReader, IgnoreComments) {
    EXPECT_NE(string("val3"), config->getString("#key3"));
}

TEST_F(testsConfigFileReader, SetValue) {
    config->setString("key4", "val4");
    EXPECT_EQ(string("val4"), config->getString("key4"));
}

TEST_F(testsConfigFileReader, EraseKey) {
    config->eraseKey("key1");
    EXPECT_TRUE(config->getString("key1").empty());
}

TEST_F(testsConfigFileReader, WriteFile) {
    string cfgFile2 = std::string(MKTMPNAME()) + "_CONFIG";
    f.open(cfgFile2.c_str());
    f << "key1=val1" << endl;
    f << HOT_DATA << "=val4" << endl;
    f.close();
    std::unique_ptr<ConfigFileReader> cfg;
    cfg.reset(new ConfigFileReader(cfgFile2));
    cfg->read();
    ASSERT_TRUE(cfg->isLoaded());
    cfg->setString("key2", "val2");
    cfg->setString(LOG_LEVEL, "val5");
    cfg->write();
    cfg.reset(new ConfigFileReader(cfgFile2));
    cfg->read();
    ASSERT_TRUE(cfg->isLoaded());
    EXPECT_EQ(string("val1"), cfg->getString("key1"));
    EXPECT_EQ(string("val2"), cfg->getString("key2"));
    EXPECT_EQ(string("val4"), cfg->getString(HOT_DATA));
    EXPECT_EQ(string("val5"), cfg->getString(LOG_LEVEL));
    remove(cfgFile2.c_str());
}

TEST_F(testsConfigFileReader, WriteFileDuplicateKeys) {
    string cfgFile2 = std::string(MKTMPNAME()) + "_CONFIG";
    f.open(cfgFile2.c_str());
    f << "key1=val1" << endl;
    f << HOT_DATA << "=val9" << endl;
    f << HOT_DATA << "=val6" << endl;
    f.close();
    std::unique_ptr<ConfigFileReader> cfg;
    cfg.reset(new ConfigFileReader(cfgFile2));
    EXPECT_TRUE(FileTools::getAbsolutePath(cfgFile2) == cfg->getFilename());
    cfg->read();
    ASSERT_TRUE(cfg->isLoaded());
    cfg->setString("key2", "val2");
    cfg->setString(LOG_LEVEL, "val7");
    cfg->setString(LOG_LEVEL, "val8");
    cfg->setInt("key1000", 1000);
    cfg->write();
    cfg.reset(new ConfigFileReader(cfgFile2));
    cfg->read();
    ASSERT_TRUE(cfg->isLoaded());
    EXPECT_EQ(string("val1"), cfg->getString("key1"));
    EXPECT_EQ(string("val2"), cfg->getString("key2"));
    EXPECT_EQ(string("val9"), cfg->getString(HOT_DATA));
    EXPECT_EQ(string("val8"), cfg->getString(LOG_LEVEL));
    EXPECT_EQ(1000, cfg->getInt("key1000"));
    remove(cfgFile2.c_str());
}

} /* namespace ibs */

