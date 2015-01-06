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
 * @file Replay.h
 * @brief IBS Execution Replay
 * @author j. caba
 */
#ifndef REPLAY_H_
#define REPLAY_H_

#include <iostream>
#include "Constants.h"
#include "StatusCode.h"
#include "Logger.h"
#include "FileTools.h"
#include "ConfigFileReader.h"
#include "AbstractController.h"

extern std::string REPLAY_LOG_LEVEL;

namespace ibs {
namespace replay {

/**
 * @brief Implements the reading of a recorded file and replay the action stored in the file.
 * The following snippet code shows usage of the class.
 * <code>
 *  int main(const int argc, const char* argv[]) {
 *   string recordFile;
 *   // ...
 *   ibs::replay::RecordFileReader reader(recordFile);
 *   return reader.replay();
 *  }
 * </code>
 */
class RecordFileReader: private BinaryFileBase {
    public:
        /**
         * @brief Open the given file.
         */
        RecordFileReader(const std::string& fname);

        /**
         * @brief Close the file stream upon destruction
         */
        virtual ~RecordFileReader();

        /**
         * @brief Replay the action from the file by creating an IBS with the saved configuration.
         */
        int replay(const std::string& configFile);

        /**
         * @brief Return the number of entries read from file
         */
        uint64_t getEntries();

        /**
         * @brief Return the validity state of the reading stream
         */
        bool isValid();

        /**
         * @brief Generate a string always identical given a key.
         * @param key Key corresponding to the data
         * @param str The output string
         * @param size The size of string to generate. The default value is 4K.
         * @param zeroToInsert The number of zero to insert. Default value is 2K.
         * @param forceKeyAsData Instead of calculating the data : data are the keys.
         *
         * NOTE: The global idea is to insert zero with a uniform random distribution
         * hoping the data generated will be more seems compressible.
         */
        static void GenerateDataForKey(const std::string& key, std::string& str, const size_t size = 4096,
                int zeroToInsert = 2048, bool forceKeyAsData = false);

        /**
         * @brief Clear the memory allocated by Data Generation .Provided to call at end of program.
         * @see GenerateDataForKey
         */
        static void ClearGeneratedDataCache();

        /**
         * @brief Utility method
         */
        static std::string IntToString(const int value);

        // create temporary name for configuration of an IBS
        static std::string GenerateRandomTemporaryFileName();

    protected:

        /**
         * @brief Read the opcode from the file.
         */
        uint64_t readOpcode();

        /**
         * @brief Read a "string" data from the file.
         */
        void readData(std::string& output);

        /**
         * @brief Read a transaction ID from the file.
         */
        uint64_t readId();

        /**
         * @brief Read expected size for fetch.
         */
        uint64_t readExpected();

        /**
         * @brief Read the result of an operation from the file.
         */
        uint64_t readResult();

        /**
         * @brief Update the internal validity stream state.
         */
        void updateValidity();

        /**
         * @brief Check the result is the one expected and stored in the file.
         */
        bool checkResult(const StatusCode& result);

        /**
         * @brief Check the result is the one expected and stored in the file.
         */
        bool checkIntResult(const int result);

        /**
         * @brief Read the first byte of the file to ensure a minimal check that the file is in the correct format.
         */
        void readCheckMagic();

        /**
         * @brief Read the entries  # from the file.
         */
        void readEntriesNb();

        /**
         * @brief Create an IBS with saved configuration but with trace log level messages.
         */
        void createIbs(const std::string& configFile);

    private:
        // hides internal implementation of replay for one operation
        bool replayOpcode(const uint64_t opcode);

        // hides internal implementation of replay loop
        int replayLoop(const uint64_t opcode);

        // warning messages on fetch
        void warnOnFetchMismatch(uint64_t got, uint64_t expected, uint32_t keyHash);

        void override(ConfigFileReader* config, std::map<std::string, std::string>& cfg, const std::string& key);

    private:
        const std::string& filename;
        std::fstream file; // file stream opened in binary mode
        bool valid; // if the stream is in a valid state
        uint64_t entries; // entries # read from file
        std::unique_ptr<AbstractController> controller; // controller to replay actions
        static Logger_t logger; // logger to warn users
        static const uint64_t sliceTooSmallCode; // get code when fetch return SliceTooSmall
};

} /* namespace replay  */
} /* namespace ibs  */

#endif /* REPLAY_H_ */
