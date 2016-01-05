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
 * @file Replay.cpp
 * @brief IBS Execution Replay
 * @author j. caba
 */
#include "Replay.h"
#include "Recorder.h"
#include "Controller.h"
#include "Transaction.h"

std::string REPLAY_LOG_LEVEL = "debug";

namespace ibs {
namespace replay {

Logger_t RecordFileReader::logger = ibs::Logger::getLogger("RecordFileReader");
const uint64_t RecordFileReader::sliceTooSmallCode = StatusCode::SliceTooSmall().getCode();

RecordFileReader::RecordFileReader(const std::string& fname) :
        filename(fname), file(fname, std::ios::binary | std::ios::in), valid(true), entries(0) {
    updateValidity();
}

RecordFileReader::~RecordFileReader() {
    file.close();
    controller.reset();
}

void RecordFileReader::createIbs(const std::string& configFile) {
    controller.reset(); // first release memory and destroy databases of an old Ibs if needed
    constexpr const char* UUID = "ffd93fa8-5952-4943-a474-84bed12b0c9f";
    constexpr const char* OWNERUUID = "aed12fbb-e76b-ae7d-4ff3-2b0c9f84bed1";
    const std::string cfgFile = GenerateRandomTemporaryFileName() + "_CONFIG";
    AbstractController* ibsCtrl = NULL;
    bool isValidConfig = !configFile.empty();
    std::unique_ptr < ConfigFileReader > config(new ConfigFileReader(configFile));
    if (isValidConfig) {
        isValidConfig = config.get() != NULL;
    }
    // 1) re-create configuration
    std::string hotDataParam;
    fetchString(file, hotDataParam);
    int n_ibp = fetchUint64(file);
    std::string bufferRLimitParam = IntToString(fetchUint64(file));
    std::string bufferRDelayParam = IntToString(fetchUint64(file));
    std::string compressionParam;
    fetchString(file, compressionParam);
    std::string bufferWriteDelayLimitParam = IntToString(fetchUint64(file));
    std::string bufferWriteDelayLevelSizeParam = IntToString(fetchUint64(file));
    std::string bufferWriteDelayIncrParam = IntToString(fetchUint64(file));
    std::string ldbBlockSize = IntToString(fetchUint64(file));
    std::string ldbBlockRestartInterval = IntToString(fetchUint64(file));
    std::string indicatedRamSize = IntToString(fetchUint64(file));

    std::map < std::string, std::string > cfg;
    if (isValidConfig) {
        // get PATH from configuration file and avoid creating
        // directories for IBP and IBPGEN
        cfg[IBP_PATH] = config->getString(IBP_PATH);
        cfg[IBP_GEN_PATH] = config->getString(IBP_GEN_PATH);
        cfg[IBS_UUID] = config->getString(IBS_UUID);
        cfg[IBS_OWNER] = config->getString(IBS_OWNER);
    }
    else {
        std::vector < std::string > ibpDirs;
        std::stringstream ss;
        for (int i = 0; i < n_ibp; i++) {
            ibpDirs.push_back(GenerateRandomTemporaryFileName() + "_IBP");
        }
        for (int i = 0; i < n_ibp; i++) {
            if (FileTools::createDirectory(ibpDirs[i]) != true) {
                LOG4IBS_ERROR(logger, "Could not create directory '" << ibpDirs[i] << "'");
            }
            ss << ibpDirs[i];
            if (i + 1 != n_ibp)
                ss << ",";
        }
        cfg[IBP_PATH] = ss.str();
        cfg[IBP_GEN_PATH] = GenerateRandomTemporaryFileName() + "_IBPGEN";
        FileTools::createDirectory (cfg[IBP_GEN_PATH]);
        cfg[IBS_UUID] = UUID;
        cfg[IBS_OWNER] = OWNERUUID;
    }
    // force parameters first
    cfg[HOT_DATA] = hotDataParam;
    cfg[IBP_NB] = IntToString(n_ibp);
    cfg[BUFFER_ROTATION_THRESHOLD] = bufferRLimitParam;
    cfg[BUFFER_ROTATION_DELAY] = bufferRDelayParam;
    cfg[COMPRESSION] = compressionParam;
    cfg[BUFFER_WRITE_DELAY_THRESHOLD] = bufferWriteDelayLimitParam;
    cfg[BUFFER_WRITE_DELAY_LEVEL_SIZE] = bufferWriteDelayLevelSizeParam;
    cfg[BUFFER_WRITE_DELAY_INCR_MS] = bufferWriteDelayIncrParam;
    cfg[LDB_BLOCK_SIZE] = ldbBlockSize;
    cfg[LDB_BLOCK_RESTART_INVERVAL] = ldbBlockRestartInterval;
    cfg[INDICATED_RAM_SIZE] = indicatedRamSize;
    // then override with valid parameters
    // from given configuration file
    if (isValidConfig) {
        override(config.get(), cfg, HOT_DATA);
        override(config.get(), cfg, IBP_NB);
        override(config.get(), cfg, BUFFER_ROTATION_THRESHOLD);
        override(config.get(), cfg, BUFFER_ROTATION_DELAY);
        override(config.get(), cfg, COMPRESSION);
        override(config.get(), cfg, BUFFER_WRITE_DELAY_THRESHOLD);
        override(config.get(), cfg, BUFFER_WRITE_DELAY_LEVEL_SIZE);
        override(config.get(), cfg, BUFFER_WRITE_DELAY_INCR_MS);
        override(config.get(), cfg, LDB_BLOCK_SIZE);
        override(config.get(), cfg, LDB_BLOCK_RESTART_INVERVAL);
        override(config.get(), cfg, INDICATED_RAM_SIZE);
    }
    cfg[LOG_LEVEL] = REPLAY_LOG_LEVEL; // forced log parameter
    // free some memory before the end of the method
    hotDataParam.clear();
    bufferRLimitParam.clear();
    bufferRDelayParam.clear();
    compressionParam.clear();
    bufferWriteDelayLimitParam.clear();
    bufferWriteDelayLevelSizeParam.clear();
    bufferWriteDelayIncrParam.clear();
    ldbBlockSize.clear();
    ldbBlockRestartInterval.clear();
    indicatedRamSize.clear();
    config.reset();
    // 2) save the configuration in the configuration file
    std::ofstream f;
    f.open(cfgFile.c_str());
    auto ite = cfg.begin();
    while (ite != cfg.end()) {
        f << ite->first << "=" << ite->second << std::endl;
        ++ite;
    }
    f.close();
    // 3) create a controller to use for replay
    Controller::create(cfgFile, ibsCtrl);
    controller.reset(ibsCtrl);
}

int RecordFileReader::replay(const std::string& configFile) {
    int resultCode = EXIT_FAILURE;
    readCheckMagic();
    if (!isValid()) {
        return resultCode;
    }
    readEntriesNb();
    if (!isValid()) {
        return resultCode;
    }
    if (isValid()) {
        std::cout << "Found " << getEntries() << " entries to replay" << std::endl;

        // read OFFSET for header
        std::ios::pos_type beginRecordFromFile = fetchUint64(file);
        if (isValid()) {
            // read configuration and create
            createIbs(configFile);
            std::ios::pos_type beginRecord = file.tellp();
            file.seekp(beginRecord);
            // begin to read the list of operations
            if (beginRecordFromFile == beginRecord) {
                bool replayOK = false;
                for (size_t i = 0; i < getEntries(); i++) {
                    double progress = static_cast<double>(i) * static_cast<double>(100.0);
                    if ((i + 1) == getEntries()) {
                        progress = 100.0;
                    }
                    else {
                        progress /= static_cast<double>(getEntries());
                    }
                    std::cout << "Replay " << i + 1 << " of " << getEntries() << " progression at " << progress << "%"
                            << std::endl;
                    if (!isValid()) {
                        LOG4IBS_ERROR(logger, "Premature stop after " << i << " entries replayed");
                        break;
                    }
                    uint64_t opcode = readOpcode();
                    replayOK = replayOpcode(opcode);
                    if (!replayOK) {
                        break;
                    }
                }
                if (replayOK) {
                    resultCode = EXIT_SUCCESS;
                }
            }
            else {
                LOG4IBS_ERROR(logger, "Premature stop after reading file header");
            }
        }
    }
    ibs::Logger::setLevel("error");
    // stop controller and release memory if not done in the saved record
    // (if the program recording was killed for example ...)
    // This avoid ugly crash and potential reachable memory leak.
    controller->stop();
    controller.reset();
    ClearGeneratedDataCache();
    return resultCode;
}

uint64_t RecordFileReader::getEntries() {
    return entries;
}

bool RecordFileReader::isValid() {
    return valid;
}

uint64_t RecordFileReader::readOpcode() {
    return fetchUint64(file);
}

void RecordFileReader::readData(std::string& output) {
    fetchString(file, output);
}

uint64_t RecordFileReader::readId() {
    return fetchUint64(file);
}

uint64_t RecordFileReader::readExpected() {
    return fetchUint64(file);
}

uint64_t RecordFileReader::readResult() {
    return fetchUint64(file);
}

void RecordFileReader::updateValidity() {
    if (valid) {
        valid = (!file.fail()) && (!file.eof());
    }
}

bool RecordFileReader::checkResult(const StatusCode& result) {
    if (isValid()) {
        uint64_t resultCode = readResult();
        if (result.getCode() != resultCode) {
            LOG4IBS_WARN(logger, "Got '" << result.getCode() << "' but expected '" << resultCode << "'");
            return true;
        }
    }
    return false;
}

bool RecordFileReader::checkIntResult(const int result) {
    if (isValid()) {
        uint64_t resultCode = readResult();
        if (static_cast<uint64_t>(result) != resultCode) {
            LOG4IBS_WARN(logger, "Got '" << result << "' but expected '" << resultCode << "'");
            return true;
        }
    }
    return false;
}

bool RecordFileReader::replayOpcode(const uint64_t opcode) {
    bool replayOK = true;
    std::string data;
    std::string oldkey;
    std::string key;
    StatusCode result;
    int intResult;
    int transactionId;
    std::string transactionUuid;
    switch (opcode) {
        case RecorderFile::START_IBS: {
            LOG4IBS_DEBUG(logger, "Stop");
            intResult = controller->start();
            checkIntResult(intResult);
            break;
        }
        case RecorderFile::STOP_IBS: {
            LOG4IBS_DEBUG(logger, "Start");
            intResult = controller->stop();
            checkIntResult(intResult);
            break;
        }

        case RecorderFile::DROP: {
            readData(key);
            LOG4IBS_DEBUG(logger, "Drop Key hash='" << DataChunk(key).hash() << "'");
            result = controller->drop(key);
            checkResult(result);
            break;
        }
        case RecorderFile::PUT: {
            readData(key);
            LOG4IBS_DEBUG(logger, "Put Key hash='" << DataChunk(key).hash() << "'");
            GenerateDataForKey(key, data);
            result = controller->put(key, data);
            if (checkResult(result)) {
                LOG4IBS_DEBUG(logger, "Key hash='" << DataChunk(key).hash() << "'");
            }
            break;
        }
        case RecorderFile::REPLACE: {
            readData(oldkey);
            readData(key);
            LOG4IBS_DEBUG(logger,
                    "Replace OldKey hash='" << DataChunk(oldkey).hash() << "' NewKey hash='" << DataChunk(key).hash()
                            << "'");
            GenerateDataForKey(key, data);
            result = controller->replace(oldkey, key, data);
            if (checkResult(result)) {
                LOG4IBS_DEBUG(logger, "OldKey hash='" << DataChunk(oldkey).hash() << "'");
                LOG4IBS_DEBUG(logger, "NewKey hash='" << DataChunk(key).hash() << "'");
            }
            break;
        }
        case RecorderFile::CREATE_TRANSACTION: {
            transactionId = readId();
            transactionUuid = controller->createTransaction();
            bool transactionIdOK = ibs::Transaction::getIdFromUuid(transactionUuid, transactionId);
            if (transactionIdOK) {
                return transactionId;
            }
            else {
                LOG4IBS_ERROR(logger, "Invalid transactionId=" << transactionId << " while CREATING TRANSACTION");
                replayOK = false;
            }
            break;
        }
        case RecorderFile::COMMIT_TRANSACTION: {
            transactionId = readId();
            bool transactionIdOK = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
            if (transactionIdOK) {
                result = controller->commitTransaction(transactionUuid);
                checkResult(result);
            }
            else {
                LOG4IBS_ERROR(logger, "Invalid transactionId=" << transactionId << " while COMMITTING TRANSACTION");
                replayOK = false;
            }
            break;
        }
        case RecorderFile::ROLLBACK_TRANSACTION: {
            transactionId = readId();
            bool transactionIdOK = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
            if (transactionIdOK) {
                controller->rollbackTransaction(transactionUuid);
            }
            else {
                LOG4IBS_ERROR(logger, "Invalid transactionId=" << transactionId << " while ROLLBACK");
                replayOK = false;
            }
            break;
        }
        case RecorderFile::PUT_TRANSACTION: {
            transactionId = readId();
            readData(key);
            bool transactionIdOK = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
            if (transactionIdOK) {
                LOG4IBS_DEBUG(logger, "PutTx Key hash='" << DataChunk(key).hash() << "' txId=" << transactionId);
                GenerateDataForKey(key, data);
                controller->put(transactionUuid, key, data);
                return replayOK;
            }
            else {
                LOG4IBS_ERROR(logger, "Invalid transactionId=" << transactionId << " while PUT");
                replayOK = false;
            }
            break;
        }
        case RecorderFile::REPLACE_TRANSACTION: {
            transactionId = readId();
            readData(oldkey);
            readData(key);
            bool transactionIdOK = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
            if (transactionIdOK) {
                LOG4IBS_DEBUG(logger,
                        "ReplaceTx OldKey hash='" << DataChunk(oldkey).hash() << "' NewKey hash='"
                                << DataChunk(key).hash() << "' txId=" << transactionId);
                GenerateDataForKey(key, data);
                controller->replace(transactionUuid, oldkey, key, data);
                return replayOK;
            }
            else {
                LOG4IBS_ERROR(logger, "Invalid transactionId=" << transactionId << " while REPLACE");
                replayOK = false;
            }
            break;
        }
        case RecorderFile::GET:
            /* for backward compatibility GET is not in Controller API anymore ... */
        case RecorderFile::FETCH: {
            readData(key);
            LOG4IBS_DEBUG(logger, "Fetch Key hash='" << DataChunk(key).hash() << "'");
            GenerateDataForKey(key, data);
            const size_t readExpectedSize = readExpected();
            size_t expectedSize = data.size();
            if (readExpectedSize != expectedSize) {
                LOG4IBS_DEBUG(logger,
                        "Saved expected size is '" << readExpectedSize << "' but generated data from key size is '"
                                << expectedSize);
            }
            uint64_t resultCode = readResult();
            std::string buffer;
            // allocate buffer size
            if (sliceTooSmallCode == resultCode) {
                // ensure sliceTooSmallCode errors are replayed too
                buffer = std::string(expectedSize / 2, 'a');
            }
            else {
                buffer = std::string(expectedSize, 'a');
            }
            expectedSize = 0;
            std::string keyToFetch = key;
            result = controller->fetch(keyToFetch, buffer, expectedSize);
            if (!result.ok() && (expectedSize != data.size())) {
                // expected is only set if an error occurs
                LOG4IBS_WARN(logger, "Wrong expected size got '" << expectedSize << "' but expected '" << data.size());
            }
            if (result.getCode() != resultCode) {
                warnOnFetchMismatch(result.getCode(), resultCode, DataChunk(key).hash());
            }
            if (result.ok()) {
                if (buffer != data) {
                    LOG4IBS_WARN(logger, "Data integrity check failed in FETCH");
                }
            }
            break;
        }

        default:
            // should not be here
            LOG4IBS_ERROR(logger, "Invalid opcode=" << opcode);
            replayOK = false;
            break;
    }
    return replayOK;
}

void RecordFileReader::warnOnFetchMismatch(uint64_t /*got*/, uint64_t /*expected*/, uint32_t keyHash) {
    std::string resultGot = StatusCode().ToString();
    std::string resultExpected = StatusCode().ToString();
    LOG4IBS_WARN(logger, "Got result='" << resultGot << "'");
    LOG4IBS_WARN(logger, "Expected result='" << resultExpected << "'");
    LOG4IBS_DEBUG(logger, "Key hash='" << keyHash << "'");
}

void RecordFileReader::readCheckMagic() {
    if (isValid()) {
        u_char fetchedMagic[4];
        for (u_short i = 0; i < 4; i++) {
            file >> fetchedMagic[i];
        }
        if (isValid()) {
            valid = (fetchedMagic[0] == 'I') && (fetchedMagic[1] == 'B') && (fetchedMagic[2] == 'S')
                    && (fetchedMagic[3] == 'R');
        }
        updateValidity();
        if (!isValid()) {
            LOG4IBS_ERROR(logger, "The file '" << filename << "' doesn't seems to be in the correct format ?!");
        }
    }
    else {
        LOG4IBS_ERROR(logger, "The file '" << filename << "' seems empty ?!");
    }
}

void RecordFileReader::readEntriesNb() {
    if (isValid()) {
        uint64_t entriesNb = fetchUint64(file);
        updateValidity();
        if (isValid()) {
            entries = entriesNb;
        }
        else {
            LOG4IBS_ERROR(logger, " The file '" << filename << "'seems incomplete while reading entries #");
        }
    }
    else {
        LOG4IBS_ERROR(logger, "The file '" << filename << "' seems corrupted, could not read entries #");
    }
}

static std::unordered_map<std::string, std::string> cache;
static std::unique_ptr<char[]> buffer;
constexpr size_t cacheSize = 1024;

void RecordFileReader::ClearGeneratedDataCache() {
    cache.clear();
    buffer.reset();
}

void RecordFileReader::GenerateDataForKey(const std::string& key, std::string& str, const size_t size, int zeroToInsert,
        bool forceKeyAsData) {
    if (forceKeyAsData) {
        str.assign(key);
    }
    else {
        //only allocate the first time
        if (NULL == buffer.get()) {
            buffer.reset(new char[size]);
        }

        auto it = cache.find(key);

        if (it != cache.end()) {
            str.assign(it->second);
        }
        else {
            static uint seed = DataChunk(key).hash();
            std::default_random_engine generator(seed);
            std::uniform_int_distribution<int> distribution(0, size - 1);
            auto dice = std::bind(distribution, generator);
            char* pt = buffer.get();
            memset(pt, 0, size);
            for (size_t i = 0; i < size;) {
                const size_t limit = (size - key.size());
                if (i <= limit) {
                    memcpy(pt + i, key.data(), key.size());
                }
                i += key.size();
            }
            str.assign(pt, size);
            pt = NULL;
            // insert zero randomly
            for (int i = 0; i < zeroToInsert; i++) {
                size_t index = dice() % size;
                str[index] = 0;
            }
        }
        if (cache.size() > cacheSize) {
            cache.clear();
        }
        cache[key] = str;
    }
}

std::string RecordFileReader::IntToString(const int value) {
    std::ostringstream ss;
    ss << value;
    return ss.str();
}

std::string RecordFileReader::GenerateRandomTemporaryFileName() {
    constexpr const char* PREFIX = "IBSREPLAY_";
    const size_t nbRamdomChar = 20;
    std::vector<char> valid = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    valid.shrink_to_fit();
    std::string randomPart;
    for (size_t i = 0; i < nbRamdomChar; i++) {
        randomPart += valid[rand() % valid.size()];
    }
    std::string dir("/tmp/");
    std::string prefix(PREFIX);
    std::string name = dir + prefix + randomPart;
    return name;
}

void RecordFileReader::override(ConfigFileReader* config, std::map<std::string, std::string>& cfg,
        const std::string& key) {
    std::string value = config->getString(key);
    if (!value.empty()) {
        cfg[key] = value;
    }
}

} /* namespace replay  */
} /* namespace ibs  */

