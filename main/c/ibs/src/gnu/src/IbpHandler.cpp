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
 * @file IbpHandler.cpp
 * @brief Ibp handler implementation/header
 * @author j. caba
 */

#include "Constants.h"
#include "IbpHandler.h"
#include "CombinedBlockStore.h"
#include "ConfigFileReader.h"
#include "StatusCode.h"
#include "Logger.h"

#include <sys/sysinfo.h>
#include <snappy.h>

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("IbpHandler");

IbpHandler::IbpHandler(ConfigFileReader& cfg) :
        ibpSet(), ibpPaths(), config(cfg), compression(CompressionType::noCompression) {
    // create ibp in memory
    std::vector<std::string> paths;
    this->config.getTokenizedString(IBP_PATH, paths);
    ibpSet.reset(new CombinedBlockStore(paths));

    initCompressionOption();
    initIbps(paths);
}

void IbpHandler::initCompressionOption() noexcept {
    // check compression status
    if (this->config.getString(COMPRESSION) == FRONT_COMPRESSION) {
        this->compression = CompressionType::frontCompression;
        LOG4IBS_DEBUG(logger, "Front-end compression enabled");
    }
    else if (this->config.getString(COMPRESSION) == BACK_COMPRESSION) {
        this->compression = CompressionType::backCompression;
        LOG4IBS_DEBUG(logger, "Back-end compression enabled");
    }
    else {
        LOG4IBS_DEBUG(logger, "Compression disabled");
        this->compression = CompressionType::noCompression;
    }
}

static uint64_t getTotalPhysicalMemory() {
    struct sysinfo info;
    sysinfo(&info);
    return static_cast<uint64_t>(info.totalram) * static_cast<uint64_t>(info.mem_unit);
}

void IbpHandler::initEngineOptions() noexcept {
    uint64_t blockSize;
    if (this->config.getInt(LDB_BLOCK_SIZE) > 0) {
        blockSize = this->config.getInt(LDB_BLOCK_SIZE);
        LOG4IBS_INFO(logger, "leveldb advanced parameter set block_size=" << blockSize);
    }
    else {
        blockSize = BLOCK_SIZE_DEF;
        LOG4IBS_DEBUG(logger, "Using leveldb default parameter block_size='" << blockSize);
    }
    this->config.setInt(LDB_BLOCK_SIZE, blockSize);
    ibpSet->setBlockSize(blockSize);

    uint64_t blockRestartInterval;
    if (this->config.getInt(LDB_BLOCK_RESTART_INVERVAL) > 0) {
        blockRestartInterval = this->config.getInt(LDB_BLOCK_RESTART_INVERVAL);
        LOG4IBS_INFO(logger, "leveldb advanced parameter set block_restart_interval=" << blockRestartInterval);
    }
    else {
        blockRestartInterval = LDB_BLOCK_RESTART_INVERVAL_DEF;
        LOG4IBS_DEBUG(logger, "Using leveldb default parameter block_restart_interval=" << blockRestartInterval);
    }
    this->config.setInt(LDB_BLOCK_RESTART_INVERVAL, blockRestartInterval);
    ibpSet->setBlockRestartInterval(blockRestartInterval);

    // configure leveldb values for ibp databases
    uint64_t indicated_ram_size = this->config.getInt(INDICATED_RAM_SIZE);
    uint64_t writeBufferSizeForIbp = WRITE_BUFFER_SIZE_FOR_IBP_DEF;
    if (indicated_ram_size == 0) {
        indicated_ram_size = getTotalPhysicalMemory() * RAM_FRACTION_DEF;
        LOG4IBS_DEBUG(logger, "No ram size indicated, will use default value of "
                << indicated_ram_size << " for ibp databases ...");
    }
    else {
        // automatic configuration of write buffers
        LOG4IBS_DEBUG(logger, "Ram size indicated=" << indicated_ram_size << " for IBS");
    }
    writeBufferSizeForIbp = optimizeCacheSizeForIbp(indicated_ram_size, WRITE_BUFFER_SIZE_FOR_IBP_DEF);
    LOG4IBS_DEBUG(logger, "Will use " << writeBufferSizeForIbp << " byte for each Ibp write buffer.");
    ibpSet->setWriteBufferSize(writeBufferSizeForIbp);

    uint64_t readCacheSizeForIbp = READ_CACHE_SIZE_FOR_IBP_DEF;
    if (indicated_ram_size != 0) {
        readCacheSizeForIbp = optimizeCacheSizeForIbp(indicated_ram_size, READ_CACHE_SIZE_FOR_IBP_DEF);
    }
    // optimize read cache when space is sufficient
    LOG4IBS_DEBUG(logger, "Will use " << readCacheSizeForIbp << " byte for each Ibp read cache.");
    ibpSet->setCacheSize(readCacheSizeForIbp);
}

void IbpHandler::initIbps(std::vector<std::string>& ibpPathList) noexcept {
    // Parse IBP directory list, comma separated
    LOG4IBS_INFO(logger, "Reading IBP path :");

    std::string ibsOwner = this->config.getString(IBS_OWNER);
    std::string ibsUuid = this->config.getString(IBS_UUID);

    int ibpIndex = 1; // Important to pick 1 because 0 is the "error" value of the configurator
    int ibpCount = ibpPathList.size();
    for (auto eachPath : ibpPathList) {
        // generate and save Signature file in each ibp
        std::ostringstream sigfile;
        sigfile << eachPath << '/' << SIGNATURE_FILE;
        std::unique_ptr<ConfigFileReader> ibpSignature(new ConfigFileReader(std::string()));
        ibpSignature->setString(IBS_OWNER, ibsOwner);
        ibpSignature->setString(IBS_UUID, ibsUuid);
        ibpSignature->setInt(IBP_ID, ibpIndex);
        ibpSignature->setInt(IBP_NB, ibpCount); // We need to know the number of Ibp to detect configuration mismatch
        ibpSignature->write(sigfile.str());
        ibpSignature.reset();

        ++ibpIndex;
    }

    // set compression in IBP before open
    if (this->compression == CompressionType::backCompression) {
        ibpSet->enableCompression();
    }
    else {
        ibpSet->disableCompression();
    }

    // read and set options of block store engine from configuration
    initEngineOptions();
}

IbpHandler::~IbpHandler() {
    // delete ibp from memory
    ibpSet.reset();
}

StatusCode IbpHandler::destroy() {
    // destroy IBP from disk.
    ibpSet->close();
    ibpSet->destroy();
    return StatusCode::OK();
}

StatusCode IbpHandler::open() {
    // create IBP on disk.
    return ibpSet->open();
}

void IbpHandler::close() {
    ibpSet->close();
}

bool IbpHandler::isClosed() {
    return ibpSet->isClosed();
}

StatusCode IbpHandler::put(const DataChunk&& key, const DataChunk&& value) {
    LOG4IBS_TRACE(logger, "put of key='" << key.hash() << "'");
    if (contains(std::move(key))) {
        LOG4IBS_DEBUG(logger, "key'" << key.hash() << "' already added");
        return StatusCode::KeyAreadyAdded();
    }
    StatusCode status;
    if (this->compression == CompressionType::frontCompression) {
        std::string compressed = value.toCompressedString();
        status = processPut(key, compressed);
    }
    else {
        status = processPut(key, value);
    }
    return status;
}

StatusCode IbpHandler::replace(const DataChunk&& /*oldKey*/, const DataChunk&& newKey, const DataChunk&& value) {
    // Old key is not removed in IBP
    // so a replace is equivalent to a PUT in ibp.
    return put(std::move(newKey), std::move(value));
}

StatusCode IbpHandler::get(const DataChunk&& key, std::string* value) {
    LOG4IBS_TRACE(logger, "get of key='" << key.hash() << "'");
    StatusCode status;

    if (this->compression == CompressionType::frontCompression) {
        std::string record;
        status = this->processGet(key, &record);
        if (status.ok()) {
            DataChunk toUncompress(record);
            std::string uncompressed = toUncompress.toUncompressedString();
            value->assign(uncompressed.data(), uncompressed.size());
        }
    }
    else {
        status = this->processGet(key, value);
    }
    return status;
}

bool IbpHandler::contains(const DataChunk&& key) {
    return ibpSet->contains(std::move(key));
}

StatusCode IbpHandler::drop(const DataChunk&& key) {
    return ibpSet->drop(std::move(key));
}

bool IbpHandler::getStats(std::string& output) {
    return ibpSet->getStats(output);
}

uint64_t IbpHandler::optimizeCacheSizeForIbp(const uint64_t usable_ram, const uint64_t minValue) const {
    const uint64_t nbIbp = ibpSet->getSize();
    const uint64_t writeRamUsage = usable_ram / nbIbp;
    constexpr uint64_t maxValue = 1 << 30;
    uint64_t res = std::max(std::min(writeRamUsage / nbIbp, maxValue), minValue);
    return res;
}

bool IbpHandler::needToCompressTransaction() const {
    return this->compression == CompressionType::frontCompression;
}

StatusCode IbpHandler::processPut(const DataChunk& key, const DataChunk& value) noexcept {
    LOG4IBS_TRACE(logger, "processPut of key='" << key.hash() << "'");
    return ibpSet->put(std::move(key), std::move(value));
}

StatusCode IbpHandler::processGet(const DataChunk& key, std::string* value) noexcept {
    LOG4IBS_TRACE(logger, "processGet of key='" << key.hash() << "'");
    return ibpSet->get(std::move(key), value);
}

StatusCode IbpHandler::atomicWrite(AbstractAtomicUpdate& updates) {
    return ibpSet->atomicWrite(updates);
}

} /* namespace ibs */
