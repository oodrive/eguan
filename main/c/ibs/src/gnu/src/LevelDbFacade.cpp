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
 * @file LevelDbFacade.cpp
 * @brief The IBS leveldb facade/source
 * @author j. caba
 */

#include "LevelDbFacade.h"
#include "FileTools.h"
#include "Constants.h"

namespace ibs {

Logger_t LevelDbFacade::logger = ibs::Logger::getLogger("LevelDbFacade");
leveldb::Options LevelDbFacade::defaultOptions; // to allocate default environment

LevelDbAtomicUpdate::~LevelDbAtomicUpdate() = default;

void LevelDbAtomicUpdate::put(const DataChunk& key, const DataChunk& value, bool /*compress*/) {
    batch.Put(LevelDbFacade::toSlice(key), LevelDbFacade::toSlice(value));
}

void LevelDbAtomicUpdate::drop(const DataChunk& key) {
    batch.Delete(LevelDbFacade::toSlice(key));
}

leveldb::Slice LevelDbFacade::toSlice(const DataChunk& chunk) noexcept {
    return leveldb::Slice(chunk.getData(), chunk.getSize());
}

StatusCode LevelDbFacade::fromStatus(const leveldb::Status& st) noexcept {
    StatusCode res;
    if (st.ok()) {
        res = StatusCode::OK();
    }
    else if (st.IsCorruption()) {
        res = StatusCode::Corruption();
    }
    else if (st.IsIOError()) {
        res = StatusCode::IOError();
    }
    else if (st.IsNotFound()) {
        res = StatusCode::KeyNotFound();
    }
    else {
        res = StatusCode(StatusCode::K_UNKOWN);
    }
    return res;
}

LevelDbFacade::LevelDbFacade(const std::string& path, bool createCache, bool createBloomFilter, bool isCompressed) :
        ReparableBlockStore(), hasCache(createCache), hasBloomFilter(createBloomFilter), isOpen(false), db(), cache(), filter(), wOptions(), rOptions(), options(
                defaultOptions), leveldbPath(path), isCompacting(false) {
    options.create_if_missing = true;
    if (isCompressed) {
        options.compression = leveldb::kSnappyCompression;
    }
    else {
        options.compression = leveldb::kNoCompression;
    }
    if (hasCache) {
        // improve read performance using a cache
        constexpr uint64_t cacheSize = READ_CACHE_SIZE_FOR_IBP_DEF;
        this->setCacheSize(cacheSize);
    }
    else {
        // if not defined a default 8MB internal cache
        LOG4IBS_INFO(logger, "Use default leveldb block's read cache");
    }
    if (hasBloomFilter) {
        // Use a bloom filters to reduce the disk lookups.
        // This is very important and the main reason of speedup
        // when data already exists and is persisted in IBS.
        //
        // Explanation of how to decide the size of the bloom
        // filter in number of bits according to the false positive
        // rate desired for the bloom filter in leveldb.
        //
        // Using the following formula for bloom filter :
        // (A) m = - ( n * ln(p) ) / (ln(2)*ln(2))
        // where n is the maximum cardinality of bloom filter
        // p the false rate (desired)
        // m the number of bits of the bloom filter
        // The bloom filter maximum of leveldb cardinality
        // can be estimated based on the information
        // that 10 bits offer ~ 1% rate
        // according to leveldb documentation,
        // and the approximation that ~ 1% = 1 + 1e-15 %
        //
        // Thus using the previous (A) formula
        // we have following (B) formula
        // that gives the number of bits required given a desired
        // false rate p.
        //
        // (B) (((10*ln(2)*ln(2))/ln((1 + 1e-15)/100))  * ln(p/100))/(ln(2)*ln(2))
        //
        // p = 1%       => 10 bits
        // p = 0.5%     => 11 bits
        // p = 0,1%     => 15 bits
        // p = 0,01%    => 20 bits
        // p = 0,001%   => 25 bits
        // p = 0,0001%  => 30 bits
        // p = 0,00001% => 35 bits
        //
        filter.reset(leveldb::NewBloomFilterPolicy(30));
        assert(NULL != filter.get());
        options.filter_policy = filter.get();
    }
    constexpr uint64_t bufferSize = CACHE_MIN_VALUE;
    options.write_buffer_size = bufferSize;
}

LevelDbFacade::~LevelDbFacade() {
    // release allocated memory
    options.block_cache = NULL;
    options.filter_policy = NULL;
    close();
    db.reset();
    cache.reset();
    filter.reset();
}

void LevelDbFacade::close() {
    // delete the associated leveldb instance to close
    isOpen = false;
    db.reset();
    isCompacting = false;
}

void LevelDbFacade::enableCompression() {
    options.compression = leveldb::kSnappyCompression;
}

void LevelDbFacade::disableCompression() {
    options.compression = leveldb::kNoCompression;
}

void LevelDbFacade::setBlockSize(uint64_t blockSize) {
    options.block_size = blockSize;
}

void LevelDbFacade::setBlockRestartInterval(uint64_t blockRestartInterval) {
    options.block_restart_interval = blockRestartInterval;
}

void LevelDbFacade::setWriteBufferSize(uint64_t writeBufferSize) {
    options.write_buffer_size = writeBufferSize;
}

void LevelDbFacade::setCacheSize(uint64_t cacheSize) {
    assert(isOpen == false); // if done after open will not be taken in account
    cache.reset(leveldb::NewLRUCache(cacheSize));
    assert(NULL != cache.get());
    options.block_cache = cache.get();
}

StatusCode LevelDbFacade::open() {
    if (isOpen.exchange(true) == true) {
        return StatusCode::NotSupported();
    }
    else {
        leveldb::DB* dbToAllocate = NULL;
        leveldb::Status status;
        leveldb::DB::Open(options, leveldbPath, &dbToAllocate);

        if (status.ok() == false) {
            //TODO: this should never happened if leveldb does it's job
            // remove if not useful
            if (dbToAllocate != NULL) {
                delete dbToAllocate;
                dbToAllocate = NULL;
            }
            // Must close to reset internal state
            close();
            return fromStatus(status);
        }
        else {
            assert(status.ok());
            //TODO: modify if leveldb change this behavior
            // for now leveldb does not support multi process open
            if (dbToAllocate == NULL) {
                close();
                return StatusCode::NotSupported();
            }
            // pass the memory to the class
            // in the smart pointer
            db.reset(dbToAllocate);
            return StatusCode::OK();
        }
    }
}

StatusCode LevelDbFacade::put(const DataChunk&& key, const DataChunk&& value) {
    LOG4IBS_TRACE(logger, "put of key='" << key.hash() << "'");
    if (isOpen) {
        return fromStatus(db->Put(wOptions, toSlice(key), toSlice(value)));
    }
    else {
        return StatusCode::NotSupported();
    }
}

StatusCode LevelDbFacade::atomicWrite(AbstractAtomicUpdate& updates) {
    if (isOpen) {
        AbstractAtomicUpdate* pointer = &updates;
        assert(pointer != NULL);
        LevelDbAtomicUpdate* updatesToApply = static_cast<LevelDbAtomicUpdate*>(pointer);
        assert(updatesToApply != NULL);

        leveldb::WriteOptions batchOptions(wOptions);
        // writes are synchronous to ensure everything goes to disk.
        batchOptions.sync = true;
        return fromStatus(db->Write(batchOptions, &updatesToApply->batch));
    }
    else {
        return StatusCode::NotSupported();
    }
}

StatusCode LevelDbFacade::get(const DataChunk&& key, std::string* value) {
    LOG4IBS_TRACE(logger, "get of key='" << key.hash() << "'");
    if (isOpen) {
        return fromStatus(db->Get(rOptions, toSlice(key), value));
    }
    else {
        return StatusCode::NotSupported();
    }
}

bool LevelDbFacade::contains(const DataChunk&& key) {
    LOG4IBS_TRACE(logger, "contains of key='" << key.hash() << "'");
    if (isOpen) {
        leveldb::Status status = db->Contains(rOptions, toSlice(key));
        return status.ok();
    }
    else {
        return false;
    }
}

StatusCode LevelDbFacade::drop(const DataChunk&& key) {
    LOG4IBS_TRACE(logger, "drop of key='" << key.hash() << "'");
    if (isOpen) {
        return fromStatus(db->Delete(wOptions, toSlice(key)));
    }
    else {
        return StatusCode::NotSupported();
    }
}

StatusCode LevelDbFacade::destroy() {
    if (isOpen) {
        return StatusCode::NotSupported();
    }
    else {
        leveldb::Status status;
        status = leveldb::DestroyDB(leveldbPath, options);
        FileTools::removeDirectory(leveldbPath);
        return fromStatus(status);
    }
}

StatusCode LevelDbFacade::repair() {
    if (isOpen) {
        return StatusCode::NotSupported();
    }
    else {
        return fromStatus(leveldb::RepairDB(leveldbPath, options));
    }
}

} /* namespace ibs */
