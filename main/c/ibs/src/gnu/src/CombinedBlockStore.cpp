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
 * @file CombinedBlockStore.cpp
 * @brief The combined block store implementation/source
 * @author j. caba
 */

#include "CombinedBlockStore.h"
#include <assert.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include "LevelDbFacade.h"

namespace ibs {

Logger_t CombinedBlockStore::logger = ibs::Logger::getLogger("CombinedBlockStore");
Logger_t CombinedBSAtomicUpdate::logger = ibs::Logger::getLogger("CombinedBSAtomicUpdate");

// ----------------------------------------
// CombinedBSAtomicUpdate code
// ----------------------------------------

CombinedBSAtomicUpdate::CombinedBSAtomicUpdate(const CombinedBlockStore& parent, bool dropOld) :
        owner(parent) {
    dropOldKey = dropOld;
    ibpBatch.reserve(owner.getSize());
    for (size_t i = 0; i < owner.getSize(); ++i) {
        ibpBatch.emplace_back(new LevelDbAtomicUpdate());
    }
}

CombinedBSAtomicUpdate::~CombinedBSAtomicUpdate() {
    while (!ibpBatch.empty()) {
        ibpBatch.back().reset();
        ibpBatch.pop_back();
    }
    ibpBatch.clear();
}

void CombinedBSAtomicUpdate::put(const DataChunk& key, const DataChunk& value, bool compress) {
    atomicProtection.lock();
    notAtomicPut(key, value, compress);
    atomicProtection.unlock();
}

void CombinedBSAtomicUpdate::drop(const DataChunk& key) {
    atomicProtection.lock();
    notAtomicDrop(key);
    atomicProtection.unlock();
}

void CombinedBSAtomicUpdate::replace(const DataChunk& oldKey, const DataChunk& newkey, const DataChunk& value,
bool compress) {
    atomicProtection.lock();
    if (dropOldKey) {
        notAtomicDrop(oldKey);
    }
    else {
        LOG4IBS_DEBUG(logger, "Drop of old key disabled.");
    }
    notAtomicPut(newkey, value, compress);
    atomicProtection.unlock();
}

void CombinedBSAtomicUpdate::clear() {
    atomicProtection.lock();
    for (auto& batch : ibpBatch) {
        batch->clear();
    }
    atomicProtection.unlock();
}

void CombinedBSAtomicUpdate::notAtomicPut(const DataChunk& key, const DataChunk& value, bool compress) {
    CombinedBlockStore& parent = const_cast<CombinedBlockStore&>(owner);
    const DataChunk& keyToCheck(key);
    if (!parent.contains(std::move(keyToCheck))) {
        std::string toStore = value.toString();
        if (compress) {
            DataChunk toCompress(toStore);
            std::string compressed = toCompress.toCompressedString();
            toStore.assign(compressed.data(), compressed.size());
        }
        int i = owner.computeId(key);
        if (i != -1) {
            ibpBatch[i]->put(key, toStore);
        }
        else {
            LOG4IBS_ERROR(logger, "Could not put data in batch");
        }
    }
    else {
        LOG4IBS_DEBUG(logger, "key='" << key.hash() << "' is already in db.");
    }
}

void CombinedBSAtomicUpdate::notAtomicDrop(const DataChunk& key) {
    int i = owner.computeId(key);
    if (i != -1) {
        ibpBatch[i]->drop(key);
    }
    else {
        LOG4IBS_ERROR(logger, "Could not drop data from batch");
    }
}

// ----------------------------------------
// CombinedBlockStore code
// ----------------------------------------

CombinedBlockStore::CombinedBlockStore(const std::vector<std::string>& paths) :
        CompressibleBlockStore(), Destroyable(), Reparable(), isOpen(false), dbs(), protectCreateOpenClose(), atomicReplace(), protectAtomicWrite(), protectCompressionChange(), protectDestroyRepair(), protectCompactRange() {
    protectCreateOpenClose.lock();
    for (auto& dbPath : paths) {
        dbs.emplace_back(createConcrete(dbPath));
    }
    protectCreateOpenClose.unlock();
}

AtomicBlockStore* CombinedBlockStore::createConcrete(const std::string& dbPath) {
    return new LevelDbFacade(dbPath, true, true, true);
}

CombinedBlockStore::~CombinedBlockStore() {
    close();
    protectCreateOpenClose.lock();
    while (!dbs.empty()) {
        dbs.back().reset();
        dbs.pop_back();
    }
    protectCreateOpenClose.unlock();
}

void CombinedBlockStore::close() {
    if (isOpen.exchange(false) == true) {
        protectCreateOpenClose.lock();
        for (auto& db : dbs) {
            db->close();
        }
        protectCreateOpenClose.unlock();
    }
}

StatusCode CombinedBlockStore::open() {
    if (isOpen.exchange(true) == true) {
        return StatusCode::NotSupported();
    }
    else {
        protectCreateOpenClose.lock();
        // TODO: merge result of open
        // - on failure, close opened db and udpate isOpen
        for (auto& db : dbs) {
            db->open();
        }
        protectCreateOpenClose.unlock();
        return StatusCode::OK();
    }
}

uint32_t CombinedBlockStore::pseudoHash(const DataChunk& key) noexcept {
    LOG4IBS_TRACE(logger, "pseudoHash of key='" << key.hash() << "'");
    constexpr size_t sizeOfTypeUsed = sizeof(uint32_t);
    size_t keySize = key.getSize();
    if (keySize < sizeOfTypeUsed) {
        size_t sum = 0;
        for (size_t i = 0; i < keySize; i++) {
            sum += key[i];
        }
        return sum;
    }

    // Get integer
    size_t whereToGetInt = (keySize - sizeOfTypeUsed) / 2;
    return key.toInt(whereToGetInt); /* use 4 bytes in the middle */
}

uint32_t CombinedBlockStore::computeId(const DataChunk& key) const noexcept {
    LOG4IBS_TRACE(logger, "computeId of key='" << key.hash() << "'");
    size_t nbIbp = getSize();

    uint32_t id = pseudoHash(key);

    // get an index in [0 ; nbIbp [ interval
    // for table lookup
    id %= nbIbp;

    return id;
}

StatusCode CombinedBlockStore::checkAndComputeIdFromKey(const DataChunk& key, int& i) noexcept {
    LOG4IBS_TRACE(logger, "checkAndComputeIdFromKey of key='" << key.hash() << "'");
    i = computeId(key); /* i-th Ibp that will be used */
    if (i == -1) {
        LOG4IBS_WARN(logger, "No IBP available to handle request.");
        assert(i == -1);
        return StatusCode::NotSupported();
    }
    else {
        LOG4IBS_DEBUG(logger, "IBP " << i << " available to handle request.");
    }
    assert(i != -1);
    return StatusCode::OK();
}

StatusCode CombinedBlockStore::put(const DataChunk&& key, const DataChunk&& value) {
    LOG4IBS_TRACE(logger, "put of key='" << key.hash() << "'");
    int i;
    StatusCode st = checkAndComputeIdFromKey(key, i);
    if (st.ok()) {
        return dbs[i]->put(std::move(key), std::move(value));
    }
    else {
        return st;
    }
}

StatusCode CombinedBlockStore::get(const DataChunk&& key, std::string* value) {
    LOG4IBS_TRACE(logger, "get of key='" << key.hash() << "'");
    int i;
    StatusCode st = checkAndComputeIdFromKey(key, i);
    if (st.ok()) {
        return dbs[i]->get(std::move(key), value);
    }
    else {
        return st;
    }
}

bool CombinedBlockStore::contains(const DataChunk&& key) {
    int i;
    StatusCode st = checkAndComputeIdFromKey(key, i);
    if (st.ok()) {
        return dbs[i]->contains(std::move(key));
    }
    else {
        return false;
    }
}

StatusCode CombinedBlockStore::drop(const DataChunk&& key) {
    int i;
    StatusCode st = checkAndComputeIdFromKey(key, i);
    if (st.ok()) {
        return dbs[i]->drop(std::move(key));
    }
    else {
        return st;
    }
}

StatusCode CombinedBlockStore::atomicWrite(AbstractAtomicUpdate& updates) {
    protectAtomicWrite.lock();
    AbstractAtomicUpdate* pointer = &updates;
    assert(pointer != NULL);
    CombinedBSAtomicUpdate* updatesToApply = static_cast<CombinedBSAtomicUpdate*>(pointer);
    assert(updatesToApply != NULL);

    StatusCode errCode = StatusCode::OK();
    for (size_t i = 0; i < updatesToApply->ibpBatch.size(); ++i) {
        AbstractAtomicUpdate* batch = updatesToApply->ibpBatch[i].get();
        assert(batch != NULL);
        StatusCode st = dbs[i]->atomicWrite(*batch);
        if (!st.ok()) {
            // report first error
            errCode = st;
        }
    }
    protectAtomicWrite.unlock();
    return errCode;
}

bool CombinedBlockStore::getStats(std::string& output) {
    std::ostringstream allStats;
    allStats << "Stats : " << std::endl << std::endl;
    const size_t nbBS = dbs.size();
    for (size_t i = 0; i < nbBS; ++i) {
        std::string ibpStats;
        StorableBlockStore* storableDB = dynamic_cast<StorableBlockStore*>(dbs[i].get());
        if (storableDB != NULL) {
            bool ibpOk = storableDB->getStats(ibpStats);
            if (ibpOk) {
                allStats << "Stats for '" << storableDB->getStoragePath() << "'" << std::endl;
                allStats << ibpStats << std::endl;
            }
            else {
                allStats << "Could not get stats for '" << storableDB->getStoragePath() << "'" << std::endl;
            }
        }
    }

    output = allStats.str();
    return true;
}

void CombinedBlockStore::enableCompression() {
    assert(isClosed());
    protectCompressionChange.lock();
    for (auto& db : dbs) {
        CompressibleBlockStore* compressible = dynamic_cast<CompressibleBlockStore*>(db.get());
        if (NULL != compressible) {
            compressible->enableCompression();
        }
    }
    protectCompressionChange.unlock();
}

void CombinedBlockStore::disableCompression() {
    assert(isClosed());
    protectCompressionChange.lock();
    for (auto& db : dbs) {
        CompressibleBlockStore* compressible = dynamic_cast<CompressibleBlockStore*>(db.get());
        if (NULL != compressible) {
            compressible->disableCompression();
        }
    }
    protectCompressionChange.unlock();
}

StatusCode CombinedBlockStore::destroy() {
    if (!isClosed()) {
        return StatusCode::NotSupported();
    }
    else {
        protectDestroyRepair.lock();
        for (auto& db : dbs) {
            StorableBlockStore* storable = dynamic_cast<StorableBlockStore*>(db.get());
            if (NULL != storable) {
                storable->destroy();
            }
        }
        protectDestroyRepair.unlock();
        return StatusCode::OK();
    }
}

StatusCode CombinedBlockStore::repair() {
    if (!isClosed()) {
        return StatusCode::NotSupported();
    }
    else {
        protectDestroyRepair.lock();
        for (auto& db : dbs) {
            ReparableBlockStore* storable = dynamic_cast<ReparableBlockStore*>(db.get());
            if (NULL != storable) {
                storable->repair();
            }
        }
        protectDestroyRepair.unlock();
        return StatusCode::OK();
    }
}

void CombinedBlockStore::setBlockSize(uint64_t blockSize) {
    for (auto& db : dbs) {
        LevelDbFacade* ldb = dynamic_cast<LevelDbFacade*>(db.get());
        if (ldb) {
            ldb->setBlockSize(blockSize);
        }
    }
}

void CombinedBlockStore::setBlockRestartInterval(uint64_t blockRestartInterval) {
    for (auto& db : dbs) {
        LevelDbFacade* ldb = dynamic_cast<LevelDbFacade*>(db.get());
        if (ldb) {
            ldb->setBlockRestartInterval(blockRestartInterval);
        }
    }
}

void CombinedBlockStore::setWriteBufferSize(uint64_t writeBufferSize) {
    for (auto& db : dbs) {
        LevelDbFacade* ldb = dynamic_cast<LevelDbFacade*>(db.get());
        if (ldb) {
            ldb->setWriteBufferSize(writeBufferSize);
        }
    }
}

void CombinedBlockStore::setCacheSize(uint64_t cacheSize) {
    for (auto& db : dbs) {
        LevelDbFacade* ldb = dynamic_cast<LevelDbFacade*>(db.get());
        if (ldb) {
            ldb->setCacheSize(cacheSize);
        }
    }
}

}
/* namespace ibs */
