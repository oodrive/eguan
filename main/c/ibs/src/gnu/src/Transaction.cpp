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
 * @file Transaction.cpp
 * @brief The transaction implementation/source
 * @author j. caba
 */

#include "Transaction.h"
#include "CombinedBlockStore.h"
#include "IbpHandler.h"
#include "IbpGenHandler.h"

namespace ibs {

Logger_t Transaction::logger = ibs::Logger::getLogger("Transaction");
Transaction::uuidToId_t Transaction::uuidToId;
Transaction::idToUuid_t Transaction::idToUuid;
PosixRWLock Transaction::mapLock;
long Transaction::global_counter = 1;

Transaction::Transaction(AtomicBlockStore& in) :
        AbstractTransaction(in), updates(), uuid(), id(), isIdBinded(false), compress(false) {
    AtomicBlockStore* pointer = &in;
    assert(pointer != NULL);

    IbpHandler* ibp = dynamic_cast<IbpHandler*>(pointer);
    IbpGenHandler* ibpgen = dynamic_cast<IbpGenHandler*>(pointer);
    if (ibpgen != NULL) {
        updates.reset(new IbpGenAtomicUpdate());
    }
    else if (ibp != NULL) {
        updates.reset(new CombinedBSAtomicUpdate(*ibp->ibpSet.get(), false));
        compress = ibp->needToCompressTransaction();
    }
    else {
        // should not happen ...
        assert(false);
    }
    uuid = updates->getUuid();
    bindId();
}

Transaction::~Transaction() {
    updates.reset();
    unbindId();
}

int Transaction::getId() const {
    return id;
}

bool Transaction::getUuidFromId(int id, std::string& outputUuid) {
    bool isValid = false;
    mapLock.writeLock();
    auto it = idToUuid.find(id);
    if (it != idToUuid.end()) {
        outputUuid = it->second;
        isValid = true;
    }
    mapLock.unlock();
    return isValid;
}

bool Transaction::getIdFromUuid(const std::string& uuid, int& outputId) {
    bool isValid = false;
    mapLock.writeLock();
    auto it = uuidToId.find(uuid);
    if (it != uuidToId.end()) {
        outputId = it->second;
        isValid = true;
    }
    mapLock.unlock();
    return isValid;
}

const std::string& Transaction::getUuid() const {
    return uuid;
}

bool Transaction::put(const DataChunk& key, const DataChunk& value) {
    bool alreadyAdded = contains(key);
    updates->put(std::move(key), std::move(value), compress);
    return alreadyAdded;
}

void Transaction::drop(const DataChunk& key) {
    updates->drop(std::move(key));
}

bool Transaction::replace(const DataChunk& oldKey, const DataChunk& newKey, const DataChunk& value) {
    bool alreadyAdded = contains(newKey);
    if (oldKey == newKey) {
        // nothing to do, only warn user
        LOG4IBS_WARN(logger,
                "Detected replace of identical old and new key during transaction id=" << getId() << " ?!");
    }
    else {
        updates->replace(std::move(oldKey), std::move(newKey), std::move(value), compress);
    }
    return alreadyAdded;
}

bool Transaction::contains(const DataChunk& key) {
    return db.contains(std::move(key));
}

void Transaction::rollback() {
    updates->clear();
    unbindId();
}

StatusCode Transaction::commit() {
    StatusCode resCode = db.atomicWrite(*updates);
    rollback(); //clear pending changes
    return resCode;
}

void Transaction::incrementCounter() {
    if (global_counter == INT64_MAX) {
        global_counter = 1;
    }
    else {
        ++global_counter;
    }
}

int Transaction::generateId(const std::string& uuidToMap) {
    mapLock.writeLock();
    auto it = idToUuid.find(global_counter);
    while (it != idToUuid.end()) {
        incrementCounter();
        it = idToUuid.find(global_counter);
    }
    assert(it == idToUuid.end());
    int64_t generated = global_counter;
    idToUuid[generated] = uuidToMap;
    uuidToId[uuidToMap] = generated;
    mapLock.unlock();
    return generated;
}

void Transaction::releaseId(int idToRelease) {
    mapLock.writeLock();
    auto it1 = idToUuid.find(idToRelease);
    std::string uuidToRelease;
    if (it1 != idToUuid.end()) {
        uuidToRelease = it1->second;
        idToUuid.erase(it1);
        auto it2 = uuidToId.find(uuidToRelease);
        if (it2 != uuidToId.end()) {
            uuidToId.erase(it2);
        }
    }
    mapLock.unlock();
}

void Transaction::bindId() {
    id = generateId(uuid);
    isIdBinded = true;
}

void Transaction::unbindId() {
    bool expect = true;
    if (isIdBinded.compare_exchange_strong(expect, false)) {
        releaseId(id);
    }
}

} /* namespace ibs */
