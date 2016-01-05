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
 * @file IbsHandler.cpp
 * @brief Ibs handler implementation/source
 * @author j. caba
 */

#include "IbsHandler.h"
#include "IbpHandler.h"
#include "IbpGenHandler.h"
#include "ConfigFileReader.h"
#include "Constants.h"
#include "Logger.h"
#include "Transaction.h"

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("IbsHandler");

IbsHandler::IbsHandler(ConfigFileReader& cfg) :
        AtomicBlockStore(), hotDataEnabled(false), pImpl() {
    // 1) check if hot data is enabled
    std::string hotdataOption = cfg.getString(HOT_DATA);
    if ((hotdataOption == YES) || hotdataOption.empty()) {
        hotDataEnabled = true;
        LOG4IBS_DEBUG(logger, "Hot data enabled");
    }
    else {
        hotDataEnabled = false;
        LOG4IBS_DEBUG(logger, "Hot data disabled");
    }
    // 2) choose implementation
    if (hotDataEnabled) {
        pImpl.reset(new IbpGenHandler(cfg));
    }
    else {
        pImpl.reset(new IbpHandler(cfg));
    }
    replaceable = dynamic_cast<Replaceable*>(pImpl.get());
    assert(replaceable != NULL);
}

bool IbsHandler::isHotDataEnabled() {
    return hotDataEnabled;
}

IbsHandler::~IbsHandler() {
    cleanTransaction();

    // release memory
    pImpl.reset();
}

StatusCode IbsHandler::open() {
    return pImpl->open();
}

void IbsHandler::close() {
    pImpl->close();
}

bool IbsHandler::isClosed() {
    return pImpl->isClosed();
}

StatusCode IbsHandler::put(const DataChunk&& key, const DataChunk&& value) {
    return pImpl->put(std::move(key), std::move(value));
}

StatusCode IbsHandler::replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) {
    return replaceable->replace(std::move(oldKey), std::move(newKey), std::move(value));
}

StatusCode IbsHandler::get(const DataChunk&& key, std::string* value) {
    return pImpl->get(std::move(key), value);
}

bool IbsHandler::contains(const DataChunk&& key) {
    return pImpl->contains(std::move(key));
}

StatusCode IbsHandler::drop(const DataChunk&& key) {
    return pImpl->drop(std::move(key));
}

StatusCode IbsHandler::atomicWrite(AbstractAtomicUpdate& updates) {
    return pImpl->atomicWrite(updates);
}

bool IbsHandler::getStats(std::string& output) {
    return pImpl->getStats(output);
}

StatusCode IbsHandler::destroy() {
    Destroyable* storable = dynamic_cast<Destroyable*>(pImpl.get());
    if (storable == NULL) {
        return StatusCode::OK();
    }
    else {
        return storable->destroy();
    }
}

void IbsHandler::cleanTransaction() noexcept {
    // clean transaction
    idToTransactionMapLock.writeLock();
    for (auto& pair : idToTransactionMap) {
        if (pair.second.get() != NULL) {
            pair.second->rollback();
        }
        pair.second.reset(); //release memory
        // should be true or it's a memory leak
        assert(pair.second.get() == NULL);
    }
    idToTransactionMap.clear();
    idToTransactionMapLock.unlock();
}

void IbsHandler::findTransaction(const std::string& id, std::shared_ptr<AbstractTransaction>& transaction) {
    idToTransactionMapLock.readLock();
    auto it = idToTransactionMap.find(id);
    if (it != idToTransactionMap.end()) {
        transaction = it->second;
    }
    idToTransactionMapLock.unlock();
}

std::string IbsHandler::createTransaction() {
    assert(pImpl.get() != NULL);
    std::shared_ptr < AbstractTransaction > transaction(new Transaction(*pImpl.get()));
    idToTransactionMapLock.readLock();
    bool idDoesExist = idToTransactionMap.find(transaction->getUuid()) != idToTransactionMap.end();
    idToTransactionMapLock.unlock();
    while (idDoesExist) {
        transaction.reset(new Transaction(*this));
        idToTransactionMapLock.readLock();
        idDoesExist = idToTransactionMap.find(transaction->getUuid()) != idToTransactionMap.end();
        idToTransactionMapLock.unlock();
    }
    idToTransactionMapLock.writeLock();
    std::string uuid = transaction->getUuid();
    idToTransactionMap[uuid] = std::move(transaction);
    idToTransactionMapLock.unlock();
    return uuid;
}

StatusCode IbsHandler::commitTransaction(const std::string& id) noexcept {
    std::shared_ptr < AbstractTransaction > transaction(NULL);
    findTransaction(id, transaction);
    if (transaction.get() != NULL) {
        StatusCode res = transaction->commit();
        idToTransactionMapLock.writeLock();
        auto it = idToTransactionMap.find(transaction->getUuid());
        if (it != idToTransactionMap.end()) {
            it->second.reset(); //release memory
            // should be true or it's a memory leak
            assert(it->second.get() == NULL);
            idToTransactionMap.erase(it);
        }
        idToTransactionMapLock.unlock();
        return res;
    }
    else {
        return StatusCode::TransactionNotFound();
    }
}

void IbsHandler::rollbackTransaction(const std::string& id) {
    std::shared_ptr < AbstractTransaction > transaction(NULL);
    findTransaction(id, transaction);
    if (transaction.get() != NULL) {
        transaction->rollback();
        idToTransactionMapLock.writeLock();
        idToTransactionMap[transaction->getUuid()].reset();
        auto it = idToTransactionMap.find(id);
        if (it != idToTransactionMap.end()) {
            it->second.reset(); //release memory
            // should be true or it's a memory leak
            assert(it->second.get() == NULL);
            idToTransactionMap.erase(it);
        }
        idToTransactionMapLock.unlock();
    }
}

bool IbsHandler::put(const std::string& id, const DataChunk&& key, const DataChunk&& value) {
    bool isAlreadyPresent = false;
    std::shared_ptr < AbstractTransaction > transaction(NULL);
    findTransaction(id, transaction);
    if (transaction.get() != NULL) {
        isAlreadyPresent = transaction->put(key, value);
    }
    return isAlreadyPresent;
}

void IbsHandler::drop(const std::string& id, const DataChunk&& key) {
    std::shared_ptr < AbstractTransaction > transaction(NULL);
    findTransaction(id, transaction);
    if (transaction.get() != NULL) {
        transaction->drop(key);
    }
}

bool IbsHandler::replace(const std::string& id, const DataChunk&& oldKey, const DataChunk&& newKey,
        const DataChunk&& value) {
    bool isAlreadyPresent = false;
    std::shared_ptr < AbstractTransaction > transaction(NULL);
    findTransaction(id, transaction);
    if (transaction.get() != NULL) {
        isAlreadyPresent = transaction->replace(oldKey, newKey, value);
    }
    return isAlreadyPresent;
}

} /* namespace ibs */

