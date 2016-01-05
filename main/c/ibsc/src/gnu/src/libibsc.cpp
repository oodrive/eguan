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
 * @file libcibs.cpp
 */
#include <sys/types.h>
#include "libibsc.h"
#include "libibsc.hpp"
#include "Transaction.h"

/* WARNING declare two variables : instance and ibs */
#define IBS_CHECK_ID(id) \
IbsCBindings* instance = IbsCBindings::getInstance() ;\
AbstractController* ibs = instance->getIbsById(id) ;\
if(NULL == ibs)\
    return IBS__INVALID_IBS_ID;

/* protection of map id<>ibs by lock */
pthread_rwlock_t IbsCBindings::_mapProtectionLock = PTHREAD_RWLOCK_INITIALIZER;

AbstractController* IbsCBindings::getIbsById(int idToLookup) {
    AbstractController* foundIbs = NULL;
    idToIbsMap_t::iterator found;
    if (pthread_rwlock_rdlock(&_mapProtectionLock) != 0)
        return NULL;
    found = ctrlMap.find(idToLookup);
    if (found != ctrlMap.end())
        foundIbs = found->second;
    pthread_rwlock_unlock(&_mapProtectionLock);
    return foundIbs;
}

int IbsCBindings::addIbsWithId(AbstractController* ibsToAdd) {
    assert(NULL != ibsToAdd);
    if (pthread_rwlock_wrlock(&_mapProtectionLock) != 0) {
        return IBS__INTERNAL_ERROR;
    }
    int id = this->idx;
    ctrlMap[id] = ibsToAdd;
    /* increment to prepare for next id generation*/
    this->idx++;
    pthread_rwlock_unlock(&_mapProtectionLock);
    /* but return the actual saved IBS id */
    return id;
}

int IbsCBindings::deleteIbsById(const int id) {
    AbstractController* ibs = getIbsById(id);
    if (NULL != ibs) {
        if (pthread_rwlock_wrlock(&_mapProtectionLock) != 0) {
            return IBS__INTERNAL_ERROR;
        }
        ctrlMap.erase(id);
        pthread_rwlock_unlock(&_mapProtectionLock);
        delete ibs;
    }
    return 0;
}

int ibsCreate(const char* fname) {
    return IbsCBindings::getInstance()->addIbp(fname, true);
}

int ibsInit(const char* fname) {
    return IbsCBindings::getInstance()->addIbp(fname, false);
}

int ibsStart(const int id) {
    IBS_CHECK_ID(id);
    if (ibs->start() != true)
        return IBS__UNKNOW_ERROR;
    return 0;
}

int ibsStop(const int id) {
    IBS_CHECK_ID(id);
    if (ibs->stop() != true)
        return IBS__UNKNOW_ERROR;
    return 0;
}

int ibsDelete(const int id) {
    IBS_CHECK_ID(id);
    if (ibs->stop() != true) {
        /* The way ibsDelete is defined, the memory need to be released
         *  even if closed failed to avoid a memory leak !!! */
        instance->deleteIbsById(id);
        return IBS__UNKNOW_ERROR;
    }
    return instance->deleteIbsById(id);
}

int ibsDestroy(const int id) {
    IBS_CHECK_ID(id);
    if (ibs->destroy() != true)
        return IBS__UNKNOW_ERROR;
    return 0;
}

int ibsHotDataEnabled(const int id) {
    IBS_CHECK_ID(id);
    return ibs->hotDataEnabled() ? TRUE : FALSE;
}

/**
 * TODO: Don't forget to change this function
 * if you add/remove error codes and/or status codes
 * */
int errorCodeFromStatusCode(ibs::StatusCode status) {
    if (status.IsKeyNotFound())
        return IBS__KEY_NOT_FOUND;
    if (status.IsIOError())
        return IBS__IO_ERROR;
    if (status.IsCorruption())
        return IBS__DATA_CORRUPTION;
    if (status.IsConfigError())
        return IBS__CONFIG_ERROR;
    if (status.IsCreateInExistingIBS())
        return IBS__CREATE_IN_NON_EMPTY_DIR;
    if (status.IsInitInEmptyDirectory())
        return IBS__INIT_FROM_EMPTY_DIR;
    if (status.IsSliceTooSmall())
        return IBS__BUFFER_TOO_SMALL;
    if (status.IsKeyAlreadyAdded())
        return IBS__KEY_ALREADY_ADDED;
    if (status.IsTransactionNotFound())
        return IBS__INVALID_TRANSACTION_ID;
    if (status.getCode() == StatusCode::K_DUMP_ERROR) {
        return IBS__DUMP_ERROR;
    }
    return IBS__UNKNOW_ERROR;
}

int ibsGet(const int id, const char* key, const size_t keyLength, char* data, const size_t dataMaxLength,
        size_t* dataLength) {
    // using directly JNI buffer
    // should be quicker
    // than creating another buffer ...
    IBS_CHECK_ID(id);
    uint size;
    DataChunk _key(key, keyLength);
    DataChunk _data(data, dataMaxLength); //Buffer from JNI
    ibs::StatusCode status;
    size_t expected = 0;
    status = ibs->fetch(std::move(_key), std::move(_data), expected);
    if (status.IsSliceTooSmall()) {
        *dataLength = expected;
    }
    if (!status.ok()) {
        return errorCodeFromStatusCode(status);
    }
    size = _data.getSize();
    *dataLength = size;
    assert(size <= dataMaxLength);
    return size;
}

int ibsPut(const int id, const char* key, const size_t keyLength, const char* data, const size_t dataLength) {
    IBS_CHECK_ID(id);
    DataChunk _key(key, keyLength);
    DataChunk _data(data, dataLength);
    ibs::StatusCode status;
    status = ibs->put(std::move(_key), std::move(_data));
    if (status.ok())
        return 0;
    else {
        return errorCodeFromStatusCode(status);
    }
}

int ibsDel(const int id, const char* key, const size_t keyLength) {
    IBS_CHECK_ID(id);
    DataChunk _key(key, keyLength);
    ibs::StatusCode status;
    status = ibs->drop(std::move(_key));
    if (status.ok())
        return 0;
    else {
        return errorCodeFromStatusCode(status);
    }
    return 0;
}

int ibsReplace(const int id, const char* oldKey, const size_t oldKeyLength, const char* newKey,
        const size_t newKeyLength, const char* newData, const int newDataLength) {
    IBS_CHECK_ID(id);
    DataChunk _newKey(newKey, newKeyLength);
    DataChunk _oldKey(oldKey, oldKeyLength);
    DataChunk _newData(newData, newDataLength);
    ibs::StatusCode status;
    status = ibs->replace(std::move(_oldKey), std::move(_newKey), std::move(_newData));
    if (status.ok())
        return 0;
    else {
        return errorCodeFromStatusCode(status);
    }
}

/* lock for double-checked mechanism  */
pthread_mutex_t IbsCBindings::_singletonLock = PTHREAD_MUTEX_INITIALIZER;

IbsCBindings* IbsCBindings::getInstance() {
    /* double-checked locking */
    if (NULL == _instance) {
        pthread_mutex_lock(&_singletonLock);
        if (NULL == _instance) {
            _instance = new IbsCBindings();
            _instance->idx = 1;
        }
        pthread_mutex_unlock(&_singletonLock);
    }
    return _instance;
}

int IbsCBindings::addIbp(const char* fname, bool create) {
    std::string f(fname ? fname : "");
    ibs::AbstractController* Ibs = NULL;
    ibs::StatusCode status = create ? ibs::Controller::create(f, Ibs) : ibs::Controller::init(f, Ibs);
    if (NULL == Ibs) {
        return errorCodeFromStatusCode(status);
    }
    else {
        return addIbsWithId(Ibs);
    }
}

int ibsCreateTransaction(const int id) {
    IBS_CHECK_ID(id);
    std::string transactionUuid = ibs->createTransaction();
    int transactionId;
    bool isValid = ibs::Transaction::getIdFromUuid(transactionUuid, transactionId);
    if (isValid) {
        return transactionId;
    }
    else {
        return IBS__INVALID_TRANSACTION_ID;
    }
}

int ibsCommitTransaction(const int id, const int transactionId) {
    IBS_CHECK_ID(id);
    std::string transactionUuid;
    bool isValid = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
    if (isValid) {
        StatusCode status = ibs->commitTransaction(transactionUuid);
        if (status.ok()) {
            return 0;
        }
        else {
            return errorCodeFromStatusCode(status);
        }
    }
    else {
        return IBS__INVALID_TRANSACTION_ID;
    }
}

int ibsRollbackTransaction(const int id, const int transactionId) {
    IBS_CHECK_ID(id);
    std::string transactionUuid;
    bool isValid = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
    if (isValid) {
        ibs->rollbackTransaction(transactionUuid);
        return 0;
    }
    else {
        return IBS__INVALID_TRANSACTION_ID;
    }
}

int ibsPutTransaction(const int id, const int transactionId, const char* key, const size_t keyLength, const char* data,
        const size_t dataLength) {
    IBS_CHECK_ID(id);
    std::string transactionUuid;
    bool isValid = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
    if (isValid) {
        DataChunk _key(key, keyLength);
        DataChunk _data(data, dataLength);
        bool isAlreadyPresent = ibs->put(transactionUuid, std::move(_key), std::move(_data));
        return isAlreadyPresent ? IBS__KEY_ALREADY_ADDED : 0;
    }
    else {
        return IBS__INVALID_TRANSACTION_ID;
    }
}

int ibsReplaceTransaction(const int id, const int transactionId, const char* oldKey, const size_t oldKeyLength,
        const char* newKey, const size_t newKeyLength, const char* newData, const int newDataLength) {
    IBS_CHECK_ID(id);
    std::string transactionUuid;
    bool isValid = ibs::Transaction::getUuidFromId(transactionId, transactionUuid);
    if (isValid) {
        DataChunk _newKey(newKey, newKeyLength);
        DataChunk _oldKey(oldKey, oldKeyLength);
        DataChunk _newData(newData, newDataLength);
        bool isAlreadyPresent = ibs->replace(transactionUuid, std::move(_oldKey), std::move(_newKey),
                std::move(_newData));
        return isAlreadyPresent ? IBS__KEY_ALREADY_ADDED : 0;
    }
    else {
        return IBS__INVALID_TRANSACTION_ID;
    }
}

IbsCBindings::IbsCBindings() {
}

IbsCBindings::~IbsCBindings() {
    for (auto ite : ctrlMap) {
        if (ite.second != NULL) {
            ite.second->stop();
            delete ite.second;
        }
    }
    ctrlMap.clear();
}

IbsCBindings *IbsCBindings::_instance = NULL;
