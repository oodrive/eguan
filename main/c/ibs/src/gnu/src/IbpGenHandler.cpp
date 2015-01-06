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
 * @file IbpGenHandler.cpp
 * @brief IbpGen handler implementation/header
 * @author j. caba
 */
#include "IbpGenHandler.h"
#include "Logger.h"
#include "CombinedBlockStore.h"
#include "Constants.h"
#include "ConfigFileReader.h"
#include <limits>

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("IbpGenHandler");

IbpGenHandler::IbpGenHandler(ConfigFileReader& cfg) :
        baseDirectory(cfg.getString(IBP_GEN_PATH)), closed(true), dumpError(false), delay(), creationLoop(*this), dumpLoop(
                *this), buffers(), persistedData(cfg), locks() {
    // initialize write slow down parameters
    delay.threshold = BUFFER_WRITE_DELAY_THRESHOLD_DEF;
    delay.levelSize = BUFFER_WRITE_DELAY_LEVEL_DEF;
    delay.incrUs = BUFFER_WRITE_DELAY_INCR_DEF;
    delay.active = false;

    int delayThr = cfg.getInt(BUFFER_WRITE_DELAY_THRESHOLD);
    if (delayThr > 0) {
        this->delay.threshold = delayThr;
    }

    int levelSz = cfg.getInt(BUFFER_WRITE_DELAY_LEVEL_SIZE);
    if (levelSz > 0) {
        this->delay.levelSize = levelSz;
    }

    int delayInc = cfg.getInt(BUFFER_WRITE_DELAY_INCR_MS);
    if (delayInc > 0) {
        this->delay.incrUs = delayInc * 1000;
    }
    LOG4IBS_DEBUG(logger,
            "Buffer write delay initialized; delayThreshold=" << this->delay.threshold << " levelSize=" << this->delay.levelSize << " delayIncrement=" << this->delay.incrUs << "microseconds");

    // initialize write conditions delay and threshold
    delay.writeCreationThreshold = BUFFER_ROTATION_THRESHOLD_DEF;
    if (cfg.getInt(BUFFER_ROTATION_THRESHOLD) > 0) {
        delay.writeCreationThreshold = cfg.getInt(BUFFER_ROTATION_THRESHOLD);
    }
    LOG4IBS_DEBUG(logger, "Buffer switching max write = " << delay.writeCreationThreshold);

    delay.writeCreationMaxDelay = BUFFER_ROTATION_DELAY_DEF * SECOND_TO_MICROSECOND; //convert to microsecond
    if (cfg.getInt(BUFFER_ROTATION_DELAY) > 0) {
        delay.writeCreationMaxDelay = cfg.getInt(BUFFER_ROTATION_DELAY) * SECOND_TO_MICROSECOND;
    }
    LOG4IBS_DEBUG(logger, "Buffer switching delay = " << delay.writeCreationMaxDelay);

    delay.dumpAtStopLimit = DUMP_AT_STOP_BEST_EFFORT_DELAY_DEF;
    if (cfg.getInt(DUMP_AT_STOP_BEST_EFFORT_DELAY) > 0) {
        delay.dumpAtStopLimit = cfg.getInt(DUMP_AT_STOP_BEST_EFFORT_DELAY);
    }
}

IbpGenHandler::~IbpGenHandler() {
    close();
}

StatusCode IbpGenHandler::open() {
    if (closed.exchange(false) == true) {
        // TODO: use result of open ...
        // open ibp ...
        persistedData.open();

        // re inject old data in case of reboot/crash of machine
        // or not complete dump at stop
        reInjectOldData();

        // need to create at least one buffer before start for new put ...
        creationLoop.createNext(*this);
        // start threads ...
        creationLoop.start();
        dumpLoop.start();
    }
    return StatusCode::OK();
}

void IbpGenHandler::close() {
    if (closed.exchange(true) == false) {
        // stop threads ...
        creationLoop.conditions.reset();
        creationLoop.stop();
        dumpLoop.stop();
        promoteAllData(delay.dumpAtStopLimit);
        // do close ...
        if (!buffers.isEmpty()) {
            for (auto weak_pointer : buffers.toVector()) {
                if (auto buffer = weak_pointer.lock()) {
                    buffer->close();
                }
            }
        }
        buffers.reborn();
        // close ibp ..
        persistedData.close();
        dumpError = false;
    }
}

bool IbpGenHandler::isClosed() {
    return closed;
}

StatusCode IbpGenHandler::put(const DataChunk&& key, const DataChunk&& value) {
    if (closed) {
        return StatusCode::NotSupported();
    }
    const DataChunk& keyToMove(key);
    if (persistedData.contains(std::move(keyToMove))) {
        return StatusCode::KeyAreadyAdded();
    }
    locks.dataIntegrity.writeLock();
    bool isInHotData = searchNIncrement(std::move(key));
    if (isInHotData) {
        locks.dataIntegrity.unlock();
        return StatusCode::KeyAreadyAdded();
    }
    StatusCode st = putInNewestBuffer(key, value);
    locks.dataIntegrity.unlock();
    if (st.IsKeyNotFound()) {
        // was trying to increment a key
        // if we have it then it's not an error.
        const DataChunk& keyToSearch(key);
        if (contains(std::move(keyToSearch))) {
            return StatusCode::KeyAreadyAdded();
        }
    }
    // in case of error and an error on last dump
    // report origin of error as a DUMP_ERROR
    if (!st.ok() && dumpError) {
        st = StatusCode(StatusCode::K_DUMP_ERROR);
    }
    return st;
}

StatusCode IbpGenHandler::replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) {
    if (closed) {
        return StatusCode::NotSupported();
    }
    locks.atomicReplaceMutex.lock();
    // replace don't remove data persisted ...
    locks.dataIntegrity.writeLock();
    dropFromBuffers(std::move(oldKey)); // process old key ...
    locks.dataIntegrity.unlock();
    StatusCode st = put(std::move(newKey), std::move(value));
    locks.atomicReplaceMutex.unlock();
    return st;
}

StatusCode IbpGenHandler::get(const DataChunk&& key, std::string* value) {
    if (closed || NULL == value) {
        return StatusCode::NotSupported();
    }
    const DataChunk& keyToMove(key);
    // First read from buffers possibly being promoted
    // in an IBP. And only after look in IBP if not found.
    locks.dataIntegrity.readLock();
    StatusCode st = getFromBuffers(std::move(key), value);
    locks.dataIntegrity.unlock();
    bool needTocheckInPersistedData = !st.IsKeyFound() || value->empty();
    if (needTocheckInPersistedData) {
        st = persistedData.get(std::move(keyToMove), value);
    }
    // in case of error and an error on last dump
    // report origin of error as a DUMP_ERROR
    if (!st.ok() && dumpError) {
        st = StatusCode(StatusCode::K_DUMP_ERROR);
    }
    return st;
}

bool IbpGenHandler::contains(const DataChunk&& key) {
    if (closed) {
        return false;
    }
    const DataChunk& keyToCheck(key);
    locks.dataIntegrity.readLock();
    bool isFoundInBuffers = isInBuffers(std::move(keyToCheck));
    locks.dataIntegrity.unlock();
    return isFoundInBuffers || persistedData.contains(std::move(key));
}

StatusCode IbpGenHandler::drop(const DataChunk&& key) {
    if (closed) {
        return StatusCode::NotSupported();
    }
    const DataChunk& keyToMove(key);
    locks.dataIntegrity.writeLock();
    dropFromBuffers(std::move(keyToMove));
    locks.dataIntegrity.unlock();
    return persistedData.drop(std::move(key));
}

StatusCode IbpGenHandler::atomicWrite(AbstractAtomicUpdate& updates) {
    if (closed) {
        return StatusCode::NotSupported();
    }
    locks.atomicWriteMutex.lock();
    AbstractAtomicUpdate* pointer = &updates;
    assert(pointer != NULL);
    IbpGenAtomicUpdate* pointerUpdatesToApply = static_cast<IbpGenAtomicUpdate*>(pointer);
    assert(pointerUpdatesToApply != NULL);
    IbpGenAtomicUpdate& updatesToApply = *pointerUpdatesToApply;

    StatusCode retCode = StatusCode::OK();
    for (auto& kv : updatesToApply.keysToDropOrPut) {
        const std::string& key(kv.first);
        switch (kv.second) {
            case IbpGenAtomicUpdate::DROP: {
                // process old key ...
                locks.dataIntegrity.writeLock();
                dropFromBuffers(key);
                locks.dataIntegrity.unlock();
                break;
            }
            case IbpGenAtomicUpdate::PUT: {
                std::string fetchedValue;
                auto it = updatesToApply.keysToAddOnRam.find(key);
                if (it == updatesToApply.keysToAddOnRam.end()) {
                    LOG4IBS_ERROR(logger, "Could not get data while commit");
                }
                else {
                    fetchedValue.assign(it->second);
                    StatusCode tmp = put(std::move(key), fetchedValue);
                    if (!tmp.ok() && !tmp.IsKeyAlreadyAdded()) {
                        LOG4IBS_ERROR(logger, "Could not process new key status=" << tmp.ToString());
                        retCode = tmp;
                    }
                }
                break;
            }
            default: {
                LOG4IBS_ERROR(logger, "Unknown opcode=" << kv.second);
                break;
            }
        }
    }
    updates.clear(); // release memory and empty buffered updates
    locks.atomicWriteMutex.unlock();
    return retCode;
}

bool IbpGenHandler::getStats(std::string& output) {
    return persistedData.getStats(output);
}

StatusCode IbpGenHandler::destroy() {
    close();
    persistedData.destroy();
    locks.dataIntegrity.writeLock();
    destroyAllBuffers();
    locks.dataIntegrity.unlock();
    FileTools::removeDirectory(baseDirectory);
    return StatusCode::OK();
}

StatusCode IbpGenHandler::putInNewestBuffer(const DataChunk& key, const DataChunk& value) {
    if (delay.active) {
        // put/write slow down
        ::usleep(delay.value);
    }
    auto newest = buffers.getNewest();
    if (auto currentBuffer = newest.lock()) {
        StatusCode status = currentBuffer->put(std::move(key), std::move(value));
        if (status.ok()) {
            const uint64_t rotationMaxWrite = getWriteThreshold();
            // Checking max writes threshold
            uint64_t nbWrite = creationLoop.conditions.updateWriteOps();
            if (nbWrite >= rotationMaxWrite && !creationLoop.conditions.isCreationPending()) {
                // We are writing intensively and the max number of writes was reached : a new buffer database is needed
                creationLoop.conditions.requestCreation();
                // be nice to other threads too
                std::this_thread::yield();
            }
        }
        return status;
    }
    return StatusCode::IOError();
}

bool IbpGenHandler::isInBuffers(const DataChunk& key) {
    bool found = false;
    for (auto weak_pointer : buffers.toVector()) {
        if (auto buffer = weak_pointer.lock()) {
            const DataChunk& keyToMove(key);
            if (buffer->contains(std::move(keyToMove))) {
                found = true;
                break;
            }
        }
    }
    return found;
}

bool IbpGenHandler::searchNIncrement(const DataChunk& key) {
    bool found = false;
    for (auto weak_pointer : buffers.toVector()) {
        if (auto buffer = weak_pointer.lock()) {
            const DataChunk& keyToMove(key);
            if (buffer->searchNIncrement(std::move(keyToMove))) {
                found = true;
                break;
            }
        }
    }
    return found;
}

bool IbpGenHandler::dropFromBuffers(const DataChunk& key) {
    bool found = false;
    for (auto weak_pointer : buffers.toVector()) {
        if (auto buffer = weak_pointer.lock()) {
            const DataChunk& keyToMove(key);
            StatusCode st = buffer->drop(std::move(keyToMove));
            if (st.ok()) {
                found = true;
                break;
            }
        }
    }
    return found;
}

StatusCode IbpGenHandler::getFromBuffers(const DataChunk&& key, std::string* value) noexcept {
    if (value == NULL) {
        return StatusCode::InvalidArgument();
    }
    StatusCode st = StatusCode::KeyNotFound();
    for (auto weak_pointer : buffers.toVector()) {
        if (auto buffer = weak_pointer.lock()) {
            const DataChunk& keyToMove(key);
            std::string fetched;
            StatusCode status = buffer->get(std::move(keyToMove), &fetched);
            if (status.IsKeyFound()) {
                value->assign(fetched);
                st = status;
                break;
            }
        }
    }
    return st;
}

void IbpGenHandler::destroyAllBuffers() {
    for (auto weak_pointer : buffers.toVector()) {
        if (auto buffer = weak_pointer.lock()) {
            Destroyable* db = dynamic_cast<Destroyable*>(buffer.get());
            if (db != NULL) {
                db->destroy();
            }
        }
    }
}

void IbpGenHandler::updateWriteDelay() noexcept {
    int nbDb = buffers.count();
    // update write delay status and delay value
    int delayDelta = nbDb - delay.threshold;
    // compute slow down delay
    delay.value = (delayDelta / delay.levelSize) * delay.incrUs;
    delay.active = ((delay.threshold < nbDb) && (delay.value > 0));
    if (delay.active) {
        LOG4IBS_DEBUG(logger,
                "Write delay active nbDb=" << nbDb << " delayDelta=" << delayDelta << " delayValue=" << delay.value);
    }
    else {
        LOG4IBS_DEBUG(logger, "Write delay not active nbDb=" << nbDb);
    }
}

RefCountedDb* IbpGenHandler::createOpenNewBuffer(const std::string& dbFile) {
    RefCountedDb* toOpen = new RefCountedDb(dbFile);
    if (toOpen != NULL) {
        toOpen->open();
    }
    return toOpen;
}

void IbpGenHandler::reInjectOldData() {
    // 1) detect buffers files
    std::vector<std::string> fileList;
    FileTools::list(baseDirectory, fileList, FileTools::not_a_directory);
    std::vector<std::string> bufferList;
    for (const auto& file : fileList) {
        if (FileTools::hasIbsExtension(FileTools::getBasename(file)) && RefCountedDbFile::hasValidHeader(file)) {
            bufferList.emplace_back(getBaseDirectory() + "/" + file);
        }
    }
    fileList.clear();
    if (!bufferList.empty()) {
        // 2) re inject buffers
        std::sort(bufferList.begin(), bufferList.end());
        for (const auto& buffer : bufferList) {
            buffers.insertNewest(createOpenNewBuffer(buffer));
        }
        // 3) promote all the injected buffers taking all time needed
        promoteAllData(std::numeric_limits<uint64_t>::max());
    }
}

void IbpGenHandler::promoteAllData(const uint64_t secondsLimit) {
    std::chrono::time_point<std::chrono::system_clock> startTime = std::chrono::system_clock::now();
    while (!buffers.isEmpty() && !dumpError) {
        promoteOldestData();
        std::chrono::time_point<std::chrono::system_clock> endTime = std::chrono::system_clock::now();
        uint64_t elapsed = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime).count();
        if (elapsed >= secondsLimit) {
            // stop dump if elapsed too much time
            break;
        }
    }
}

void IbpGenHandler::promoteOldestData() {
    locks.serializeDumpMutex.lock();
    if (auto oldest = buffers.getOldest().lock()) {
        RefCountedDb* oldestDb = dynamic_cast<RefCountedDb*>(oldest.get());
        if (NULL != oldestDb) {
            // set base to dump to "read only mode"
            LOG4IBS_INFO(logger, "Set base " << oldestDb->getStoragePath() << " to dump in read only mode.");
            oldestDb->setReadOnly();
            // prepare data to write
            CombinedBlockStore* ibpSet = persistedData.ibpSet.get();
            if (NULL != ibpSet) {
                // initialize to a non OK code, any invalid code should do
                StatusCode dumpSt = StatusCode::NotSupported();
                constexpr int retry = PERSIST_RETRY;
                for (int i = 0; i < retry; ++i) {
                    std::unique_ptr<AbstractAtomicUpdate> updates;
                    updates.reset(new CombinedBSAtomicUpdate(*ibpSet, false));
                    if (NULL != updates.get()) {
                        std::unordered_map<std::string, std::string> map;
                        oldestDb->readFromDisk(map);
                        for (auto pair : map) {
                            // only persist if not already persisted
                            if (!persistedData.contains(pair.first)) {
                                // need to compress if front compression is enabled
                                bool compress = persistedData.compression
                                        == IbpHandler::CompressionType::frontCompression;
                                updates->put(pair.first, pair.second, compress);
                            }
                        }
                        // write in the IBP
                        LOG4IBS_INFO(logger, "Persisting " << map.size() << " keys");
                        dumpSt = persistedData.atomicWrite(*updates.get());
                        // check that all is persisted correctly
                        if (dumpSt.ok()) {
                            bool allIsPersisted = true;
                            for (auto pair : map) {
                                if (!persistedData.contains(pair.first)) {
                                    allIsPersisted = false;
                                    break;
                                }
                            }
                            if (!allIsPersisted) {
                                // could not persist all data
                                dumpSt = StatusCode::IOError();
                            }
                        }
                        updates.reset();
                        map.clear();
                    }
                }
                if (dumpSt.ok()) {
                    // delete base from ibpgen
                    buffers.removeOldest();
                    dumpError = false;
                }
                else {
                    LOG4IBS_ERROR(logger, "Could not persist data, check space available on disks.");
                    LOG4IBS_INFO(logger, "Will not delete temporary database, set it back to read/write mode.");
                    dumpError = true;
                    oldestDb->setReadWrite();
                }
            }
        }
        locks.serializeDumpMutex.unlock();
    }
}

} /* namespace ibs */

