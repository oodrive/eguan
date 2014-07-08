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
 * @file Recorder.cpp
 * @brief The IBS recorder/source
 * @author j. caba
 */
#include "Recorder.h"
#include "Controller.h"
#include "Transaction.h"

namespace ibs {

Logger_t RecorderFile::logger = ibs::Logger::getLogger("RecorderFile");

const u_char RecorderFile::magic[magicSize] = { 'I', 'B', 'S', 'R' }; // magic number starting a recorder file

RecorderFile::RecorderFile(const std::string& fileToSave, const ConfigFileReader* ibsCfg) :
        filename(fileToSave), cfg(ibsCfg), nbEntries(0), updateEntriesPos(), fileLock() {
    // create a file with magic number
    std::ofstream f(filename, std::ios::binary);
    setHeader(f);
    f.close();
}

void RecorderFile::lockFile() {
    fileLock.lock();
}

void RecorderFile::unlockFile() {
    fileLock.unlock();
}

const std::string& RecorderFile::getFilename() const {
    return filename;
}

void RecorderFile::appendResult(const StatusCode& result) {
    std::ofstream f(filename, append);
    setResult(f, result);
    f.close();
}

void RecorderFile::appendIntResult(const int result) {
    std::ofstream f(filename, append);
    setUint64(f, result);
    f.close();
}

void RecorderFile::appendExpected(const size_t expected) {
    std::ofstream f(filename, append);
    setUint64(f, expected);
    f.close();
}

void RecorderFile::appendDrop(const DataChunk& key) {
    updateNbEntries();
    appendOpcode(DROP);
    appendDataChunk(key);
}

void RecorderFile::appendPut(const DataChunk& key) {
    updateNbEntries();
    appendOpcode(PUT);
    appendDataChunk(key);
}

void RecorderFile::appendReplace(const DataChunk& oldkey, const DataChunk& key) {
    updateNbEntries();
    appendOpcode(REPLACE);
    appendDataChunk(oldkey);
    appendDataChunk(key);
}

void RecorderFile::appendCreateTransactionOpcode() {
    updateNbEntries();
    appendOpcode(CREATE_TRANSACTION);
}

void RecorderFile::appendCreateTransactionId(const int id) {
    std::ofstream f(filename, append);
    setId(f, id);
    f.close();
}

void RecorderFile::appendCommitTransaction(const int id) {
    updateNbEntries();
    appendOpcode(COMMIT_TRANSACTION);
    appendId(id);
}

void RecorderFile::appendRollbackTransaction(const int id) {
    updateNbEntries();
    appendOpcode(ROLLBACK_TRANSACTION);
    appendId(id);
}

void RecorderFile::appendPutTransaction(const int id, const DataChunk& key) {
    updateNbEntries();
    appendOpcode(PUT_TRANSACTION);
    appendId(id);
    appendDataChunk(key);
}

void RecorderFile::appendReplaceTransaction(const int id, const DataChunk& oldkey, const DataChunk& key) {
    updateNbEntries();
    appendOpcode(REPLACE_TRANSACTION);
    appendId(id);
    appendDataChunk(oldkey);
    appendDataChunk(key);
}

void RecorderFile::appendGet(const DataChunk& key) {
    updateNbEntries();
    appendOpcode(GET);
    appendDataChunk(key);
}

void RecorderFile::appendFetch(const DataChunk& key) {
    updateNbEntries();
    appendOpcode(FETCH);
    appendDataChunk(key);
}

void RecorderFile::appendStart() {
    updateNbEntries();
    appendOpcode(START_IBS);
}

void RecorderFile::appendStop() {
    updateNbEntries();
    appendOpcode(STOP_IBS);
}

void RecorderFile::updateNbEntries() {
    ++nbEntries;
    std::fstream f(filename, std::ios::binary | std::ios::out | std::ios::in);
    f.seekp(updateEntriesPos);
    updateUint64(f, nbEntries);
    f.close();
}

void RecorderFile::setHeader(std::ofstream& f) {
    for (u_short i = 0; i < magicSize; i++) {
        f << magic[i];
    }
    updateEntriesPos = f.tellp();
    // add number of entries initialized
    setUint64(f, nbEntries);
    std::ios::pos_type whereToStoreOffset = f.tellp();
    setUint64(f, whereToStoreOffset); //reserve space for the offset
    // save configuration
    setString(f, cfg->getString(HOT_DATA));
    setUint64(f, cfg->getInt(IBP_NB));
    setUint64(f, cfg->getInt(BUFFER_ROTATION_THRESHOLD));
    setUint64(f, cfg->getInt(BUFFER_ROTATION_DELAY));
    setString(f, cfg->getString(COMPRESSION));
    setUint64(f, cfg->getInt(BUFFER_WRITE_DELAY_THRESHOLD));
    setUint64(f, cfg->getInt(BUFFER_WRITE_DELAY_LEVEL_SIZE));
    setUint64(f, cfg->getInt(BUFFER_WRITE_DELAY_INCR_MS));
    setUint64(f, cfg->getInt(LDB_BLOCK_SIZE));
    setUint64(f, cfg->getInt(LDB_BLOCK_RESTART_INVERVAL));
    setUint64(f, cfg->getInt(INDICATED_RAM_SIZE));

    // set to NULL to avoid usage
    // memory is owned and deleted by ibs::Controller
    // after call to allocateRecordFile
    cfg = NULL;
    // save record begin offset
    std::ios::pos_type posEnd = f.tellp();
    f.seekp(whereToStoreOffset);
    setUint64(f, posEnd);
    f.seekp(posEnd);
}

void RecorderFile::appendDataChunk(const DataChunk& datachunk) {
    std::ofstream f(filename, append);
    setDataChunk(f, datachunk);
    f.close();
}

void RecorderFile::appendId(const int id) {
    std::ofstream f(filename, append);
    setId(f, id);
    f.close();
}

void RecorderFile::appendOpcode(const enum OPCODE opcode) {
    std::ofstream f(filename, append);
    setOpcode(f, opcode);
    f.close();
}

void RecorderFile::setOpcode(std::ofstream& f, const enum OPCODE opcode) {
    setUint64(f, opcode);
}

void RecorderFile::setResult(std::ofstream& f, const StatusCode& result) {
    setUint64(f, result.getCode());
}

void RecorderFile::setId(std::ofstream& f, int id) {
    setUint64(f, id);
}

void RecorderFile::setDataChunk(std::ofstream& f, const DataChunk& datachunk) {
    setString(f, datachunk.toString());
}

void Recorder::allocateRecorder(AbstractController*& po_controller, AbstractController* real) noexcept {
    if (real != NULL) {
        Recorder* rec = new Recorder();
        assert(NULL != rec);
        rec->realController.reset(real);
        po_controller = rec;
    }
}

RecorderFile::~RecorderFile() = default;

Recorder::~Recorder() = default;

StatusCode Recorder::create(const std::string& fname, AbstractController*& po_controller) {
    AbstractController* ctrl = NULL;
    // Note it's important to avoid infinite loop
    StatusCode retCode = Controller::create(fname, ctrl, false);
    allocateRecorder(po_controller, ctrl);
    assert(NULL != ctrl);
    return retCode;
}

StatusCode Recorder::init(const std::string& fname, AbstractController*& po_controller) {
    AbstractController* ctrl = NULL;
    // Note it's important to avoid infinite loop
    StatusCode retCode = Controller::init(fname, ctrl, false);
    allocateRecorder(po_controller, ctrl);
    assert(NULL != ctrl);
    return retCode;
}

bool Recorder::start() {
    // save call in record file without storing the block value
    recordFile->lockFile();
    recordFile->appendStart();
    bool result = realController->start();
    recordFile->appendIntResult(result);
    recordFile->unlockFile();
    return result;
}

bool Recorder::stop() {
    // save call in record file without storing the block value
    recordFile->lockFile();
    recordFile->appendStop();
    bool result = realController->stop();
    recordFile->appendIntResult(result);
    recordFile->unlockFile();
    return result;
}

std::string Recorder::getUuid() {
    return realController->getUuid();
}

std::string Recorder::getOwnerUuid() {
    return realController->getOwnerUuid();
}

std::string Recorder::getConfigFile() {
    return realController->getConfigFile();
}

StatusCode Recorder::put(const DataChunk&& key, const DataChunk&& value) {
    // save call in record file without storing the block value
    recordFile->lockFile();
    recordFile->appendPut(key);
    StatusCode result = realController->put(std::move(key), std::move(value));
    recordFile->appendResult(result);
    recordFile->unlockFile();
    return result;
}

StatusCode Recorder::fetch(const DataChunk&& key, DataChunk&& value, size_t& expected) {
    // save call in record file
    recordFile->lockFile();
    recordFile->appendFetch(key);
    StatusCode result = realController->fetch(std::move(key), std::move(value), expected);
    recordFile->appendExpected(expected);
    recordFile->appendResult(result);
    recordFile->unlockFile();
    return result;
}

StatusCode Recorder::drop(const DataChunk&& key) {
    // save call in record file
    recordFile->lockFile();
    recordFile->appendDrop(key);
    StatusCode result = realController->drop(std::move(key));
    recordFile->appendResult(result);
    recordFile->unlockFile();
    return result;
}

StatusCode Recorder::replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) {
    // save call in record file without storing the block value
    recordFile->lockFile();
    recordFile->appendReplace(oldKey, newKey);
    StatusCode result = realController->replace(std::move(oldKey), std::move(newKey), std::move(value));
    recordFile->appendResult(result);
    recordFile->unlockFile();
    return result;
}

std::string Recorder::createTransaction() {
    // save call in record file
    recordFile->lockFile();
    recordFile->appendCreateTransactionOpcode();
    std::string uuid = realController->createTransaction();
    int id = 0;
    Transaction::getIdFromUuid(uuid, id);
    recordFile->appendCreateTransactionId(id);
    recordFile->unlockFile();
    return uuid;
    return std::string();
}

void Recorder::rollbackTransaction(const std::string& uuid) {
    // save call in record file
    int id = 0;
    Transaction::getIdFromUuid(uuid, id);
    recordFile->lockFile();
    recordFile->appendRollbackTransaction(id);
    recordFile->unlockFile();
    realController->rollbackTransaction(uuid);
}

StatusCode Recorder::commitTransaction(const std::string& uuid) {
    // save call in record file
    int id = 0;
    Transaction::getIdFromUuid(uuid, id);
    recordFile->lockFile();
    recordFile->appendCommitTransaction(id);
    StatusCode result = realController->commitTransaction(uuid);
    recordFile->appendResult(result);
    recordFile->unlockFile();
    return result;
    return StatusCode::OK();
}

bool Recorder::put(const std::string& uuid, const DataChunk&& key, const DataChunk&& value) {
    // save call in record file without storing the block value
    int id = 0;
    Transaction::getIdFromUuid(uuid, id);
    recordFile->lockFile();
    recordFile->appendPutTransaction(id, key);
    recordFile->unlockFile();
    return realController->put(uuid, std::move(key), std::move(value));
    //TODO: add return value in record file for replay ?
    return false;
}

bool Recorder::replace(const std::string& uuid, const DataChunk&& oldKey, const DataChunk&& newKey,
        const DataChunk&& value) {
    // save call in record file without storing the block value
    int id = 0;
    Transaction::getIdFromUuid(uuid, id);
    recordFile->lockFile();
    recordFile->appendReplaceTransaction(id, oldKey, newKey);
    recordFile->unlockFile();
    return realController->replace(uuid, std::move(oldKey), std::move(newKey), std::move(value));
    //TODO: add return value in record file for replay ?
    return false;
}

bool Recorder::destroy() {
    return realController->destroy();
}

bool Recorder::hotDataEnabled() {
    return realController->hotDataEnabled();
}

void Recorder::allocateRecordFile(const std::string& filename, const ConfigFileReader* cfg) {
    recordFile.reset(new RecorderFile(filename, cfg));
}

}/* namespace ibs */
