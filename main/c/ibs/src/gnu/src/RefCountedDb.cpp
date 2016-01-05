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
 * @file RefCountedDb.cpp
 * @brief The reference counted db/source
 * @author j. caba
 */

#include "RefCountedDb.h"
#include "Logger.h"

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("RefCountedDb");

RefCountedDb::RefCountedDb(const std::string& fname) :
        AbstractBlockStore(), Storable(), isOpen(false), readOnly(false), file(), filename(fname), map() {
}

RefCountedDb::~RefCountedDb() {
    // clear memory
    mapProtect.writeLock();
    map.clear();
    mapProtect.unlock();
    close();
    destroy();
}

StatusCode RefCountedDb::open() {
    if (isOpen.exchange(true) == false) {
        file.reset(new RefCountedDbFile(filename));
        if (!FileTools::exists(filename)) {
            return StatusCode::IOError();
        }
        return StatusCode::OK();
    }
    else {
        return StatusCode::NotSupported();
    }
}

void RefCountedDb::close() {
    if (isOpen.exchange(false) == true) {
        file.reset();
    }
}

StatusCode RefCountedDb::put(const DataChunk&& key, const DataChunk&& value) {
    if (isOpen == false) {
        return StatusCode::NotSupported();
    }
    if (readOnly) {
        return StatusCode::NotSupported();
    }
    std::string keyStr = key.toString();
    mapProtect.readLock();
    auto it = map.find(keyStr);
    if (it == map.end()) {
        mapProtect.unlock();

        // NULL value is accepted only to "refresh" an already added value.
        if (value.isReferencing(NULL)) {
            LOG4IBS_ERROR(logger, "Detected invalid refresh of key='"<< key.hash() <<"'");
            return StatusCode::KeyNotFound();
        }
        // not in map so add it
        std::ios::pos_type offset = file->appendNewKeyData(key, value);

        mapProtect.writeLock();
        map[keyStr] = std::make_pair(1, offset);
        mapProtect.unlock();
    }
    else {
        LOG4IBS_DEBUG(logger, "Increment while in put ...");
        // is in map
        mapProtect.unlock();

        atomicIncrement(keyStr);
    }
    return StatusCode::OK();
}

StatusCode RefCountedDb::get(const DataChunk&& key, std::string* value) {
    if (isOpen == false) {
        return StatusCode::NotSupported();
    }
    if (value == NULL) {
        return StatusCode::InvalidArgument();
    }
    std::string keyStr = key.toString();
    mapProtect.readLock();
    auto it = map.find(keyStr);
    if (it == map.end()) {
        mapProtect.unlock();
        // not in map
        return StatusCode::KeyNotFound();
    }
    else {
        // is in map
        std::ios::pos_type offset = it->second.second;
        uint64_t ref = it->second.first;
        mapProtect.unlock();

        if (ref != 0) {
            // get value from file
            std::string fetched;
            file->getData(offset, fetched);
            value->assign(fetched.data(), fetched.size());
            // file was removed - this should not append.
            if (!FileTools::exists(filename)) {
                close();
                return StatusCode::IOError();
            }
        }
        else {
            // in map but with no reference anymore
            return StatusCode::KeyNotFound();
        }
    }
    return StatusCode::OK();
}

bool RefCountedDb::contains(const DataChunk&& key) {
    std::string keyStr = key.toString();

    mapProtect.readLock();
    auto it = map.find(keyStr);
    if (it == map.end()) {
        mapProtect.unlock();
        // not in map
        return false;
    }
    else {
        // is in map
        uint64_t ref = it->second.first;
        mapProtect.unlock();

        return (ref != 0);
    }
}

void RefCountedDb::atomicIncrement(const std::string& keyStr) {
    mapProtect.writeLock();
    std::ios::pos_type offset = map[keyStr].second;
    uint64_t ref = map[keyStr].first + 1;
    map[keyStr] = std::make_pair(ref, offset);
    mapProtect.unlock();
}

void RefCountedDb::atomicDecrement(const std::string& keyStr) {
    mapProtect.writeLock();
    std::ios::pos_type offset = map[keyStr].second;
    uint64_t oldref = map[keyStr].first;
    uint64_t ref = oldref > 0 ? oldref - 1 : 0;
    map[keyStr] = std::make_pair(ref, offset);
    mapProtect.unlock();
}

bool RefCountedDb::searchNIncrement(const DataChunk&& key) {
    if (isOpen == false) {
        return false;
    }
    if (readOnly) {
        return false;
    }
    std::string keyStr = key.toString();
    mapProtect.readLock();
    auto it = map.find(keyStr);
    if (it == map.end()) {
        mapProtect.unlock();
        // not in map
        return false;
    }
    else {
        // is in map
        mapProtect.unlock();

        atomicIncrement(keyStr);
        return true;
    }
}

StatusCode RefCountedDb::drop(const DataChunk&& key) {
    if (isOpen == false) {
        return StatusCode::NotSupported();
    }
    if (readOnly) {
        return StatusCode::NotSupported();
    }
    std::string keyStr = key.toString();

    mapProtect.readLock();
    auto it = map.find(keyStr);
    if (it != map.end()) {
        // in map
        mapProtect.unlock();

        atomicDecrement(keyStr);
        return StatusCode::OK();
    }
    else {
        // not in map
        mapProtect.unlock();
        return StatusCode::KeyNotFound();
    }
}

StatusCode RefCountedDb::destroy() {
    if (isOpen == false) {
        FileTools::removeFile(filename);
        return StatusCode::OK();
    }
    else {
        LOG4IBS_WARN(logger, "Can't remove opened file '" << filename << "'");
        return StatusCode::NotSupported();
    }
}

void RefCountedDb::readFromDisk(std::unordered_map<std::string, std::string>& out) {
    out.clear();
    for (auto& pair : map) {
        std::string key;
        std::string fetched;
        key = pair.first;
        StatusCode st = get(key, &fetched);
        if (!key.empty() && !fetched.empty() && st.ok()) {
            out[key] = fetched;
        }
    }
}

} /* namespace ibs */
