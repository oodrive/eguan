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
 * @file AbstractAtomicUpdate.h
 * @brief The IBS abstract atomic update API/header
 * @author j. caba
 */

#ifndef ABSTRACTATOMICUPDATE_H_
#define ABSTRACTATOMICUPDATE_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "Locks.h"
#include <string>
#include <unordered_map>
#include <unordered_set>

/* lib UUID */
#include <uuid/uuid.h>

namespace ibs {

/**
 * @brief Defines the interface for the atomic updates for the block stores.
 *
 * All methods should be atomic and thread safe.
 * @see AbstractBlockStore
 */
class AbstractAtomicUpdate {
    protected:
        AbstractAtomicUpdate() :
                uuid() {
            generateUuid();
        }
    public:
        virtual ~AbstractAtomicUpdate() {
        }

        // Store the mapping "key->value" in the database.
        virtual void put(const DataChunk& key, const DataChunk& value, bool compress = false) = 0;

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        virtual void drop(const DataChunk& key) = 0;

        // Store the mapping "newkey->value" in the database and remove oldKey mapping
        virtual void replace(const DataChunk& oldKey, const DataChunk& newkey, const DataChunk& value, bool compress = false) = 0;

        // Clear all updates buffered in this batch.
        virtual void clear() = 0;

        // get unique id of the transaction
        const std::string& getUuid() {
            return uuid;
        }

    protected:
        void generateUuid() {
            uuid_t _uuid;
            uuid_generate(_uuid);
            char* c_uuid = new char[64];
            uuid_unparse(_uuid, c_uuid);
            std::string out(c_uuid);
            delete[] c_uuid;
            this->uuid = out;
        }

    private:
        // non copyable
        AbstractAtomicUpdate(const AbstractAtomicUpdate&) = delete;
        AbstractAtomicUpdate& operator=(const AbstractAtomicUpdate&) = delete;

        std::string uuid; // unique id of the transaction
};

} /* namespace ibs */

#endif /* ABSTRACTATOMICUPDATE_H_ */
