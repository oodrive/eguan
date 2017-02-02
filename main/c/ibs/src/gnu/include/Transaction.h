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
 * @file Transaction.h
 * @brief The transaction implementation/header
 * @author j. caba
 */
#ifndef TRANSACTION_H_
#define TRANSACTION_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "AbstractTransaction.h"
#include "AbstractAtomicUpdate.h"
#include "AbstractBlockStore.h"
#include "Locks.h"
#include "Logger.h"
#include <string>
#include <memory>
#include <atomic>
#include <unordered_map>
#include <cstdint>

namespace ibs {

/**
 * @class Defines an implementation for a transaction in a block store.
 */
class Transaction: public AbstractTransaction {
    public:
        Transaction(AtomicBlockStore& in);

        virtual ~Transaction();

        virtual int getId() const;

        static bool getUuidFromId(int id, std::string& outputUuid);

        static bool getIdFromUuid(const std::string& uuid, int& outputId);

        virtual const std::string& getUuid() const;

        virtual bool put(const DataChunk& key, const DataChunk& value);

        virtual void drop(const DataChunk& key);

        virtual bool replace(const DataChunk& oldKey, const DataChunk& newkey, const DataChunk& value);

        virtual bool contains(const DataChunk& key);

        virtual void rollback();

        virtual StatusCode commit();
    protected:
        static void incrementCounter();
        static int generateId(const std::string& uuidToMap);
        static void releaseId(int idToRelease);

        /**
         * @brief Bind an id to a uuid
         */
        void bindId();

        /**
         * @brief To ensure the id is unbind only once, only if it wasn't done before.
         * Call this method to remove id association with uuid.
         */
        void unbindId();
    private:
        // non copyable
        Transaction(const Transaction&) = delete;
        Transaction& operator=(const Transaction&) = delete;

        std::unique_ptr<AbstractAtomicUpdate> updates; // updates to apply in commit
        std::string uuid;
        std::atomic<int> id;
        std::atomic<bool> isIdBinded;
        std::atomic<bool> compress;

        typedef std::unordered_map<std::string, int> uuidToId_t;
        typedef std::unordered_map<int, std::string> idToUuid_t;
        static uuidToId_t uuidToId; /** map a uuid to an id */
        static idToUuid_t idToUuid; /** map an id to a uuid */
        static PosixRWLock mapLock; /** protected the maps against concurrent access */
        static int64_t global_counter; /** global counter to generate integer id to map with uuid. (value zero is reserved) */
        static Logger_t logger;
};

} /* namespace ibs */

#endif /* TRANSACTION_H_ */
