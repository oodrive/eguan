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
 * @file CombinedBlockStore.h
 * @brief The combined block store implementation/header
 * @author j. caba
 */
#ifndef COMBINEDBLOCKSTORE_H_
#define COMBINEDBLOCKSTORE_H_

#include "Logger.h"
#include "StatusCode.h"
#include "Locks.h"
#include "AbstractBlockStore.h"
#include "AbstractAtomicUpdate.h"
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace ibs {

class CombinedBlockStore;

/**
 * @brief Batch to store write in a CombinedBlockStore
 *
 * NOTE: Used for implementing transaction feature when "hot data" are disabled
 * and to dump data when "hot data" are enabled.
 * @see CombinedBlockStore
 */
class CombinedBSAtomicUpdate: public AbstractAtomicUpdate {
    public:
        CombinedBSAtomicUpdate(const CombinedBlockStore& parent, bool dropOld);

        virtual ~CombinedBSAtomicUpdate();

        // Store the mapping "key->value" in the database.
        virtual void put(const DataChunk& key, const DataChunk& value, bool compress);

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        virtual void drop(const DataChunk& key);

        // Store the mapping "newkey->value" in the database and remove oldKey mapping
        virtual void replace(const DataChunk& oldKey, const DataChunk& newkey, const DataChunk& value, bool compress);

        // Clear all updates buffered in this batch.
        virtual void clear();

    protected:
        void notAtomicPut(const DataChunk& key, const DataChunk& value, bool compress);

        void notAtomicDrop(const DataChunk& key);

    private:
        // non copyable
        CombinedBSAtomicUpdate(const CombinedBSAtomicUpdate&) = delete;
        CombinedBSAtomicUpdate& operator=(const CombinedBSAtomicUpdate&) = delete;

        std::atomic<bool> dropOldKey;
        std::vector<std::shared_ptr<AbstractAtomicUpdate>> ibpBatch;
        PosixSpinLock atomicProtection; /** ensure atomicity of operations */

        const CombinedBlockStore& owner;
        friend class CombinedBlockStore;

        static Logger_t logger;
};

/**
 * @brief Combine a set of Immutable block store. 
 *
 * This class manage a set of block store as AbstractBlockStore
 * like it was only one block store.
 * This is done by distributing keys in the different underlining block store.
 *
 * NOTE: The provided paths list should not be empty or the block store will not be valid.
 */
class CombinedBlockStore: public CompressibleBlockStore, public Destroyable, public Reparable {
    public:
        /**
         * @brief Constructor with storage path to the underlining block store.
         * @param path The storage paths.
         */
        CombinedBlockStore(const std::vector<std::string>& paths);

        /**
         * @brief Default destructor.
         */
        virtual ~CombinedBlockStore();

        /**
         * @brief Open the database.
         */
        virtual StatusCode open();

        /**
         * @brief Close the database.
         */
        virtual void close();

        /**
         * @brief Check if the database is closed.
         */
        virtual bool isClosed() {
            return !isOpen;
        }

        /**
         * @brief If this is a valid block store
         * i.e: if provided paths list is non empty.
         */
        bool isValid() {
            return dbs.size() > 0;
        }

        /**
         * @brief Put a value in the database.
         *
         * @param key A string that must be kept to be able
         * to get back the value. The key could be seen as buffer stored
         * in <code>key.data()</code> with a length of <code>key.size()</code>.
         * So the key does not have to be a <code>\0</code> terminated string.
         * @param value The value the caller wants to save.
         * Save bytes from <code>value.data()</code> with a length
         * <code>value.size()</code>.
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value);

        /**
         * @brief Look for a record.
         *
         * This method searches the key<->value data store 
         * to find a record associated to a key. Both key and
         * value are <code>std::string</code>. The length may vary.
         *
         * @remarks Blocking & thread safe.
         * @param key The key that identified requested data.
         * @param value A pointer to a <code>std::string</code>, 
         * to avoid useless <code>memcpy</code>, it the searched
         * value will be store inside this string.
         * @return An <code>ibs::Status</code> reflecting 
         * what happened during request.
         */
        virtual StatusCode get(const DataChunk&& key, std::string* value);

        /**
         * @brief Look for a key
         *
         * This method browse a key.
         *
         * @remarks Blocking & thread safe.
         * @return Return true iff the key already exists. 
         * Return false if the database is close or the key does not exists.
         */
        virtual bool contains(const DataChunk&& key);

        /**
         * @brief Drop a record.
         * @remarks Blocking & thread safe.
         * @param key The key associated to the record.
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key);

        /**
         * @brief Insert a set of records
         *
         * @param updates  The updates to apply to the database.
         * Must be an IbpSetAtomicUpdate instance.
         *
         * @return Always return OK.
         */
        virtual StatusCode atomicWrite(AbstractAtomicUpdate& updates);

        /**
         * @brief Get a statistic description of the database for debug
         * @return If the output data is valid
         */
        virtual bool getStats(std::string& output);

        /**
         * @brief Enable embedded compression in leveldb.
         *
         * @remark Can only change parameter if the block store is not open.
         */
        virtual void enableCompression();

        /**
         * @brief Disable embedded compression in leveldb.
         *
         * @remark Can only change parameter if the block store is not open.
         */
        virtual void disableCompression();

        /**
         * @brief  Destroy the content of the database on disk.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode destroy();

        /**
         * @brief  Try to repair a database if it's "broken"
         * and open fails for example.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode repair();

        /**
         * @brief Return the number of block store combined together.
         * @return the number of block store combined together.
         */
        size_t getSize() const {
            return dbs.size();
        }

        // Specific calls
        virtual void setBlockSize(uint64_t blockSize);
        virtual void setBlockRestartInterval(uint64_t blockRestartInterval);
        virtual void setWriteBufferSize(uint64_t writeBufferSize);
        virtual void setCacheSize(uint64_t cacheSize);

    protected:
        CombinedBlockStore() = delete;

        /**
         * @brief Method that create the concrete block store used.
         * By default use LevelDB as engine, override this method to use another
         * block store engine, like rocksDB, LMDB ...
         */
        virtual AtomicBlockStore* createConcrete(const std::string& dbPath);

        /**
         * @brief Calculate the hash as int of a key.
         */
        static uint32_t pseudoHash(const DataChunk& key) noexcept;

        /**
         * @brief Compute ID of a block store given a key.
         */
        uint32_t computeId(const DataChunk& key) const noexcept;

        /**
         * @brief Check and compute ID from a key to distribute blocks.
         */
        StatusCode checkAndComputeIdFromKey(const DataChunk& key, int& i) noexcept;

        friend class CombinedBSAtomicUpdate;

    private:
        // non copyable
        CombinedBlockStore(const CombinedBlockStore&) = delete;
        CombinedBlockStore& operator=(const CombinedBlockStore&) = delete;

        std::atomic<bool> isOpen; /** If the database is open */
        std::vector<std::shared_ptr<AtomicBlockStore>> dbs; /** db instances */
        PosixSpinLock protectCreateOpenClose; /** only protect open/close */
        PosixSpinLock atomicReplace; /** only protect replace from != block store */
        PosixSpinLock protectAtomicWrite; /** ensure atomicity of atomicWrite */
        PosixSpinLock protectCompressionChange; /** ensure atomicity of  enable/disable of compression */
        PosixSpinLock protectDestroyRepair; /** ensure atomicity of destroy/repair */
        PosixSpinLock protectCompactRange; /** ensure atomicity of CompactRange */

        static Logger_t logger;
};

} /* namespace ibs */
#endif /* COMBINEDBLOCKSTORE_H_ */
