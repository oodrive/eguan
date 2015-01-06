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
 * @file LevelDbFacade.h
 * @brief The leveldb facade/header
 * @author j. caba
 */
#ifndef LEVELDBFACADE_H_
#define LEVELDBFACADE_H_

#include "Logger.h"
#include "StatusCode.h"
#include "AbstractBlockStore.h"
#include "AbstractAtomicUpdate.h"

/* leveldb headers */
#include <leveldb/db.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>
#include <leveldb/filter_policy.h>
#include <leveldb/write_batch.h>
#include <leveldb/cache.h>

namespace ibs {

class LevelDbAtomicUpdate: public AbstractAtomicUpdate {
    public:
        LevelDbAtomicUpdate() :
                AbstractAtomicUpdate(), batch() {
        }

        virtual ~LevelDbAtomicUpdate();

        // Store the mapping "key->value" in the database.
        virtual void put(const DataChunk& key, const DataChunk& value, bool compress);

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        virtual void drop(const DataChunk& key);

        // Store the mapping "newkey->value" in the database and remove oldKey mapping
        virtual void replace(const DataChunk& oldKey, const DataChunk& newKey, const DataChunk& value, bool compress) {
            drop(oldKey);
            put(newKey, value, compress);
        }

        // Clear all updates buffered in this batch.
        virtual void clear() {
            batch.Clear();
        }
    private:
        friend class LevelDbFacade;
        leveldb::WriteBatch batch;
};

/**
 * @class Implement a facade for LevelDb database conforming to AbstractBlockStore
 * All methods are atomic and thread safe.
 */
class LevelDbFacade: public ReparableBlockStore {
    public:
        /**
         * @brief Constructor with storage path.
         * @param path The storage path.
         */
        LevelDbFacade(const std::string& path, bool createCache, bool createBloomFilter, bool isCompressed);

        /**
         * @brief Close the database at destruction.
         */
        virtual ~LevelDbFacade();

        /**
         * @brief Enable embedded compression in leveldb.
         * @remark The parameter will be taken in account at the next open.
         */
        virtual void enableCompression();

        /**
         * @brief Disable embedded compression in leveldb.
         * @remark The parameter will be taken in account at the next open.
         */
        virtual void disableCompression();

        /**
         * @brief Simply fetch the storage directory.
         * @return The path of the directory where the leveldb database is stored.
         */
        virtual const std::string& getStoragePath() const {
            return leveldbPath;
        }

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
         * @brief Put a value in the database.
         *
         * @param key A string that must be kept to be able to get back the value. The key could be seen as buffer
         * stored in <code>key.data()</code> with a length of <code>key.size()</code>. So the key does not have to be
         * a <code>\0</code> terminated string.
         * @param value The value the caller wants to save. Save bytes from <code>value.data()</code> with a length
         * <code>value.size()</code>.
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value);

        /**
         * @brief  Get a value from the database when the size of the value is unknown.
         * @param  key The key used store a value.
         * @param  value A pointer to a string into which the value will be store (and allocated).
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode get(const DataChunk&& key, std::string* value);

        /**
         * @brief   Check that the key exists in the database
         * @return  true if the key exists in the database, false otherwise.
         * @remarks If the database is closed return false.
         */
        virtual bool contains(const DataChunk&& key);

        /**
         * @brief  Drop the key (and the associated value) from the database
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key);

        /**
         * @brief  Destroy the content of the database
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode destroy();

        /**
         * @brief  Try to repair a database if it's "broken"
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode repair();

        /**
         * @brief Insert/delete a set of values in the database as an atomic update.
         * @param update The updates to apply to the database.
         * Must be an LevelDbAtomicUpdate instance.
         *
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode atomicWrite(AbstractAtomicUpdate& updates);

        /**
         * @brief Get a statistic description of the database for debug
         * @return If the output data is valid
         */
        virtual bool getStats(std::string& output) {
            if (isOpen && db.get() != NULL) {
                return db->GetProperty("leveldb.stats", &output);
            }
            else {
                output.assign("Database '" + leveldbPath + "' closed.");
                return true;
            }
        }

        // leveldb specific calls
        void setBlockSize(uint64_t blockSize);
        void setBlockRestartInterval(uint64_t blockRestartInterval);
        void setWriteBufferSize(uint64_t writeBufferSize);
        void setCacheSize(uint64_t cacheSize);

        /**
         * @brief Conversion from DataChunk to leveldb::Slice
         *
         * Create a leveldb::slice that refers to the data in chunk.
         */
        static leveldb::Slice toSlice(const DataChunk& chunk) noexcept;

        /**
         * @briefConversion to StatusCode from leveldb::Status
         */
        static StatusCode fromStatus(const leveldb::Status& st) noexcept;

    protected:
        // non copyable
        LevelDbFacade(const LevelDbFacade&) = delete;
        LevelDbFacade& operator=(const LevelDbFacade&) = delete;
        friend class LevelDbFacadeIterator;

    private:
        const bool hasCache; /** If a cache was created */
        const bool hasBloomFilter; /** If a bloom filter was created */
        std::atomic<bool> isOpen; /** If the database is open */
        std::unique_ptr<leveldb::DB> db; /** LevelDb instance. */
        std::unique_ptr<leveldb::Cache> cache; /** LevelDb read block cache */
        std::unique_ptr<const leveldb::FilterPolicy> filter; /** LevelDb filter */
        leveldb::WriteOptions wOptions; /** Default write options. */
        leveldb::ReadOptions rOptions; /** Default read options. */
        leveldb::Options options; /** Leveldb options. Used at initialization time. */
        const std::string leveldbPath; /** file system location for the levelDB database. */
        std::atomic<bool> isCompacting; /** If manual compaction is pending */

        static leveldb::Options defaultOptions; // to allocate default environment
        static Logger_t logger;
};

} /* namespace ibs */
#endif /* LEVELDBFACADE_H_ */
