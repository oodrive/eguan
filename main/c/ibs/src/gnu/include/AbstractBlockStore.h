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
 * @file AbstractBlockStore.h
 * @brief The IBS abstract block store API/header
 * @author j. caba
 */
#ifndef ABSTRACTBLOCKSTORE_H_
#define ABSTRACTBLOCKSTORE_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "AbstractAtomicUpdate.h"
#include <memory>

namespace ibs {

/**
 * @brief A block store defines the interface for the facade
 * of the databases engines to store blocks and implement a block repository.
 *
 * All methods should be atomic and thread safe.
 */
class AbstractBlockStore {
    protected:
        AbstractBlockStore() = default;
    public:
        virtual ~AbstractBlockStore() {
        }

        /**
         * @brief Open the database.
         */
        virtual StatusCode open() = 0;

        /**
         * @brief Close the database.
         */
        virtual void close() = 0;

        /**
         * @brief Check if the database is closed.
         */
        virtual bool isClosed() = 0;

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
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value) = 0;

        /**
         * @brief  Get a value from the database when the size of the value
         * is unknown.
         *
         * @param  key The key used store a value.
         * @param  value A pointer to a string into which the value
         * will be store (and allocated).
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode get(const DataChunk&& key, std::string* value) = 0;

        /**
         * @brief  Check that the key exists in the database
         *
         * @return true if the key exists in the database, false otherwise.
         */
        virtual bool contains(const DataChunk&& key) = 0;

        /**
         * @brief  Drop the key (and the associated value) from the database
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key) = 0;

    private:
        // non copyable
        AbstractBlockStore(const AbstractBlockStore&) = delete;
        AbstractBlockStore& operator=(const AbstractBlockStore&) = delete;
};

/**
 * @brief A block store that support be atomic writes and stats.
 *
 * All methods should be atomic and thread safe.
 */
class AtomicBlockStore: public AbstractBlockStore {
    protected:
        AtomicBlockStore() = default;
    public:
        virtual ~AtomicBlockStore() {
        }

        /**
         * @brief Insert/delete a set of values in the database
         * as an atomic update.
         *
         * @param update The updates to apply to the database.
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode atomicWrite(AbstractAtomicUpdate& updates) = 0;

        /**
         * @brief Get a statistic description of the database for debug
         *
         * @return If the output data is valid
         */
        virtual bool getStats(std::string& output) = 0;

    private:
        // non copyable
        AtomicBlockStore(const AtomicBlockStore&) = delete;
        AtomicBlockStore& operator=(const AtomicBlockStore&) = delete;
};

/**
 * @brief Defines the interface for a block store
 * that can potentially be compressed. for the facade
 *
 * All methods should be atomic and thread safe.
 */
class CompressibleBlockStore: public AtomicBlockStore {
    protected:
        CompressibleBlockStore() = default;
    public:
        virtual ~CompressibleBlockStore() {
        }

        /**
         * @brief Enable embedded compression in leveldb.
         *
         * @remark The parameter will be taken in account at the next open.
         */
        virtual void enableCompression() = 0;

        /**
         * @brief Disable embedded compression in leveldb.
         *
         * @remark The parameter will be taken in account at the next open.
         */
        virtual void disableCompression() = 0;
};

/**
 * @brief Defines the interface for getStoragePath method
 */
class Storable {
    protected:
        Storable() = default;
        virtual ~Storable() {
        }
    public:
        /**
         * @brief Simply fetch the storage directory path name.
         *
         * @return The path of the directory/or file where the database is stored.
         */
        virtual const std::string& getStoragePath() const = 0;
};

/**
 * @brief Defines the interface for destroy method
 */
class Destroyable {
    protected:
        Destroyable() = default;
        virtual ~Destroyable() {
        }
    public:
        /**
         * @brief  Destroy the content of the database on disk.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode destroy() = 0;
};

class Reparable {
    protected:
        Reparable() = default;
        virtual ~Reparable() {
        }
    public:
        /**
         * @brief  Try to repair a database if it's "broken"
         * and open fails for example.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode repair() = 0;
};

/**
 * @brief Defines the interface for a block store
 * that can be stored in files on disk.
 *
 * All methods should be atomic and thread safe.
 */
class StorableBlockStore: public CompressibleBlockStore, public Storable, public Destroyable {
    protected:
        StorableBlockStore() = default;
};

/**
 * @brief Defines the interface for a block store
 * that can be stored in files on disk, and that can recover
 * from crash/reboot of machine by repair of file structure.
 * Potententially losing some of the lastest data added
 * but letting the database in a consistent state.
 *
 * All methods should be atomic and thread safe.
 */
class ReparableBlockStore: public StorableBlockStore, public Reparable {
    protected:
        ReparableBlockStore() = default;
};

class Replaceable {
    protected:
        Replaceable() = default;
        virtual ~Replaceable() {
        }
    public:

        /**
         * @brief Replace a value in database by another
         * with potentially a new key.
         *
         * @param oldKey The key used to initially store the old value.
         * @param newKey The key for the new value.
         * @param value  New value to store.
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) = 0;
};

} /* namespace ibs */
#endif /* ABSTRACTBLOCKSTORE_H_ */
