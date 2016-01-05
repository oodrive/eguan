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
 * @file IbpGenHandler.h
 * @brief IbpGen handler implementation/header
 * @author j. caba
 */
#ifndef IBPGENHANDLER_H_
#define IBPGENHANDLER_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "RefCountedDb.h"
#include "IbpHandler.h"
#include "CreationTask.h"
#include "DumpTask.h"
#include "ConcurrentFIFO.h"
#include "Locks.h"

#include <string>
#include <atomic>
#include <list>
#include <utility>

namespace ibs {

class ConfigFileReader;

/**
 * @brief Regroup values for the put/write slow down or config in IbpGenHandler
 * @see IbpGenHandler
 */
struct DelayValues {
        /* slow down value */
        std::atomic<int> threshold; /** write delay triggering threshold */
        std::atomic<int> levelSize; /** size of one delay level once threshold is reached */
        std::atomic<int> incrUs; /** delay in microseconds to add for each delay level above threshold */
        std::atomic<bool> active; /** the flag indicating if writes are delayed */
        std::atomic<int> value; /** the current delay value in microseconds */

        /* config values */
        std::atomic<uint64_t> writeCreationMaxDelay; /** maximum delay before creation of a buffer */
        std::atomic<uint64_t> writeCreationThreshold; /** maximum number of write before creation of a buffer */
        std::atomic<uint64_t> dumpAtStopLimit; /** limit in seconds to spend dumping at stop */
};

/**
 * @brief Regroup locks used in IbpGenHandler implementation
 * @see IbpGenHandler
 */
struct IbpGenLocks {
        /**
         * @brief Ensure data integrity with multi-thread calls
         */
        PosixRWLock dataIntegrity;

        /**
         * @brief Ensure atomicWrite is atomic.
         */
        PosixMutex atomicWriteMutex;

        /**
         * @brief Ensure replace is atomic.
         */
        PosixMutex atomicReplaceMutex;

        /**
         * @brief Ensure dump is incremental and serialized.
         */
        PosixMutex serializeDumpMutex;
};

/**
 * @brief Implement AbstractAtomicUpdate interface for IbpGenHandler
 * for transaction feature when "hot data" are enabled.
 * @see IbpGenHandler
 */
class IbpGenAtomicUpdate: public AbstractAtomicUpdate {
    public:
        IbpGenAtomicUpdate() :
                AbstractAtomicUpdate(), keysToDropOrPut(), uuid(), atomicLock(), keysToAddOnRam() {
            generateUuid();
            createDb();
        }
    public:
        virtual ~IbpGenAtomicUpdate() {
            // release allocated memory
            clear();
        }

        // Store the mapping "key->value" in the database.
        virtual void put(const DataChunk& key, const DataChunk& value, bool /*compress*/) {
            atomicLock.lock();
            notAtomicPut(key, value);
            atomicLock.unlock();
        }

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        virtual void drop(const DataChunk& key) {
            atomicLock.lock();
            notAtomicDrop(key);
            atomicLock.unlock();
        }

        // Store the mapping "newkey->value" in the database
        // and remove oldKey mapping if not used anymore
        virtual void replace(const DataChunk& oldKey, const DataChunk& newKey, const DataChunk& value, bool /*compress*/) {
            atomicLock.lock();
            notAtomicDrop(oldKey);
            if (newKey.isReferencing(NULL) == false) {
                notAtomicPut(newKey, value);
            }
            atomicLock.unlock();
        }

        // Clear all updates buffered in this atomic update.
        virtual void clear() {
            atomicLock.lock();
            for (auto kv : keysToDropOrPut) {
                kv.first.clear();
            }
            keysToDropOrPut.clear();
            destroyDb();
            atomicLock.unlock();
        }

    protected:
        void createDb() {
            keysToAddOnRam.clear();
        }
        void destroyDb() {
            keysToAddOnRam.clear();
        }
        // Store the mapping "key->value" in the database.
        virtual void notAtomicPut(const DataChunk& key, const DataChunk& value) {
            std::string sKey;
            sKey.assign(key.toString());
            keysToAddOnRam[sKey] = value.toString();
            keysToDropOrPut.emplace_back(toStore_t(sKey, PUT));
        }

        // If the database contains a mapping for "key", erase it.  Else do nothing.
        virtual void notAtomicDrop(const DataChunk& key) {
            std::string sKey;
            sKey.assign(key.toString());
            keysToDropOrPut.emplace_back(toStore_t(sKey, DROP));
        }
    private:
        friend class IbpGenHandler;
        enum OPCODE {
            DROP, PUT
        };
        // list of key<->value in order of additions
        // first element in the pair is the key
        // second element is opcode DROP or PUT
        typedef std::pair<std::string, OPCODE> toStore_t;
        std::list<toStore_t> keysToDropOrPut;
        std::string uuid; // unique id of the transaction
        PosixSpinLock atomicLock; // ensure each operation are "atomic"
        std::unordered_map<std::string, std::string> keysToAddOnRam; // hash table to store the keys and value to add
};

/**
 * @brief Handle a buffer mechanism to give a chance for temporary values
 * not be be persisted. This buffer mechanism is used on top of an IbpHandler
 * that persist all the data.
 *
 * @see IbpHandler
 */
class IbpGenHandler: public AtomicBlockStore, public Replaceable, public Destroyable {
    public:
        /**
         * @brief Constructor from configuration.
         * @param The in memory configuration.
         */
        IbpGenHandler(ConfigFileReader& cfg);

        /**
         * @brief Default destructor.
         */
        virtual ~IbpGenHandler();

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
        virtual bool isClosed();

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
         * @brief Replace a value in database by another
         * with potentially a new key atomically.
         *
         * Don't remove data persisted only data waiting to be persisted.
         *
         * @param oldKey The key used to initially store the old value.
         * @param newKey The key for the new value.
         * @param value  New value to store.
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value);

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
        virtual StatusCode get(const DataChunk&& key, std::string* value);

        /**
         * @brief  Check that the key exists in the database
         *
         * @return true if the key exists in the database, false otherwise.
         * Note: normally this method should be a
         * combination of isPersisted and isWaitingPersistence methods.
         */
        virtual bool contains(const DataChunk&& key);

        /**
         * @brief  Drop the key (and the associated value) from the database
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key);

        /**
         * @brief Insert/delete a set of values in the database
         * as an atomic update.
         *
         * @param update The updates to apply to the database.
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode atomicWrite(AbstractAtomicUpdate& updates);

        /**
         * @brief Get a statistic description of the database for debug
         *
         * Describe the persisted data state.
         *
         * @return If the output data is valid
         */
        virtual bool getStats(std::string& output);

        /**
         * @brief Wipe the data in the IBP directory on disk.
         * This method is for special use only.
         * @remarks Be careful !!!
         * @return Always return OK.
         */
        virtual StatusCode destroy();

    protected:
        IbpGenHandler() = delete;

        /**
         * @brief Re inject old data (buffers) in case of reboot/crash of machine
         */
        void reInjectOldData();

        /**
         * @brief Promote all data that was not persisted yet.
         * @param timed limit for operation.
         */
        void promoteAllData(const uint64_t secondsLimit);

        /**
         * @brief Promote oldest data that was not persisted yet.
         */
        void promoteOldestData();

        /**
         * @brief Add key in newest buffer.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         */
        StatusCode putInNewestBuffer(const DataChunk& key, const DataChunk& value);

        /**
         * @brief Drop key from buffers.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         */
        bool dropFromBuffers(const DataChunk& key);

        /**
         * @brief Get key from buffers.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         */
        StatusCode getFromBuffers(const DataChunk&& key, std::string* value) noexcept;

        /**
         * @brief Check if key is in buffers.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         */
        bool isInBuffers(const DataChunk& key);

        /**
         * @brief Destroy all buffers.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         */
        void destroyAllBuffers();

        /**
         * @brief Search for a key and increment it's reference counter if known.
         * Do nothing if the key is not known.
         *
         * NOTE: This method is NOT thread safe
         * and is protected by dataIntegrity lock.
         *
         * @param key The key to search.
         * @return True if key already exists, false otherwise.
         */
        bool searchNIncrement(const DataChunk& key);

        /**
         * @brief Update the slow-down delay based on number of buffers.
         */
        void updateWriteDelay() noexcept;

        /**
         * @brief Get the directory containing this database.
         */
        std::string getBaseDirectory() const {
            return baseDirectory;
        }

        /**
         * @brief Return allocated buffer (need to be freed)
         */
        RefCountedDb* createOpenNewBuffer(const std::string& dbFile);

        /**
         * @brief Return the limit of time before creating a new buffer
         * if not created by number of write criteria.
         * @see getWriteMaxDelay
         */
        uint64_t getWriteMaxDelay() {
            return delay.writeCreationMaxDelay;
        }

        /**
         * @brief Return the limit of write before creating a new buffer
         * if not created by time criteria.
         * @see getWriteMaxDelay
         */
        uint64_t getWriteThreshold() {
            return delay.writeCreationThreshold;
        }

    private:
        // non copyable
        IbpGenHandler(const IbpGenHandler&) = delete;
        IbpGenHandler& operator=(const IbpGenHandler&) = delete;

        const std::string baseDirectory;/** where to store buffers on disk */

        std::atomic<bool> closed; /** is db closed/open ie: stopped/started ... */
        std::atomic<bool> dumpError; /** if an error occurred during dump (no more space on disk) ... */

        DelayValues delay; /** delay values for the put slow down implemented or config */

        /**
         * @brief Thread that create buffers
         */
        CreationTask creationLoop;
        friend class CreationTask;

        /**
         * @brief Thread that dump/persist buffers before destruction.
         */
        DumpTask dumpLoop;
        friend class DumpTask;

        /**
         * @brief buffer databases FIFO owning the database (memory)
         */
        ConcurrentFIFO<RefCountedDb> buffers;

        /**
         * @brief Persisted data are on ibp
         */
        IbpHandler persistedData;

        /**
         * @brief All locks used to ensure multi-thread safety
         */
        IbpGenLocks locks;
};

}
/* namespace ibs */
#endif /* IBPGENHANDLER_H_ */
