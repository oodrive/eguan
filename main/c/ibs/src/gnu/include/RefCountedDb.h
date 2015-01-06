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
 * @file RefCountedDb.h
 * @brief The reference counted db/header
 * @author j. caba
 */
#ifndef REFCOUNTEDDB_H_
#define REFCOUNTEDDB_H_

#include "Locks.h"
#include "StatusCode.h"
#include "AbstractBlockStore.h"
#include "RefCountedDbFile.h"
#include <atomic>
#include <unordered_map>
#include <string>
#include <utility>
#include <cstdint>

namespace ibs {

/**
 * @brief A reference counted simple database implemented as a file on disk.
 *
 * It contains classical put/drop/get calls and handling of
 * reference counting of put/drop.
 * Once a key is put with it's value the value can't be changed,
 * only the reference counter describing number of use of this key.
 *
 * On drop the reference counter is decremented.
 * When a key is not referenced anymore it's still contained to not loose data
 * but will not be reported in get/contains operations.
 * A put will increment the reference counter and the value associated
 * can then be recovered.
 *
 * Most implementation details are in RefCountedDbFile class,
 * implementing the file format of database.
 *
 * @see RefCountedDbFile
 */
class RefCountedDb: public AbstractBlockStore, public Storable, public Destroyable {
    public:
        RefCountedDb(const std::string& fname);

        /**
         * @brief Destructor that delete file.
         */
        virtual ~RefCountedDb();

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
         */
        virtual bool contains(const DataChunk&& key);

        /**
         * @brief Search for a key and increment it's reference counter if known.
         * Do nothing if the key is not known.
         *
         * @param key The key to search.
         * @return True if key already exists, false otherwise.
         */
        bool searchNIncrement(const DataChunk&& key);

        /**
         * @brief  Drop the key (and the associated value) from the database
         *
         * Decrement reference and remove only is not referenced anymore.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key);

        /**
         * @brief Return the path of underling file of database.
         * @return The path of underling file of database.
         */
        const std::string& getStoragePath() const {
            return filename;
        }

        /**
         * @brief  Destroy the content of the database on disk.
         *
         * @return An <code>ibs::Status</code> reflecting
         * what happened during request.
         */
        virtual StatusCode destroy();

        /**
         * @brief Read referenced keys from disk.
         * @param out Where to store the key/value pairs.
         */
        void readFromDisk(std::unordered_map<std::string, std::string>& out);

        /**
         * @brief "lock" the base to read only mode.
         * Once the database is read only no write is possible.
         *
         * @see isReadOnly
         */
        void setReadOnly() {
            readOnly = true;
        }

        /**
         * @brief "unlock" the base to from read only mode.
         *
         * @see setReadOnly
         */
        void setReadWrite() {
            readOnly = false;
        }

        /**
         * @brief Indicate if the data base is "locked" in read only mode.
         * @return The boolean indicating if the data is "read only"
         * @see setReadOnly
         */
        bool isReadOnly() {
            return readOnly;
        }

    protected:
        /**
         * @brief Atomically increment counter of a key in database.
         * @param keyStr The key to search for.
         */
        void atomicIncrement(const std::string& keyStr);

        /**
         * @brief Atomically decrement counter of a key in database.
         * @param keyStr The key to search for.
         */
        void atomicDecrement(const std::string& keyStr);

    private:
        // non copyable
        RefCountedDb(const RefCountedDb&) = delete;
        RefCountedDb& operator=(const RefCountedDb&) = delete;

        PosixRWLock mapProtect; /** protect map iterator invalidation */
        std::atomic<bool> isOpen; /** if the database is open */
        std::atomic<bool> readOnly; /** if base is "locked" in read only mode */
        std::unique_ptr<RefCountedDbFile> file; /** file handle where everything is stored, should not be protected by external lock (already protected internally) */
        std::string filename; /** file name where everything is stored */

        /** Key<->(Reference counter,offset in file) association using a hash map */
        std::unordered_map<std::string, std::pair<uint64_t, std::ios::pos_type>> map;
};

}
/* namespace ibs */

#endif /* REFCOUNTEDDB_H_ */
