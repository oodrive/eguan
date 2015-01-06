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
 * @file IbsHandler.h
 * @brief Ibs handler implementation/header
 * @author j. caba
 */
#ifndef IBSHANDLER_H_
#define IBSHANDLER_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "AbstractBlockStore.h"
#include "AbstractTransaction.h"
#include <memory>

namespace ibs {

class ConfigFileReader;

/**
 * @brief Handle activation/deactivation of "hot data" and transactions.
 */
class IbsHandler: public AtomicBlockStore, public Replaceable, public Destroyable {
    public:
        /**
         * @brief Constructor from configuration.
         * @param The in memory configuration.
         */
        IbsHandler(ConfigFileReader& cfg);

        /**
         * @brief Default destructor.
         */
        virtual ~IbsHandler();

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
         * with potentially a new key.
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

        // ----------------------------------------
        // IBS transaction operations
        // ----------------------------------------

        std::string createTransaction();

        StatusCode commitTransaction(const std::string& id) noexcept;

        void rollbackTransaction(const std::string& id);

        bool put(const std::string& id, const DataChunk&& key, const DataChunk&& value);

        void drop(const std::string& id, const DataChunk&& key);

        bool replace(const std::string& id, const DataChunk&& oldKey, const DataChunk&& newKey,
                const DataChunk&& value);

        // ----------------------------------------
        // Configuration operations
        // ----------------------------------------

        bool isHotDataEnabled();

    protected:

        void cleanTransaction() noexcept;

        void findTransaction(const std::string& id, std::shared_ptr<AbstractTransaction>& transaction);

    private:
        bool hotDataEnabled;
        std::unique_ptr<AtomicBlockStore> pImpl;
        Replaceable* replaceable;

        std::unordered_map<std::string, std::shared_ptr<AbstractTransaction>> idToTransactionMap; /** id<->transaction map */
        PosixRWLock idToTransactionMapLock; /** protect concurrent access to the transaction map */
};

} /* namespace ibs */
#endif /* IBSHANDLER_H_ */
