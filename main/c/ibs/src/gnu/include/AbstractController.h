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
 * @file AbstractController.h
 * @brief The IBS abstract controller interface/header
 * @author j. caba
 */
#ifndef ABSTRACTCONTROLLER_H_
#define ABSTRACTCONTROLLER_H_

#include "DataChunk.h"
#include "StatusCode.h"

namespace ibs {

/**
 * @brief AbstractController interface.
 */
class AbstractController {
    public:
        /**
         * @brief Default destructor.
         */
        virtual ~AbstractController() {
        }

        /**
         * @brief Start the Ibs.
         *
         * Performs all starting tasks : start separate threads ....
         * @return Return true on success, false otherwise.
         */
        virtual bool start() = 0;

        /**
         * @brief Stop the Ibs.
         *
         * Performs all stopping tasks : stop separate threads, flush pending I/O ...
         * @return Return true on success, false otherwise.
         */
        virtual bool stop() = 0;

        /**
         * @brief Method to get the current Ibs UUID.
         * @return The unique UUID associated to the current Ibs.
         */
        virtual std::string getUuid() = 0;

        /**
         * @brief Method to get the owner's UUID
         * @return The unique UUID associated to the owner.
         */
        virtual std::string getOwnerUuid() = 0;

        /**
         * @brief Getter to get the current configuration file location.
         * @return The configuration file name.
         */
        virtual std::string getConfigFile() = 0;

        /**
         * @brief Handles a <i>PUT</i> request.
         *
         * This method save a record.
         *
         * @remarks Blocking & thread safe.
         * @param key A key to be able to get back the value. 
         * The key could be seen as buffer stored in <code>key.data()</code>
         * with a length of <code>key.size()</code>.
         * So the key does not have to be a <code>\0</code> terminated string.
         * @param value The value the caller wants to save.
         * Save bytes from <code>value.data()</code> with a length <code>value.size()</code>.
         * @return An <code>ibs::Status</code> reflecting what's happened during request.
         */
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value) = 0;

        /**
         * @brief Handles a <i>GET</i> request.
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @param value Where to store the value.
         * @param expected Expected size in case value is too small
         * @return An <code>ibs::Status</code> reflecting what's happened during request.
         */
        virtual StatusCode fetch(const DataChunk&& key, DataChunk&& value, size_t& expected) = 0;

        /**
         * @brief Handles a <i>DROP</i> request
         *
         * This method delete a record associated to a key.
         *
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @return An <code>ibs::Status</code> reflecting what's happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key) = 0;

        /**
         * @brief Handles a <i>REPLACE</i> request.
         *
         * This method issues a replace command. It replaces one old record by a new one. 
         * This method should be called to warn IBS that the old record
         * was a short-life record and there is no more reason to keep it.
         * This call is designed to deal with really short-life records.
         *
         * @remarks Blocking & thread safe.
         * @param oldKey The key used to initially store the old value.
         * @param newKey The key for the new value.
         * @param value A pointer to a string into which the value will be store.
         * @return An <code>ibs::Status</code> reflecting what's happened during request.
         */
        virtual StatusCode replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) = 0;

        /**
         * @brief Create a transaction in IbpGen and returns unique id of this transaction
         */
        virtual std::string createTransaction() = 0;

        /**
         * @brief Don't apply the updates stored in a transaction and destroy the transaction given by the id.
         */
        virtual void rollbackTransaction(const std::string& id) = 0;

        /**
         * @brief Apply the updates stored in a transaction and destroy the transaction given by the id.
         */
        virtual StatusCode commitTransaction(const std::string& id) = 0;

        /**
         * @brief Overloads put for transaction.
         * @return True only the key will be added false if it's already present in database.
         */
        virtual bool put(const std::string& id, const DataChunk&& key, const DataChunk&& value) = 0;

        /**
         * @brief Overloads replace for transaction.
         * @return True only the new key will be added false if it's already present in database.
         */
        virtual bool replace(const std::string& id, const DataChunk&& oldKey, const DataChunk&& newKey,
                const DataChunk&& value) = 0;

        /**
         * @brief Wipe the current IBS.
         *
         * This method is for special use only. 
         * It wipes the current IBPGEN including all on-disk data and all subsequent
         * IBP (along with their on-disk data). 
         * This method should be called at the end of each unit test to clean the environment.
         *
         * @remarks Be careful !!!
         * @return True if success, false if it fails to wipe all data.
         */
        virtual bool destroy() = 0;

        /**
         * @brief Getter for the hot data status
         * @return true if hot data are activated else false
         */
        virtual bool hotDataEnabled() = 0;

    protected:
        AbstractController() = default;

    private:
        // non copyable
        AbstractController(const AbstractController&) = delete;
        AbstractController& operator=(const AbstractController&) = delete;
};

} /* namespace ibs */

#endif /* ABSTRACTCONTROLLER_H_ */
