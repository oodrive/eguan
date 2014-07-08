/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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
 * @file Controller.h
 * @brief The IBS controller/header
 */
#ifndef IBS_CONTROLLER_H_
#define IBS_CONTROLLER_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "AbstractController.h"
#include <vector>
#include <memory>

/* lib UUID */
#include <uuid/uuid.h>

namespace ibs {

class ConfigFileReader;
class IbsHandler;

/**
 * @brief Controller class.
 *
 * This class implements a minimal factory.
 * It provides all method required to initialize and validate configuration
 * to create and use the Immutable Block System.
 */
class Controller: public AbstractController {
    public:
        /**
         * @brief Default destructor.
         */
        virtual ~Controller();

        /**
         * @brief Controller Factory for a new IBS
         * @param fname The configuration filename.
         * @param po_controller A controller pointer if success, NULL otherwise
         * @param checkRecorderOption to avoid infinite loop while using recorder
         * @return a StatusCode object to track what happens
         */
        static StatusCode create(const std::string& fname, AbstractController*& po_controller,
        bool checkRecorderOption = true);

        /**
         * @brief Controller Factory for initializing an existing IBS
         * @param fname The configuration filename.
         * @param po_controller A controller pointer if success, NULL otherwise
         * @return a StatusCode object to track what happens
         */
        static StatusCode init(const std::string& fname, AbstractController*& po_controller,
        bool checkRecorderOption = true);

        /**
         * @brief Start the Ibs.
         *
         * Performs all starting tasks : management loop and workers.
         */
        virtual bool start();

        /**
         * @brief Stop the Ibs.
         *
         * Performs all stopping tasks : management loop, workers and flush pending I/O.
         */
        virtual bool stop();

        /**
         * @brief Method to get the current Ibs UUID.
         * @return The unique UUID associated to the current Ibs.
         */
        virtual std::string getUuid();

        /**
         * @brief Method to get the owner's UUID
         * @return The unique UUID associated to the owner.
         */
        virtual std::string getOwnerUuid();

        /**
         * @brief Getter to get the current configuration location.
         * @return The configuration file name.
         */
        virtual std::string getConfigFile();

        /**
         * @brief Handles a <i>PUT</i> request.
         *
         * This method save a record.
         *
         * @remarks Blocking & thread safe.
         * @param key A string that must be kept to be able to get back the value. The key could be seen as buffer
         * stored in <code>key.data()</code> with a length of <code>key.size()</code>. So the key does not have to be
         * a <code>\0</code> terminated string.
         * @param value The value the caller wants to save. Save bytes from <code>value.data()</code> with a length
         * <code>value.size()</code>.
         * @return An <code>ibs::StatusCode</code> reflecting what happened during request.
         */
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value);

        /**
         * @brief Handles a <i>GET</i> request.
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @param value Where to store the value.
         * @param expected Expected size in case value is too small
         * @return An <code>ibs::StatusCode</code> reflecting what happened during request.
         */
        virtual StatusCode fetch(const DataChunk&& key, DataChunk&& value, size_t& expected);

        /**
         * @brief Handles a <i>DEL</i> request
         *
         * This method delete a record associated to a key.
         *
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @return An <code>ibs::StatusCode</code> reflecting what happened during request.
         */
        virtual StatusCode drop(const DataChunk&& key);

        /**
         * @brief Handles a <i>REPLACE</i> request.
         *
         * This method issues a replace command. It replaces one old record by a new one. This method should be called
         * to warn IBS that the old record was a short-life record and there is no more reason to keep it. This call is
         * designed to deal with really short-life records. This call could also be associated to a context ID. It
         * allows the caller to use the engine storage for many use cases.
         *
         * @remarks Blocking & thread safe.
         * @param oldKey The key used to initially store the old value.
         * @param newKey The key for the new value.
         * @param value A pointer to a string into which the value will be store.
         * @return An <code>ibs::StatusCode</code> reflecting what happened during request.
         */
        virtual StatusCode replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value);

        /**
         * @brief Create a transaction in IbpGen and returns unique id of this transaction
         */
        virtual std::string createTransaction();

        /**
         * @brief Don't apply the updates stored in a transaction and destroy the transaction given by the id.
         */
        virtual void rollbackTransaction(const std::string& id);

        /**
         * @brief Apply the updates stored in a transaction and destroy the transaction given by the id.
         */
        virtual StatusCode commitTransaction(const std::string& id);

        /**
         * @brief Overloads put for transaction.
         * @return True only the key will be added false if it's already present in database.
         */
        virtual bool put(const std::string& id, const DataChunk&& key, const DataChunk&& value);

        /**
         * @brief Overloads replace for transaction.
         * @return True only the new key will be added false if it's already present in database.
         */
        virtual bool replace(const std::string& id, const DataChunk&& oldKey, const DataChunk&& newKey,
                const DataChunk&& value);

        /**
         * @brief Wipe the current IBS.
         *
         * This method is for special use only. It wipes the current IBPGEN including all on-disk data and all subsequent
         * IBP (along with their on-disk data). This method is actually at the end of each test to clean the
         * environment.
         *
         * @remarks Be careful !!!
         * @return 0 if success, 1 if it fails to wipe all data.
         */
        virtual bool destroy();

        /**
         * @brief Getter for the hot data status
         * @return true if hot data are activated else false
         */
        virtual bool hotDataEnabled();

    protected:
        Controller() = default;

        // non copyable
        Controller(const Controller&) = delete;
        Controller& operator=(const Controller&) = delete;

        /**
         * @brief Factory with behavior configured for a create or a start on existing
         */
        static StatusCode initCommon(const std::string& fname, AbstractController*& po_controller,
        bool isCreate, bool checkRecorderOption);

        /**
         * @brief Check that the persisted configuration file is consistent with the new one, checking ibp path.
         * @param Absolute path of the ibpgen
         * @param Contain the ipb paths "list"
         * @return false only if the new configuration is not consistent concerning ibp path
         */
        bool checkIbpPath(const std::string& old_ibpgen_path, const std::vector<std::string>& ibp_paths);

        /**
         * @brief Check that the persisted configuration file is consistent with the new one, checking uuid, owner and compression.
         * @param Absolute path of the ibpgen
         * @return false only if the new configuration is not consistent concerning uuid and owner.
         */
        bool checkUuidOwnerAndCompression(const std::string& ibpgen_path);

        /**
         * @brief Internal configuration checking routine : common part
         */
        StatusCode checkConfigCommon(bool isCreating);

        /**
         * @brief Set the uuid.
         */
        int setUuid(const std::string& str_uuid);

        /**
         * @brief Set the owner uuid.
         */
        int setOwnerUuid(const std::string& str_uuid);

        /**
         * @brief Init log.
         */
        void initLog();

        /**
         * @brief Check for authorized keys.
         */
        StatusCode checkForKnownConfigurationKey();

    private:
        uuid_t uuid; /** local uuid */
        uuid_t ownerUuid; /** owner uuid */
        std::unique_ptr<ConfigFileReader> config; /** Local Configurator object. */
        std::unique_ptr<IbsHandler> pImpl; /** Ibs main class */
        std::string configFile; /** Configuration file path. */
};

} /* namespace ibs */

extern "C" void libibs_is_present(); /* convenient autotools testing method */

#endif /* IBS_CONTROLLER_H_ */
