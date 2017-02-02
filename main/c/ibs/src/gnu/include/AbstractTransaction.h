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
 * @file AbstractTransaction.h
 * @brief The abstract db transaction API/header
 * @author j. caba
 */
#ifndef ABSTRACTTRANSACTION_H_
#define ABSTRACTTRANSACTION_H_

#include "StatusCode.h"
#include "DataChunk.h"
#include "AbstractBlockStore.h"

namespace ibs {

/**
 * @class Defines the interface for a transaction in a block store.
 */
class AbstractTransaction {
    public:
        AbstractTransaction(AtomicBlockStore& in) :
                db(in) {
        }
        virtual ~AbstractTransaction() {
        }

        /**
         * @brief Export uuid as ID long.
         * @see getUuid
         */
        virtual int getId() const = 0;

        /**
         * @brief Return the unique id used for management of transaction.
         */
        virtual const std::string& getUuid() const = 0;

        /**
         * @brief Store the mapping "key->value" in the database.
         * @return True only the key will be added false if it's already present in database.
         */
        virtual bool put(const DataChunk& key, const DataChunk& value) = 0;

        /**
         * @brief If the database contains a mapping for "key", erase it.  Else do nothing.
         * WARNING: Use this method with cautious has it will remove data permanently!
         */
        virtual void drop(const DataChunk& key) = 0;

        /**
         * @brief Store the mapping "newkey->value" in the database and remove oldKey mapping.
         * @return True only the newkey will be added false if it's already present in database.
         */
        virtual bool replace(const DataChunk& oldKey, const DataChunk& newkey, const DataChunk& value) = 0;

        /**
         * @brief Check if the key already exist in a database
         */
        virtual bool contains(const DataChunk& key) = 0;

        /**
         * @brief Clear all the pending changes.
         */
        virtual void rollback() = 0;

        /**
         * @brief Commit the changes in a database and clear all the pending changes.
         */
        virtual StatusCode commit() = 0;

    protected:
        AtomicBlockStore& db; /** db to modify */

    private:
        // non copyable
        AbstractTransaction(const AbstractTransaction&) = delete;
        AbstractTransaction& operator=(const AbstractTransaction&) = delete;
};

} /* namespace ibs */
#endif /* ABSTRACTTRANSACTION_H_ */
