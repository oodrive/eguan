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
 * @file libibs.h
 * @brief libibs C bindings
 */
#ifndef LIBCIBS_H_
#define LIBCIBS_H_

/* handle both C & C++ */
#ifdef __cplusplus
extern "C" {
#endif

#define TRUE 1
#define FALSE 0

/**
 * @brief Error code definition
 *
 * Error codes MUST be greater then -127. Those error codes definitions impact
 * java code built on top of this library : if a code is added/removed/created
 * the associated java code must be updated accordingly.
 */
enum ibsErrorCode {
    /** IBS does not exist */
    IBS__INVALID_IBS_ID = -1,
    /** Record not found (get) or on put (refresh)*/
    IBS__KEY_NOT_FOUND = -2,
    /** The given buffer is too small for the record to get */
    IBS__BUFFER_TOO_SMALL = -3,
    /** The underlying database is corrupted */
    IBS__DATA_CORRUPTION = -4,
    /** Error during the read or the write of the datas */
    IBS__IO_ERROR = -5,
    /** Configuration error */
    IBS__CONFIG_ERROR = -6,
    /** Create on non-empty */
    IBS__CREATE_IN_NON_EMPTY_DIR = -7,
    /** Init on empty */
    IBS__INIT_FROM_EMPTY_DIR = -8,
    /** Internal error, on lock for example */
    IBS__INTERNAL_ERROR = -9,
    /** Code returned when a key is already in the database  */
    IBS__KEY_ALREADY_ADDED = -10,
    /** Code returned when transaction does not exist */
    IBS__INVALID_TRANSACTION_ID = -11,
    /** Code returned when an error occurs while atomic write (disk full ...) */
    IBS__DUMP_ERROR = -12,
    /** Unexpected error */
    IBS__UNKNOW_ERROR = -99,
    /** Error code reserved for Java calls */
    RESERVED_JAVA = -100
};

/**
 * @brief Creates an IBS instance. Blocking, thread unsafe.
 * @param fname The configuration filename (absolute path).
 * @return The IBS instance id (>0) if successful else an ibsErrorCode
 */
int ibsCreate(const char* fname);

/**
 * @brief Initializes an IBS instance. Blocking, thread unsafe.
 * @param fname The configuration filename (absolute path).
 * @return The IBS instance id (>0) if successful else an ibsErrorCode
 */
int ibsInit(const char* fname);

/**
 * @brief Start an IBS instance. Blocking & thread unsafe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return 0 if successful else an ibsErrorCode
 */
int ibsStart(const int id);

/**
 * @brief Stop an IBS instance. Blocking & thread unsafe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsStop(const int id);

/**
 * @brief Delete an IBS instance (free memory, leave on-disk data). Blocking & thread unsafe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsDelete(const int id);

/**
 * @brief Destroy an IBS instance. Wipe all on-disk data.  Blocking & thread unsafe.
 * WARNING: ibsDelete must be called.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsDestroy(const int id);

/**
 * @brief Getter for the hot data status
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return 1 if hot data are activated else 0. If the IBS instance is invalid, it returns -1
 */
int ibsHotDataEnabled(const int id);

/**
 * @brief Fetch a record an IBS instance. Blocking & thread safe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param key The key associated to the record. Raw buffer.
 * @param keyLength The size of the data in the key buffer.
 * @param data The record will be store in data. The memory must be allocated. Raw buffer.
 * @param dataMaxLength The maximum size that could fit inside data.
 * @param dataLength The length of the record fetched is written here. If dataLength > dataMaxLength, dataLength make
 * the caller aware of the required buffer size and a bufferTooSmall is returned to the caller.
 * @return The length of data written inside data. An ibsErrorCode if something went wrong.
 */
int ibsGet(const int id, const char* key, const size_t keyLength, char* data, const size_t dataMaxLength,
        size_t* dataLength);

/**
 * @brief Delete a record an IBS instance. Blocking & thread safe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param key The key associated to the record. Raw buffer.
 * @return 0 if success, an ibsErrorCode if something went wrong.
 */
int ibsDel(const int id, const char* key, const size_t keyLength);

/**
 * @brief Save a record associated to a key. Blocking & thread safe.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param key The key associated to the record. Raw buffer.
 * @param keyLength The size of the data in the key buffer.
 * @param data The record that will be store. Raw buffer.
 * @param dataLength The size that will be save from data.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsPut(const int id, const char* key, const size_t keyLength, const char* data, const size_t dataLength);

/**
 * @brief Issue a replace request.
 *
 * A replace request is an enriched put request that warns the underlying storage that a record has been replaced by a
 * new one. The main purpose of this request is to properly handle short-lived records.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param oldKey The oldKey is the key associated to the the record that is being (on client side) overwritten by the
 * new one. Raw buffer.
 * @param oldKeyLength The size of the data in the oldKey buffer.
 * @param newKey The new key associated to the new record. Raw buffer.
 * @param newKeyLength The size of the data in the newKey buffer.
 * @param data The record that will be store. Raw buffer.
 * @param dataLength The size that will be save from data.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsReplace(const int id, const char* oldKey, const size_t oldKeyLength, const char* newKey,
        const size_t newKeyLength, const char* newData, const int newDataLength);

/**
 * @brief Create a transaction.
 *
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @return The IBS transaction id (>0) if successful else an ibsErrorCode.
 */
int ibsCreateTransaction(const int id);

/**
 * @brief Commit a transaction.
 *
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param transactionId The transaction id.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsCommitTransaction(const int id, const int transactionId);

/**
 * @brief Rollback a transaction.
 *
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param transactionId The transaction id.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsRollbackTransaction(const int id, const int transactionId);

/**
 * @brief Save a record associated to a key. Blocking & thread safe.
 * This version is for use in a transaction.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param transactionId The transaction id.
 * @param key The key associated to the record. Raw buffer.
 * @param keyLength The size of the data in the key buffer.
 * @param data The record that will be store. Raw buffer.
 * @param dataLength The size that will be save from data.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsPutTransaction(const int id, const int transactionId, const char* key, const size_t keyLength, const char* data,
        const size_t dataLength);

/**
 * @brief Issue a replace request.
 *
 * A replace request is an enriched put request that warns the underlying storage that a record has been replaced by a
 * new one. The main purpose of this request is to properly handle short-lived records.
 * This version is for use in a transaction.
 * @param id The IBS id returned by <code>ibsInit(char* fname)</code>.
 * @param transactionId The transaction id.
 * @param oldKey The oldKey is the key associated to the the record that is being (on client side) overwritten by the
 * new one. Raw buffer.
 * @param oldKeyLength The size of the data in the oldKey buffer.
 * @param newKey The new key associated to the new record. Raw buffer.
 * @param newKeyLength The size of the data in the newKey buffer.
 * @param data The record that will be store. Raw buffer.
 * @param dataLength The size that will be save from data.
 * @return 0 if successful else an ibsErrorCode.
 */
int ibsReplaceTransaction(const int id, const int transactionId, const char* oldKey, const size_t oldKeyLength,
        const char* newKey, const size_t newKeyLength, const char* newData, const int newDataLength);

#ifdef __cplusplus
}
#endif

#endif /* LIBIBS_H_ */
