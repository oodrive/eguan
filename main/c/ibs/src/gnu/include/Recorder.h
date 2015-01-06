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
 * @file Recorder.h
 * @brief The IBS recorder/header
 * @author j. caba
 */
#ifndef RECORDER_H_
#define RECORDER_H_

#include "AbstractController.h"
#include "FileTools.h"
#include "Locks.h"
#include "Logger.h"
#include "ConfigFileReader.h"
#include "Constants.h"

namespace ibs {

/**
 * @brief RecorderFile class.
 *
 * This class implements the file format of the recorder.
 *
 * Structure of files, indicated size are in bytes
 * |  Magic #  | entries # | RECORD BEGIN OFFSET |   IBS config   |    record1     | ...
 * |---- 4 ----|---- 8 ----|----      8      ----| ...variable... | ...variable... | ...
 *
 * RECORD BEGIN OFFSET is the offset in the file where the record "list" begins
 *
 * Structure of IBS config :
 * |    HOT_DATA    |  IBP_NB   |
 * | ...variable... |---- 8 ----|
 * | BUFFER_ROTATION_THRESHOLD | BUFFER_ROTATION_DELAY |
 * |----         8         ----|----       8       ----|
 * | BUFFER_COMPRESSION |   COMPRESSION  |
 * | ...  variable  ... | ...variable... |
 * | BUFFER_WRITE_DELAY_THRESHOLD | BUFFER_WRITE_DELAY_LEVEL_SIZE | BUFFER_WRITE_DELAY_INCR_MS |
 * |----           8          ----|----           8           ----|----          8         ----|
 * | LDB_BLOCK_SIZE | LDB_BLOCK_RESTART_INVERVAL | LDB_WRITE_BUFFER_SIZE |
 * |----    8   ----|----          8         ----|----      8        ----|
 *
 * Structure of a record :
 * |  OPCODE   |    raw data    |
 * |---- 8 ----| ...variable... |
 *
 * OPCODE can be one of the primitive operation
 * DROP=0 with the following raw data structure
 * |       KEY      |   RESULT  |
 * | ...variable... |---- 8 ----|
 *
 * PUT=1 with the following raw data structure
 * |       KEY      |   RESULT  |
 * | ...variable... |---- 8 ----|
 *
 * REPLACE=2 with the following raw data structure
 * |     OLDKEY     |       KEY      |   RESULT  |
 * | ...variable... | ...variable... |---- 8 ----|
 * or OPCODE can be a transaction operation
 *
 * CREATE_TRANSACTION=3 with the following raw data structure
 * |     ID    |
 * |---- 8 ----|
 *
 * COMMIT_TRANSACTION=4 with the following raw data structure
 * |     ID    |   RESULT  |
 * |---- 8 ----| --- 8 ----|
 *
 * ROLLBACK_TRANSACTION=5 with the following raw data structure
 * |     ID    |
 * |---- 8 ----|
 *
 * PUT_TRANSACTION=6 with the following raw data structure
 * |     ID    |       KEY      |
 * |---- 8 ----| ...variable... |
 *
 * REPLACE_TRANSACTION=7 with the following raw data structure
 * |     ID    |     OLDKEY     |       KEY      |
 * |---- 8 ----| ...variable... | ...variable... |
 *
 * START_IBS=8 with the following raw data structure
 * |   RESULT  |
 * |---- 8 ----|
 *
 * STOP_IBS=9 with the following raw data structure
 * |   RESULT  |
 * |---- 8 ----|
 *
 * GET=10 with the following raw data structure
 * |       KEY      |   RESULT  |
 * | ...variable... |---- 8 ----|
 *
 * FETCH=11 with the following raw data structure
 * |       KEY      |  EXPECTED |   RESULT  |
 * | ...variable... |---- 8 ----|---- 8 ----|
 *
 * Structure of key/string
 * |  Length   |    raw data    |
 * |---- 8 ----| ...variable... |
 */
class RecorderFile: protected BinaryFileBase {
    public:

        enum OPCODE {
            DROP = 0,
            PUT = 1,
            REPLACE = 2,
            CREATE_TRANSACTION = 3,
            COMMIT_TRANSACTION = 4,
            ROLLBACK_TRANSACTION = 5,
            PUT_TRANSACTION = 6,
            REPLACE_TRANSACTION = 7,
            START_IBS = 8,
            STOP_IBS = 9,
            GET = 10,
            FETCH = 11
        };

        RecorderFile(const std::string& fileToSave, const ConfigFileReader* ibsCfg);

        virtual ~RecorderFile();

        void lockFile();

        void unlockFile();

        const std::string& getFilename() const;

        void appendResult(const StatusCode& result);

        void appendIntResult(const int result);

        void appendExpected(const size_t expected);

        void appendDrop(const DataChunk& key);

        void appendPut(const DataChunk& key);

        void appendReplace(const DataChunk& oldkey, const DataChunk& key);

        void appendCreateTransactionOpcode();

        void appendCreateTransactionId(const int id);

        void appendCommitTransaction(const int id);

        void appendRollbackTransaction(const int id);

        void appendPutTransaction(const int id, const DataChunk& key);

        void appendReplaceTransaction(const int id, const DataChunk& oldkey, const DataChunk& key);

        void appendGet(const DataChunk& key);

        void appendFetch(const DataChunk& key);

        void appendStart();

        void appendStop();

    protected:
        void updateNbEntries();

        void setHeader(std::ofstream& f);

        void appendDataChunk(const DataChunk& datachunk);

        void appendId(const int id);

        void appendOpcode(const enum OPCODE opcode);

        static void setOpcode(std::ofstream& f, const enum OPCODE opcode);

        static void setResult(std::ofstream& f, const StatusCode& result);

        static void setId(std::ofstream& f, int id);

        static void setDataChunk(std::ofstream& f, const DataChunk& datachunk);
    protected:
        // non copyable
        RecorderFile(const RecorderFile&) = delete;
        RecorderFile& operator=(const RecorderFile&) = delete;

    private:
        std::string filename; /** where to save the file */
        const ConfigFileReader* cfg; /** ibs config to save file  */
        uint64_t nbEntries; /** number of entries in the file */
        std::ios::pos_type updateEntriesPos; /** position in the file for updating # entries */
        PosixMutex fileLock; /** protect concurrent threads access to record file */

        static const u_short magicSize = 4; /** magic number size */
        static const u_char magic[magicSize]; /** magic number */
        static constexpr std::ios_base::openmode append = std::ios::binary | std::ios::app;
        static Logger_t logger;
};

/**
 * @brief Recorder "controller" class.
 *
 * This class implements a recorder of the action the real Controller.
 */
class Recorder: public AbstractController {
    protected:
        Recorder() = default;

        // non copyable
        Recorder(const Recorder&) = delete;
        Recorder& operator=(const Recorder&) = delete;
    public:
        /**
         * @brief Default destructor.
         */
        virtual ~Recorder();

        /**
         * @brief Recorder Factory for a new IBS
         * @param fname The configuration filename.
         * @param po_controller A controller pointer if success, NULL otherwise
         * @return a Status object to track what happens
         */
        static StatusCode create(const std::string& fname, AbstractController*& po_controller);

        /**
         * @brief Recorder Factory for initializing an existing IBS
         * @param fname The configuration filename.
         * @param po_controller A controller pointer if success, NULL otherwise
         * @return a Status object to track what happens
         */
        static StatusCode init(const std::string& fname, AbstractController*& po_controller);

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
         * This method save a record associated to an abstract context ID. This context ID is optional (default to zero
         * for both part of the ID.
         *
         * @remarks Blocking & thread safe.
         * @param key A string that must be kept to be able to get back the value. The key could be seen as buffer
         * stored in <code>key.data()</code> with a length of <code>key.size()</code>. So the key does not have to be
         * a <code>\0</code> terminated string.
         * @param value The value the caller wants to save. Save bytes from <code>value.data()</code> with a length
         * <code>value.size()</code>.
         * @param id1 Abstract context ID, first part.
         * @param id2 Abstract context ID, first part.
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode put(const DataChunk&& key, const DataChunk&& value);

        /**
         * @brief Handles a <i>GET</i> request.
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @param value Where to store the value.
         * @param expected Expected size in case value is too small
         * @return An <code>ibs::Status</code> reflecting what happened during request.
         */
        virtual StatusCode fetch(const DataChunk&& key, DataChunk&& value, size_t& expected);

        /**
         * @brief Handles a <i>DEL</i> request
         *
         * This method delete a record associated to a key.
         *
         * @remarks Blocking & thread safe.
         * @param key The key used to initially store a value.
         * @return An <code>ibs::Status</code> reflecting what happened during request.
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
         * @return An <code>ibs::Status</code> reflecting what happened during request.
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
        bool destroy();

        /**
         * @brief Getter for the hot data status
         * @return true if hot data are activated else false
         */
        bool hotDataEnabled();

        /**
         * @brief Allocate and initialize recorder file
         * @param filename Record file for saving Ibs operations
         * @param cfg Ibs configuration to save
         */
        void allocateRecordFile(const std::string& filename, const ConfigFileReader* cfg);
    protected:
        /**
         * @brief Allocate a recorder
         */
        static void allocateRecorder(AbstractController*& po_controller, AbstractController* ctrl) noexcept;
    private:
        std::unique_ptr<RecorderFile> recordFile; /** file where the calls are stored */
        std::unique_ptr<AbstractController> realController; /** Real controller */
};

} /* namespace ibs */

#endif /* RECORDER_H_ */

