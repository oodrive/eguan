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
 * @file RefCountedDbFile.h
 * @brief The reference counted db file /header
 * @author j. caba
 */
#ifndef REFCOUNTEDDBFILE_H_
#define REFCOUNTEDDBFILE_H_

#include "FileTools.h"
#include "DataChunk.h"
#include "Locks.h"
#include <iostream>

namespace ibs {

/**
 * @brief This class implements the file format of the Reference Counted Db.
 *
 * A RefCountedDbFile is always open from creation to destruction.
 * It's open in "append" mode and data are "flushed" on the fly ...
 *
 *
 * Description of format :
 *
 * File is composed of a header followed by the key/data/crc tuples
 *
 * <Header>
 * <Key><Data><SPACE RESERVED FOR CRC>
 * <Key><Data><SPACE RESERVED FOR CRC>
 * ...
 *
 * the header is compose of 5 magic characters "REFDB" + version + creation time
 * |  Magic    | Version # | CREATION TIME |
 * |---- 5 ----|---- 8 ----|----   8   ----|
 *
 * The version number describe the version of the format.
 *
 * In the version 1 the CRC space is empty and not use yet.
 *
 * @see RefCountedDb
 */
class RefCountedDbFile: public BinaryFileBase {
    public:
        /**
         * @brief Constructor opening or creating a new file
         * NOTE: this method is protected against data race by an internal lock.
         */
        RefCountedDbFile(const std::string& fileToSave) :
                filename(fileToSave), writeLock(), writeStream(filename, nominalMode) {
            writeLock.lock();
            setHeader();
            writeLock.unlock();
        }

        /**
         * @brief Destructor closing the file
         * NOTE: this method is protected against data race by an internal lock.
         */
        virtual ~RefCountedDbFile() {
            writeLock.lock();
            writeStream.close();
            writeLock.unlock();
        }

        /**
         * @brief Append new Key/Value pair
         * @return The offset in the file where the data is saved is stored.
         * NOTE: this method is protected against data race by an internal lock.
         */
        std::ios::pos_type appendNewKeyData(const DataChunk& key, const DataChunk& data) {
            writeLock.lock();
            updateString(writeStream, key.toString());
            std::ios::pos_type whereToStoreRefOffset = writeStream.tellp();
            updateString(writeStream, data.toString()); // save data
            updateUint64(writeStream, 0); // reserve space for possible CRC.
            writeStream.flush();
            writeLock.unlock();
            return whereToStoreRefOffset;
        }

        /**
         * @brief Recover data associated to a key reference counter.
         * @param an offset where the data is saved is stored.
         */
        void getData(std::ios::pos_type pos, std::string& output) {
            // each thread can have it's own read stream
            std::fstream readStream(filename, readOnlymode);
            readStream.seekp(pos);
            fetchString(readStream, output);
            readStream.close();
        }

        /**
         * @brief Check a file has valid header
         * @param filename
         * i.e: with valid header
         * @return
         */
        static bool hasValidHeader(const std::string& filename);

    protected:

        /**
         * @brief Write header
         */
        void setHeader();

    private:
        const std::string filename; /** where to save the file */
        PosixMutex writeLock; /** protect against multiple concurrent write */
        std::fstream writeStream; /** binary stream used to write data in the file */

        static const u_short magicSize = 5; /** magic number size */
        static const u_char magic[magicSize]; /** magic number */
        static constexpr uint64_t currentVersion = 1; /** version number */
        static constexpr std::ios_base::openmode nominalMode = std::ios::binary | std::ios::app | std::ios::out
                | std::ios::in;
        static constexpr std::ios_base::openmode readOnlymode = std::ios::binary | std::ios::in;
};

} /* namespace ibs */

#endif /* REFCOUNTEDDBFILE_H_ */
