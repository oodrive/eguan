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
 * @file DataChunk.h
 * @brief DataChunk implementation/header
 * @author j. caba
 */
#ifndef DATACHUNK_H_
#define DATACHUNK_H_

#include <string.h>
#include <assert.h>
#include <cstddef>
#include <string>
#include <atomic>

namespace ibs {

/**
 * @brief Utility class used to reference a buffer of data and be aware about memory copy.
 *
 * A data chunk represent a set of byte contiguous in memory.
 * The DataChunk class is designed to avoid copy
 * thus it's only natural a data chunk is not copyable itself.
 * It can be seen as a pointer to an address in memory
 * and a number of bytes to fetch from this memory.
 * This bytes can be compressed and uncompressed and returned as std::string.
 * Methods are thread safe using atomic operations on memory.
 */
class DataChunk {
    public:
        /**
         * @brief Create an empty DataChunk.
         */
        DataChunk();

        /**
         * @brief Create a DataChunk that refers to d[0,n-1].
         */
        DataChunk(const char* d, size_t n);

        /**
         * @brief Create a DataChunk that refers to d[0,strlen(d)-1]. 
         */
        DataChunk(const char* d);

        /**
         * @brief Create a slice that refers to the contents of "s"
         */
        DataChunk(const std::string& s);

        /**
         * @brief Move constructor for effective call from methods
         */
        DataChunk(const DataChunk&& b);

        /**
         * @brief Conversion to a std::string
         *
         * Return a string that contains the copy of the referenced data.
         */
        std::string toString() const noexcept;

        /**
         * @brief Is the data chunk referencing the same pointer passed as parameter ?
         */
        bool isReferencing(const char* d) const noexcept;

        /**
         * @brief Equality operator
         */
        bool operator==(const DataChunk& y) const noexcept;

        /**
         * @brief Get n-th byte in the chunk
         */
        char operator[](size_t n) const noexcept;

        /**
         * @brief Get n-th int in the chunk
         */
        int toInt(size_t n = 0) const;

        /**
         * @brief Change the DataChunk to refer to new data
         */
        void reference(const char* d) noexcept;

        /**
         * @brief Change the DataChunk to refer to new data
         */
        void reference(const DataChunk& d) noexcept;

        /**
         * @brief Compress the data referenced.
         * @see toUncompressedString
         */
        std::string toCompressedString() const;

        /**
         * @brief Uncompress the data referenced.
         * @see toCompressedString
         */
        std::string toUncompressedString() const;

        /**
         * @brief Try to copy data from input in the referenced buffer.
         * @return True if could contain input data, false otherwise.
         * @see toUncompressedString
         */
        bool copyFrom(const std::string& input);

        /**
         * @brief Calculate a hash using One at a time algorithm for table lookup
         * N.B: Used for recorder
         */
        uint32_t hash() const;

        /**
         * @brief Return the number of bytes of the data referenced.
         */
        size_t getSize() const noexcept;

        /**
         * @brief Return a pointer to the data referenced.
         */
        const char* getData() const noexcept;

    protected:
        void setData(const char* d, size_t n) noexcept;
        void setSize(size_t n) noexcept;
        void acquire();
        void release();
    private:
        std::atomic<const char*> internData;
        std::atomic<size_t> length;
        static std::atomic_flag lock;

        // non copyable, only movable
        DataChunk(const DataChunk&) = delete;
        DataChunk& operator=(const DataChunk&) = delete;
};

}
/* namespace ibs */

#endif /* DATACHUNK_H_ */
