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
 * @brief DataChunk implementation/source
 * @author j. caba
 */
#include "DataChunk.h"
#include <snappy.h>

namespace ibs {

std::atomic_flag DataChunk::lock = ATOMIC_FLAG_INIT;

DataChunk::DataChunk() :
        internData(NULL), length(0) {
}

DataChunk::DataChunk(const char* d, size_t n) :
        internData(NULL), length(0) {
    setData(d, n);
}

DataChunk::DataChunk(const char* d) :
        internData(NULL), length(0) {
    setData(d, strlen(d));

    assert(internData.is_lock_free());
    assert(length.is_lock_free());
}

DataChunk::DataChunk(const std::string& s) :
        internData(NULL), length(0) {
    setData(s.data(), s.size());
}

DataChunk::DataChunk(const DataChunk&& b) :
        internData(NULL), length(0) {
    setData(b.internData, b.length);
}

std::string DataChunk::toString() const noexcept {
    return std::string(getData(), getSize());
}

bool DataChunk::isReferencing(const char* d) const noexcept {
    return getData() == d;
}

bool DataChunk::operator==(const DataChunk& y) const noexcept {
    return ((getSize() == y.getSize())
            && ((isReferencing(y.getData())) || (memcmp(getData(), y.getData(), getSize()) == 0)));
}

char DataChunk::operator[](size_t n) const noexcept {
    assert(n < getSize());
    return *(getData() + n);
}

void DataChunk::reference(const char* d) noexcept {
    setData(d, strlen(d));
}

void DataChunk::reference(const DataChunk& d) noexcept {
    setData(d.internData, d.length);
}

uint32_t DataChunk::hash() const {
    // One at a time hash for table lookup from Bob Jenkins,
    // this hash function is under public domain
    uint32_t h = 0;

    DataChunk* self = const_cast<DataChunk*>(this);

    // lock write while reading buffer
    self->acquire();
    for (size_t i = 0; i < getSize(); i++) {
        h += (*this)[i];
        h += (h << 10);
        h ^= (h >> 6);
    }
    self->release();

    h += (h << 3);
    h ^= (h >> 11);
    h += (h << 15);

    return h;
}

int DataChunk::toInt(size_t n) const {
    DataChunk* self = const_cast<DataChunk*>(this);

    // let be this pointers :
    //    char* whereToEndCopy = const_cast<char*>(getData()) + n +  sizeof(int);
    //    char* whereBufferEnds = const_cast<char*>(getData()) + getSize();
    // the following proposition :
    //    whereToEndCopy <= whereBufferEnds is equivalent to
    //    n +  sizeof(int) <= getSize() that again is equivalent to
    //    n <= getSize() - sizeof(int)
    // thus the following assertion must pass
    // if the function is used properly with correct bounds :
    assert(n <= getSize() - sizeof(int));

    int ret;
    self->acquire();
    memcpy(&ret, getData() + n, sizeof(int));
    self->release();
    return ret;
}

std::string DataChunk::toCompressedString() const {
    std::string compressed;
    snappy::Compress(getData(), getSize(), &compressed);
    return compressed;
}

std::string DataChunk::toUncompressedString() const {
    std::string uncompressed;
    snappy::Uncompress(getData(), getSize(), &uncompressed);
    return uncompressed;
}

bool DataChunk::copyFrom(const std::string& input) {
    if (input.size() > getSize()) {
        return false;
    }
    else {
        acquire();
        memcpy(const_cast<char*>(getData()), input.data(), input.size());
        setSize(input.size()); //size also need to change
        release();
        return true;
    }
}

size_t DataChunk::getSize() const noexcept {
    return length.load(std::memory_order_relaxed);
}

const char* DataChunk::getData() const noexcept {
    return internData.load(std::memory_order_relaxed);
}

void DataChunk::setData(const char* d, size_t n) noexcept {
    acquire();
    internData.store(d, std::memory_order_relaxed);
    setSize(n);
    release();
}

void DataChunk::setSize(size_t n) noexcept {
    length.store(n, std::memory_order_relaxed);
}

void DataChunk::acquire() {
    while (std::atomic_flag_test_and_set_explicit(&lock, std::memory_order_acquire))
        ; // spin until the lock is acquired
}

void DataChunk::release() {
    std::atomic_flag_clear_explicit(&lock, std::memory_order_release);
}

} /* namespace ibs */
