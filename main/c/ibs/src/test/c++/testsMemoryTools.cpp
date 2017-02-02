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
 * @file testsMemoryTools.cpp
 * @brief Memory Tools for unit/regression testings/source
 * @author j. caba
 */
#include "testsMemoryTools.h"
#include "testsTools.h"
#include <execinfo.h>

using namespace std;
using namespace ibs;

static int_fast64_t allocation = 0;

void resetAllocationCounter() {
    allocation = 0;
}

int_fast64_t getAllocationCount() {
    return allocation;
}

static size_t allocatedBytes = 0;

extern size_t getAllocatedBytes() {
    return allocatedBytes;
}

const int beginHeader = 0xABABABA;
const int beginBlock = 0xF0F0F0F;
const int endBlock = 0x0F0F0F0;

// global overloading of new to count number of new
// allocated bytes and detect overflow/memory corruption
static void* innerNew(size_t size) {
    ++allocation;
    size_t totalSize = size + sizeof(int) * 3 + sizeof(size_t);
    void* p = malloc(size + sizeof(int) * 3 + sizeof(size_t));
    if (p == NULL) {
        // did malloc succeed?
        throw std::bad_alloc(); // ANSI/ISO compliant behavior
    }
    else {
        bzero(p, totalSize);
        int* header = (int*) p;
        size_t* saveAllocated = (size_t*) ((char*) p + sizeof(int));
        int* begin = (int*) ((char*) p + sizeof(int) + sizeof(size_t));
        void* realPointer = (void*) ((char*) p + sizeof(int) * 2 + sizeof(size_t));
        int* end = (int*) ((char*) p + size + sizeof(int) * 2 + sizeof(size_t));

        *header = beginHeader;
        *saveAllocated = size;
        *begin = beginBlock;
        *end = endBlock;

        p = realPointer;
        allocatedBytes += size;
    }
    return p;
}

void* operator new(size_t size) {
    return innerNew(size);
}

void* operator new[](size_t size) {
    return innerNew(size);
}

// global overloading of delete to count number of delete
static void innerDelete(void* p) throw () {
    // also count deletion of NULL pointer because it's an hint of potential wrong memory handling,
    // and a potential performance amelioration by removing the calls to delete of a NULL pointer ...
    --allocation;

    if (p == NULL) // standard permit deletion of NULL pointer
        return;

    void* realPointer = (void*) ((char*) p - sizeof(int) * 2 - sizeof(size_t));

    int* header = (int*) realPointer;
    int* begin = (int*) ((char*) realPointer + sizeof(int) + sizeof(size_t));
    size_t* saveAllocated = (size_t*) ((char*) realPointer + sizeof(int));

    bool hasError = false;
    int check = 0;

    if (p != NULL) {
        check = *header;
    }
    else {
        hasError = false; // should not be here (detected by early check)
    }

    if (!hasError && (check != beginHeader)) {
        cerr << "Wrong block header" << endl;
        hasError = true;
    }

    if (!hasError && (*begin != beginBlock)) {
        cerr << "Wrong block begin" << endl;
        hasError = true;
    }

    size_t size = 0;
    if (!hasError) {
        size = *saveAllocated;
        int* end = (int*) ((char*) realPointer + size + sizeof(int) * 2 + sizeof(size_t));

        if (*end != endBlock) {
            cerr << "Wrong block end" << endl;
            hasError = true;
        }
    }

    if (hasError) {
        cerr << "Detected memory corruption." << endl;
        return;
    }

    char* toFree = (char*) realPointer;
    if (!hasError && size != 0) {
        size_t totalSize = size + sizeof(int) * 3 + sizeof(size_t);
        bzero(realPointer, totalSize);
    }
    free(toFree);
    allocatedBytes -= size;
}

void operator delete(void *p) throw () {
    innerDelete(p);
}

void operator delete[](void *p) throw () {
    innerDelete(p);
}

