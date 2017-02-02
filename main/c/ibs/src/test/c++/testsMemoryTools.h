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
 * @file testsMemoryTools.h
 * @brief Memory Tools for unit/regression testings/header
 * @author jcaba
 */
#ifndef TESTSMEMORYTOOLS_H_
#define TESTSMEMORYTOOLS_H_

#include <new>
#include <cstddef>
#include <cstdint>

// very basic memory leak checker
// to detect regression in memory management
// count allocation ie: number of new/delete
// count number of bytes allocated,
// allocated memory is a little bigger to add markers
// at begin and end of allocated blocks
// and saving size at the beginning of the blocks

extern void resetAllocationCounter(); // reset the counter of new/delete
extern int_fast64_t getAllocationCount(); // count number of new/delete

extern size_t getAllocatedBytes(); // get the number of bytes dynamically allocated

extern void* operator new(size_t size);
extern void* operator new[](size_t size);
extern void operator delete(void* ptr) throw ();
extern void operator delete[](void* ptr) throw ();

#endif /* TESTSMEMORYTOOLS_H_ */
