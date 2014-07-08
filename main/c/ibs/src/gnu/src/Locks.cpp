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
 * @file Locks.cpp
 * @brief Different locks implementation wrapper for the Ibs/source
 * @author j. caba
 */
#include "Locks.h"

namespace ibs {

Lock::~Lock() = default;

SharedExclusiveLock::~SharedExclusiveLock() = default;

PosixRWLock::PosixRWLock() :
        SharedExclusiveLock(), impl(PTHREAD_RWLOCK_INITIALIZER) {
}

PosixRWLock::~PosixRWLock() {
    unlock();
}

void PosixRWLock::readLock() {
    pthread_rwlock_rdlock(&impl);
}

void PosixRWLock::writeLock() {
    pthread_rwlock_wrlock(&impl);
}

void PosixRWLock::unlock() {
    pthread_rwlock_unlock(&impl);
}

StdMutex::~StdMutex() {
    unlock();
}

void StdMutex::lock() {
    impl.lock();
}

void StdMutex::unlock() {
    impl.unlock();
}

PosixMutex::PosixMutex() :
        Lock(), impl(PTHREAD_MUTEX_INITIALIZER) {
}

PosixMutex::~PosixMutex() {
    unlock();
}

bool PosixMutex::trylock() {
    return pthread_mutex_trylock(&impl) == 0;
}

void PosixMutex::lock() {
    pthread_mutex_lock(&impl);
}

void PosixMutex::unlock() {
    pthread_mutex_unlock(&impl);
}

PosixSpinLock::PosixSpinLock() :
        Lock() {
    pthread_spin_init(&impl, 0);
}

PosixSpinLock::~PosixSpinLock() {
    unlock();
}

void PosixSpinLock::lock() {
    pthread_spin_lock(&impl);
}

void PosixSpinLock::unlock() {
    pthread_spin_unlock(&impl);
}

} /* namespace ibs */
