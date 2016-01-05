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
 * @file Locks.h
 * @brief Different locks implementation wrapper for the Ibs/header
 * @author j. caba
 */
#ifndef LOCKS_H_
#define LOCKS_H_

#include <mutex>
#include <pthread.h>

namespace ibs {

/**
 * @brief Exclusive lock interface
 */
class Lock {
    public:
        /**
         * @brief Acquire the lock
         */
        virtual void lock() = 0;

        /**
         * @brief Release the lock
         */
        virtual void unlock() = 0;
    protected:
        Lock() = default;
        virtual ~Lock();

    private:
        // non copyable
        Lock(const Lock&) = delete;
        Lock& operator=(const Lock&) = delete;
};

/**
 * @brief Shared exclusive, multiple readers/single writer lock interface
 */
class SharedExclusiveLock {
    public:
        /**
         * @brief Acquire the lock in read mode
         */
        virtual void readLock() = 0;

        /**
         * @brief Acquire the lock in write mode
         */
        virtual void writeLock() = 0;

        /**
         * @brief Release the lock
         */
        virtual void unlock() = 0;
    protected:
        SharedExclusiveLock() = default;
        virtual ~SharedExclusiveLock();

    private:
        // non copyable
        SharedExclusiveLock(const SharedExclusiveLock&) = delete;
        SharedExclusiveLock& operator=(const SharedExclusiveLock&) = delete;
};

/**
 * @brief This is an implementation of a SharedExclusiveLock using Posix pthread_rwlock_t
 */
class PosixRWLock: public SharedExclusiveLock {
    public:
        /**
         * @brief default ctor
         */
        PosixRWLock();

        /**
         * @brief dtor
         */
        virtual ~PosixRWLock();

        /**
         * @brief Acquire the lock in read mode
         */
        virtual void readLock();

        /**
         * @brief Acquire the lock in write mode
         */
        virtual void writeLock();

        /**
         * @brief Release the lock
         */
        virtual void unlock();
    private:
        pthread_rwlock_t impl;
};

/**
 * @brief This is an implementation of a Lock using C++11 std::mutex
 */
class StdMutex: public Lock {
    public:
        /**
         * @brief default ctor
         */
        StdMutex() = default;

        /**
         * @brief dtor
         */
        virtual ~StdMutex();

        /**
         * @brief Lock the mutex
         */
        void lock();

        /**
         * @brief Unlock the mutex
         */
        void unlock();
    private:
        std::mutex impl;
};

/**
 * @brief This is an implementation of a Lock using Posix pthread_mutex_t
 */
class PosixMutex: public Lock {
    public:
        /**
         * @brief default ctor
         */
        PosixMutex();

        /**
         * @brief dtor
         */
        virtual ~PosixMutex();

        /**
         * @brief Try to lock the mutex
         * @return true if the lock is taken.
         */
        bool trylock();

        /**
         * @brief Lock the mutex
         */
        virtual void lock();

        /**
         * @brief Unlock the mutex
         */
        virtual void unlock();
    private:
        pthread_mutex_t impl;
};

/**
 * @brief This is an implementation of a Lock using Posix pthread_spinlock_t
 */
class PosixSpinLock: public Lock {
    public:
        /**
         * @brief default ctor
         */
        PosixSpinLock();

        /**
         * @brief dtor
         */
        virtual ~PosixSpinLock();

        /**
         * @brief Lock the mutex
         */
        void lock();

        /**
         * @brief Unlock the mutex
         */
        void unlock();
    private:
        pthread_spinlock_t impl;
};

} /* namespace ibs */
#endif /* LOCKS_H_ */
