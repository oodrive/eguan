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
 * @file ConcurrentFIFO.h
 * @brief Concurrent FIFO implementation/header
 * @author j. caba
 */
#ifndef CONCURRENTFIFO_H_
#define CONCURRENTFIFO_H_

#include <iostream>
#include <deque>
#include <utility>
#include <vector>
#include <memory>
#include <mutex>
#include <sstream>
#include <unistd.h>
#include "Locks.h"

namespace ibs {

/**
 * @brief Defines an generic FIFO container for concurrency,
 * providing thread-safety to access container or iterate over it.
 */
template<typename T>
class ConcurrentFIFO {
    public:
        ConcurrentFIFO() :
                _container(), _lock() {
            reborn();
        }

        virtual ~ConcurrentFIFO() {
            reborn();
        }

        void removeOldest() {
            _lock.writeLock();
            if (!_container.empty()) {
                _container.front().reset();
                _container.pop_front();
            }
            _lock.unlock();
        }

        void insertNewest(T* val) {
            _lock.writeLock();
            _container.emplace_back(std::shared_ptr<T>(val));
            _lock.unlock();
        }

        bool isEmpty() {
            _lock.readLock();
            bool res = _container.empty();
            _lock.unlock();
            return res;
        }

        /**
         * @brief utility method to avoid reallocating
         * only "reborn" the object as if it was just created
         */
        void reborn() {
            _lock.writeLock();
            while (!_container.empty()) {
                _container.front().reset();
                _container.pop_front();
            }
            _container.clear();
            _lock.unlock();
        }

        std::weak_ptr<T> getOldest() {
            if (isEmpty()) {
                return std::weak_ptr<T>();
            }
            _lock.readLock();
            std::weak_ptr<T> res = _container.front();
            _lock.unlock();
            return res;
        }

        std::weak_ptr<T> getNewest() {
            if (isEmpty()) {
                return std::weak_ptr<T>();
            }
            _lock.readLock();
            std::weak_ptr<T> res = _container.back();
            _lock.unlock();
            return res;
        }

        size_t count() {
            _lock.readLock();
            size_t res = _container.size();
            _lock.unlock();
            return res;
        }

        /**
         * @brief Convert to a vector ordered from newest to oldest for iteration
         * Main advantage is to iterate over all items without taking any lock.
         * A read lock is taken only while converting to ensure iterator are valid
         * in this method.
         * All the caller has to do is iterate over without taking any lock,
         * avoiding waits if the operation to do in each iteration is costly.
         */
        std::vector<std::weak_ptr<T> > toVector() {
            std::vector<std::weak_ptr<T> > res;
            res.reserve(_container.size());
            _lock.readLock();
            for (auto it = _container.rbegin(); it != _container.rend(); ++it) {
                res.emplace_back(*it);
            }
            _lock.unlock();
            return res;
        }

    protected:
        /**
         * @brief container owning the allocated memory
         */
        std::deque<std::shared_ptr<T>> _container;

        /**
         * @brief Multiple read/Single writer concurrency protection
         */
        PosixRWLock _lock;

    private:
        // non copyable
        ConcurrentFIFO(const ConcurrentFIFO&) = delete;
        ConcurrentFIFO& operator=(const ConcurrentFIFO&) = delete;
};

} /* namespace ibs */

#endif /* CONCURRENTFIFO_H_ */
