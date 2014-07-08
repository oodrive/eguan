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
 * @file CreationTask.h
 * @brief The creation task API/header
 * @author j. caba
 */
#ifndef CREATIONTASK_H_
#define CREATIONTASK_H_

#include "AbstractTask.h"
#include "Locks.h"
#include <iostream>

namespace ibs {

class IbpGenHandler;

/**
 * @brief Hold information to decide when to create a new ibpgen database
 *
 * This class is thread safe.
 */
class CreationConditions {
    public:
        CreationConditions() :
                needCreation(false), sharedLock(), hotDataWaitTimeSpent(0), writeOps(0) {
        }

        /**
         * @brief Updates the number of write operations done
         * @return The number of operation after the update.
         */
        inline uint64_t updateWriteOps();

        /**
         * @brief Updates the time spent in seconds watching data.
         * @param The duration in seconds to add to the time spent
         * @return The time in seconds spent after the update.
         */
        inline uint64_t updateTimeSpent(uint64_t sec);

        /**
         * @brief Indicate a request fore creation is pending
         */
        inline bool isCreationPending();

        /**
         * @brief Request a buffer creation.
         */
        inline void requestCreation();

        /**
         * @Reset the counters
         */
        inline void reset();

    private:
        bool needCreation; /** request for creation */
        PosixRWLock sharedLock; /** protect attributes from multiple thread writes */
        uint64_t hotDataWaitTimeSpent; /** Time spent watching hotdatas in second */
        uint64_t writeOps; /** Write operations done */
};

inline uint64_t CreationConditions::updateWriteOps() {
    uint64_t writes;
    sharedLock.writeLock();
    ++writeOps;
    writes = writeOps;
    sharedLock.unlock();
    return writes;
}

inline uint64_t CreationConditions::updateTimeSpent(uint64_t sec) {
    uint64_t spent;
    sharedLock.writeLock();
    hotDataWaitTimeSpent += sec;
    spent = hotDataWaitTimeSpent;
    sharedLock.unlock();
    return spent;
}

inline bool CreationConditions::isCreationPending() {
    sharedLock.readLock();
    bool res = needCreation;
    sharedLock.unlock();
    return res;
}

inline void CreationConditions::requestCreation() {
    sharedLock.writeLock();
    needCreation = true;
    sharedLock.unlock();
}

inline void CreationConditions::reset() {
    sharedLock.writeLock();
    writeOps = 0;
    hotDataWaitTimeSpent = 0;
    needCreation = false;
    sharedLock.unlock();
}

/**
 * @brief Defines the implementation for the task creating database in ibpgen.
 * i.e: Creation of the newest ibpgen database.
 */
class CreationTask: public AbstractTask<IbpGenHandler> {
    public:
        CreationTask(IbpGenHandler& ipbGen) :
                AbstractTask(ipbGen), creationProtection(), conditions(), newestIndex(0) {
        }

        virtual void createThread() {
            task = std::move(std::thread(CreateLoop, std::ref(*this)));
        }

    protected:
        /**
         * @brief To print stats in debug log mode or above
         */
        void printStats(IbpGenHandler& toDescribe);

        /**
         * @brief Create a new buffer.
         */
        bool createNext(IbpGenHandler& whereToCreate) noexcept;

        static void CreateLoop(CreationTask& self) noexcept;

    private:
        /** to ensure creation of buffer is atomic ... */
        PosixSpinLock creationProtection;

        CreationConditions conditions;/** creation condition */

        uint64_t newestIndex; // current index for naming of db ...

        friend class IbpGenHandler;
};

} /* namespace ibs */
#endif /* CREATIONTASK_H_ */
