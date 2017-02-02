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
 * @file testsLocks.h
 * @author j. caba
 */
#include "testsTools.h"
#include "Locks.h"
#include "Logger.h"
#include <thread>
#include <atomic>
#include <future>

namespace ibs {

namespace testLocks {

PosixRWLock posixRWLock;
StdMutex stdMutex;
PosixMutex posixMutex;
PosixSpinLock posixSpinLock;

const int nbTests = 4;
const int nbThreads = 50;
static std::promise<bool> promises[nbThreads][nbTests];
static std::future<bool> futures[nbThreads][nbTests];
static std::atomic<int> numberOfAccess;
// ensure only one test access the thread array at a time
static std::mutex threadsMutex;

static Logger_t logger = ibs::Logger::getLogger("testsIbsLocks");

bool SharedExclusiveLockExclusiveAccessTest(const int nbLoop, SharedExclusiveLock* lockImpl) {
    lockImpl->readLock();
    EXPECT_TRUE(numberOfAccess == 0);
    lockImpl->unlock();
    lockImpl->writeLock();
    for (int i = 0; i < nbLoop; i++) {
        numberOfAccess++;
    }
    bool ret = numberOfAccess == nbLoop;
    EXPECT_TRUE(ret);
    numberOfAccess = 0;
    EXPECT_TRUE(numberOfAccess == 0);
    lockImpl->unlock();
    return ret;
}

bool PosixRWLockExclusiveAccessTest(const int nbLoop) {
    return SharedExclusiveLockExclusiveAccessTest(nbLoop, &posixRWLock);
}

bool LockExclusiveAccessTest(const int nbLoop, Lock* lockImpl) {
    EXPECT_TRUE(lockImpl != NULL);
    lockImpl->lock();
    EXPECT_TRUE(numberOfAccess == 0);
    for (int i = 0; i < nbLoop; i++) {
        numberOfAccess++;
    }
    bool ret = numberOfAccess == nbLoop;
    EXPECT_TRUE(ret);
    numberOfAccess = 0;
    EXPECT_TRUE(numberOfAccess == 0);
    lockImpl->unlock();
    return ret;
}

bool StdMutexExclusiveAccessTest(const int nbLoop) {
    return LockExclusiveAccessTest(nbLoop, &stdMutex);
}

bool PosixMutexExclusiveAccessTest(const int nbLoop) {
    return LockExclusiveAccessTest(nbLoop, &posixMutex);
}

bool PosixSpinLockExclusiveAccessTest(const int nbLoop) {
    return LockExclusiveAccessTest(nbLoop, &posixSpinLock);
}

typedef bool (*func_t)(const int);

template<func_t func>
void detach_threads(const int test) {
    const int nbDataLoops = 100;
    numberOfAccess = 0;
    EXPECT_TRUE(numberOfAccess == 0);
    for (int i = 0; i < nbThreads; i++) {
        promises[i][test] = std::promise<bool>();
        futures[i][test] = promises[i][test].get_future();
        LOG4IBS_DEBUG(logger, "detach future[" << i << "][" << test << "]");
        std::thread([](std::promise<bool>& p) {p.set_value(func(nbDataLoops));}, std::ref(promises[i][test])).detach();
    }
}

void wait_futures(const int test) {
    for (int i = 0; i < nbThreads; i++) {
        LOG4IBS_DEBUG(logger, "waiting for future[" << i << "][" << test << "]");
        bool hasError = false;
        // The behavior is undefined if valid() == false
        if (futures[i][test].valid() != false) {
            futures[i][test].wait();
            EXPECT_TRUE(futures[i][test].valid());
            if (futures[i][test].valid() != false) {
                EXPECT_TRUE(futures[i][test].get());
            }
            else {
                hasError = true;
            }
        }
        else {
            hasError = true;
        }

        if (hasError) {
            LOG4IBS_ERROR(logger, "This unit test need a fix, should not be here. ");
            EXPECT_TRUE(false);
        }
    }
    EXPECT_TRUE(numberOfAccess == 0);
}

template<func_t func>
void shared_code_for_tests(const int test) {
    LOG4IBS_WARN(logger, "Waiting for critical section");
    threadsMutex.lock();
    LOG4IBS_WARN(logger, "Entering critical section");
    numberOfAccess = 0;
    detach_threads<func>(test);
    wait_futures(test);
    LOG4IBS_WARN(logger, "Leaving critical section");
    threadsMutex.unlock();
}

} // namespace testLocks

using namespace testLocks;

class testsLocks: public ::testing::Test {
    protected:

        testsLocks() {
        }

        virtual ~testsLocks() {
        }

        virtual void SetUp() {
            ::testing::FLAGS_gtest_death_test_style = "threadsafe";
        }

        virtual void TearDown() {
        }
};

TEST_F(testsLocks, TestPosixRWLock) {
    LOG4IBS_WARN(logger, "Launching test TestPosixRWLock");
    shared_code_for_tests<PosixRWLockExclusiveAccessTest>(0);
    LOG4IBS_WARN(logger, "End of test TestPosixRWLock");
}

TEST_F(testsLocks, TestStdMutex) {
    LOG4IBS_WARN(logger, "Launching test TestStdMutex");
    shared_code_for_tests<StdMutexExclusiveAccessTest>(1);
    LOG4IBS_WARN(logger, "End of test TestStdMutex");
}

TEST_F(testsLocks, TestPosixMutex) {
    LOG4IBS_WARN(logger, "Launching test TestPosixMutex");
    shared_code_for_tests<PosixMutexExclusiveAccessTest>(2);
    LOG4IBS_WARN(logger, "End of test TestPosixMutex");
}

TEST_F(testsLocks, TestPosixSpinLock) {
    LOG4IBS_WARN(logger, "Launching test TestPosixSpinLock");
    shared_code_for_tests<PosixSpinLockExclusiveAccessTest>(3);
    LOG4IBS_WARN(logger, "End of test TestPosixSpinLock");
}

TEST_F(testsLocks, DummyTestPosixTryMutex) {
    LOG4IBS_WARN(logger, "Launching test DummyTestPosixTryMutex");
    PosixMutex mutex4TryLockTest;
    if (mutex4TryLockTest.trylock()) {
        mutex4TryLockTest.unlock();
    }
    LOG4IBS_WARN(logger, "End of test DummyTestPosixTryMutex");
}

} /* namespace ibs */

