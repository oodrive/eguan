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
 * @file AbstractTask.h
 * @brief The abstract task API/header
 * @author j. caba
 */
#ifndef ABSTRACTTASK_H_
#define ABSTRACTTASK_H_

#include <thread>
#include <atomic>
#include <system_error>

namespace ibs {

/**
 * @brief Defines the generic interface for a task executed in a separate thread
 * and owned by another parent instance.
 */
template<typename T>
class AbstractTask {
    protected:
        AbstractTask(T& owner) :
                parent(owner), task(), started(false), stopping(false) {
        }

        /**
         * @brief Implement this method to initialize task member (thread creation)
         */
        virtual void createThread() = 0;

    public:
        /**
         * @brief At destruction stop task (if not already done)
         */
        virtual ~AbstractTask() {
            stop();
        }

        /**
         * @brief Create and start the task in a separate thread. (if not already done)
         */
        void start() {
            if (started.exchange(true) == false) {
                createThread();
            }
        }

        /**
         * @return If the task is stopped.
         */
        bool isStarted() const {
            return started;
        }

        /**
         * @brief Join and stop the task's separate thread.
         */
        void stop() {
            if (started) {
                if (stopping.exchange(true) == false) {
                    if (task.joinable()) {
                        try {
                            // join the thread
                            task.join();
                        }
                        catch (std::system_error& e) {
                            // try to detach then
                            // because thread need to be joined or detach at destruction
                            try {
                                task.detach();
                            }
                            catch (...) {
                            }
                        }
                    }
                    started = false;
                    stopping = false;
                }
            }
        }

        /**
         * @return If the task is stopped.
         */
        bool isStopped() const {
            return !started;
        }

    protected:
        T& parent; /** parent instance */
        std::thread task; /** the separate thread for task */

    private:
        std::atomic<bool> started; /** if the thread is started */
        std::atomic<bool> stopping; /** if the thread is stopping */

        // non copyable
        AbstractTask(const AbstractTask&) = delete;
        AbstractTask& operator=(const AbstractTask&) = delete;
};

} /* namespace ibs */
#endif /* ABSTRACTTASK_H_ */
