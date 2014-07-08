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
 * @file DumpTask.h
 * @brief The dump task API/header
 * @author j. caba
 */
#ifndef DUMPTASK_H_
#define DUMPTASK_H_

#include "AbstractTask.h"

namespace ibs {

class IbpGenHandler;

/**
 * @brief Defines the implementation for the task dumping database from ibpgen to ibp.
 * i.e: Persistence and destruction of the oldest ibpgen database.
 */
class DumpTask: public AbstractTask<IbpGenHandler> {
    public:
        DumpTask(IbpGenHandler& ipbGen) :
                AbstractTask(ipbGen), dumping(false) {
        }

        virtual void createThread() {
            task = std::move(std::thread(DumpLoop, std::ref(*this)));
        }

        bool isDumping() const {
            return dumping;
        }

    protected:
        static void DumpLoop(DumpTask& self) noexcept;

        bool getDumpCondition(const size_t limit);

        bool dumpNext(const int limit);

    private:
        std::atomic<bool> dumping; /** if dumping/persiting data */
};

} /* namespace ibs */
#endif /* DUMPTASK_H_ */
