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
 * @file DumpTask.cpp
 * @brief The creation task API/source
 * @author j. caba
 */
#include "CreationTask.h"
#include "IbpGenHandler.h"
#include "FileTools.h"
#include "Logger.h"
#include "Constants.h"

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("DumpTask");

bool DumpTask::getDumpCondition(const size_t limit) {
    return parent.buffers.count() > limit;
}

bool DumpTask::dumpNext(const int limit) {
    bool stillNeedToDump = false;
    // protect against concurrent dump
    // dump is incremental
    if (this->dumping.exchange(true) == false) {
        if (this->getDumpCondition(limit)) {
            LOG4IBS_DEBUG(logger, "Start promotion of data in the IBP databases at dump limit=" << limit);
            parent.promoteOldestData();
            LOG4IBS_DEBUG(logger, "End promotion of data in the IBP databases at dump limit=" << limit);

            stillNeedToDump = true; // let check next iteration
        }else{
            stillNeedToDump = false;
        }
        this->dumping = false;
    }
    else {
        stillNeedToDump = true; // already dumping set to true to retry a dump later
    }
    return stillNeedToDump;
}

void DumpTask::DumpLoop(DumpTask& self) noexcept {
    LOG4IBS_DEBUG(logger, "Starting buffer persistence from hot to cold data loop.");

    IbpGenHandler& parent = self.parent;

    while (!parent.isClosed()) {
        bool stillNeedToDump = self.dumpNext(PERSIST_LIMIT);
        if (!stillNeedToDump) {
            // nothing to dump
            std::chrono::microseconds waitDuration(PERSIST_WAIT_STEP);
            std::this_thread::sleep_for(waitDuration);
        }
    }
}

} /* namespace ibs */

