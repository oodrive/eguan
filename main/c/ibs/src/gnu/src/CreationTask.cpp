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
 * @file CreationTask.cpp
 * @brief The creation task API/source
 * @author j. caba
 */
#include "CreationTask.h"
#include "IbpGenHandler.h"
#include "FileTools.h"
#include "Logger.h"
#include "Constants.h"
#include "RefCountedDb.h"

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("CreationTask");

void CreationTask::printStats(IbpGenHandler& toDescribe) {
    std::string stats;
    bool isValid = toDescribe.getStats(stats);
    if (isValid && !stats.empty()) {
        LOG4IBS_DEBUG(logger, "Stats: " << stats);
    }
}

bool CreationTask::createNext(IbpGenHandler& whereToCreate) noexcept {
    bool created = false;
    creationProtection.lock();
    uint64_t bufferIdToCreate = newestIndex + 1;
    std::string dbFile;
    {
        std::ostringstream oss;
        std::string directory = whereToCreate.getBaseDirectory();
        oss << directory << "/" << "buffer_" << bufferIdToCreate << ".ibs";

        dbFile = oss.str();
    }
    if (!dbFile.empty()) {
        RefCountedDb* db = whereToCreate.createOpenNewBuffer(dbFile);
        if (NULL == db) {
            LOG4IBS_ERROR(logger, "Creation of next buffer '" << dbFile << "' failed");
            delete db;
        }
        else {
            LOG4IBS_DEBUG(logger, "Creation of next buffer '" << dbFile << "' OK");
            whereToCreate.buffers.insertNewest(db);
            newestIndex = bufferIdToCreate;
            whereToCreate.updateWriteDelay();
            created = true;
        }
    }
    creationProtection.unlock();
    return created;
}

void CreationTask::CreateLoop(CreationTask& self) noexcept {
    LOG4IBS_DEBUG(logger, "Starting buffer creation loop.");

    IbpGenHandler& parent = self.parent;

    while (!parent.isClosed()) {
        bool creationNeeded = self.conditions.isCreationPending();
        LOG4IBS_DEBUG(logger, "buffer rotation waiting delay=" << BUFFER_ROTATION_WAIT_STEP);
        std::chrono::microseconds waitDuration(BUFFER_ROTATION_WAIT_STEP);
        std::this_thread::sleep_for(waitDuration);

        uint64_t timeSpent = self.conditions.updateTimeSpent(BUFFER_ROTATION_WAIT_STEP);
        const uint64_t timeCriteria = parent.getWriteMaxDelay();

        if (creationNeeded || (timeSpent >= timeCriteria)) {
            // The delay was reached or a request to create
            // a new buffer in the FIFO was done on put in hot data
            LOG4IBS_DEBUG(logger, "create next " << (creationNeeded ? "based on write" : "based on time"));
            self.createNext(parent);
            self.conditions.reset();
            if (LOG4IBS_IS_ENABLED_FOR_DEBUG (logger))
                self.printStats(parent);
        }
    }
}

} /* namespace ibs */

