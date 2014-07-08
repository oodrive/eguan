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
 * @file RefCountedDbFile.cpp
 * @brief The reference counted db file /source
 * @author j. caba
 */

#include "RefCountedDbFile.h"

namespace ibs {

const u_char RefCountedDbFile::magic[magicSize] = { 'R', 'E', 'F', 'D', 'B' }; // magic number starting a recorder file

void RefCountedDbFile::setHeader() {
    writeStream.seekp(0, std::ios::beg);
    // set magic numbers
    for (u_short i = 0; i < magicSize; i++) {
        writeStream << magic[i];
    }
    // set version of format
    updateUint64(writeStream, currentVersion);
    // set creation time
    updateUint64(writeStream, static_cast<uint64_t>(time(NULL)));
    writeStream.seekp(0, std::ios::end);
    writeStream.flush();
}

bool RefCountedDbFile::hasValidHeader(const std::string& filename) {
    std::ifstream file(filename, readOnlymode);

    // read magic #
    u_char fetchedMagic[5];
    for (u_short i = 0; i < 5; i++) {
        file >> fetchedMagic[i];
    }
    bool isGood = (fetchedMagic[0] == 'R') && (fetchedMagic[1] == 'E') && (fetchedMagic[2] == 'F')
            && (fetchedMagic[3] == 'D') && (fetchedMagic[4] == 'B');
    if (isGood) {
        // get version of format
        uint64_t version = 0;
        version = getUint64(file);
        isGood = (version == currentVersion);
    }
    file.close();
    return isGood;
}

} /* namespace ibs */
