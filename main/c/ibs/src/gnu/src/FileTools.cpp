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
 * @file FileTools.cpp
 * @brief Various file tools for the Ibs/source
 */

#include "FileTools.h"
#include "Logger.h"
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <ftw.h>
#include <libgen.h>
#include <string.h>
#include <string>

namespace ibs {

static Logger_t logger = ibs::Logger::getLogger("FileTools");

bool FileTools::exists(const std::string& path) {
    struct stat sb;
    return 0 == stat(path.c_str(), &sb);
}

typedef char* (*func_t)(char*);

template<func_t func>
std::string parsename(const std::string& path) {
    // according to man page input could be modified
    std::string pathCopy;
    pathCopy.assign(path);
    char* name = func(const_cast<char*>(pathCopy.c_str())); // statically allocated (DO NOT free!)
    std::string result;
    result.assign(name);
    return result;
}

std::string FileTools::getBasename(const std::string& path) {
    // let space in template the compiler won't like "<::"
    // parsename< ::basename >(path);
    std::string absPath = getAbsolutePath(path);
    return parsename< ::basename >(path);
}

bool FileTools::hasIbsExtension(const std::string& fileBasename) {
    size_t size = fileBasename.size();
    bool endWithIbsExtension = (size >= 4) && (fileBasename[size - 4] == '.') && (fileBasename[size - 3] == 'i')
            && (fileBasename[size - 2] == 'b') && (fileBasename[size - 1] == 's');
    return endWithIbsExtension;
}

std::string FileTools::getAbsolutePath(const std::string& inputPath) {
    const char* pathname = inputPath.c_str();
    std::string path;
    char commonBuffer[PATH_MAX];
    memset(commonBuffer, 0, PATH_MAX);
    if (realpath(pathname, commonBuffer) != NULL) {
        path = std::string(commonBuffer);
    }
    else {
        LOG4IBS_DEBUG(logger, "Error while getting absolute path for path=" << inputPath);
    }
    return path;
}

bool FileTools::isDirectory(const std::string& path) {
    struct stat sb;
    return (0 == stat(path.c_str(), &sb) && S_ISDIR(sb.st_mode));
}

bool FileTools::dirOk(const std::string& path) {
    struct stat sb;
    if (stat(path.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) { // path exists
        if (access(path.c_str(), R_OK | W_OK | X_OK) == 0) { // is writable
            return true;
        }
        else {
            LOG4IBS_DEBUG(logger, "Insufficient permission on dir path='" << path << "'");
        }
    }
    else {
        LOG4IBS_DEBUG(logger, "Unable to access dir path='" << path << "'");
    }
    return false;
}

bool FileTools::isFile(const std::string& path) {
    struct stat sb;
    return (0 == stat(path.c_str(), &sb) && S_ISREG(sb.st_mode));
}

bool FileTools::isReadable(const std::string& path) {
    bool result = false;
    struct stat sb;
    if (0 == stat(path.c_str(), &sb)) {
        result = (0 == access(path.c_str(), R_OK | F_OK));
    }
    return result;
}

bool FileTools::isWritable(const std::string& path) {
    bool result = false;
    struct stat sb;
    if (0 == stat(path.c_str(), &sb)) {
        result = (0 == access(path.c_str(), R_OK | W_OK | F_OK));
    }
    return result;
}

bool FileTools::list(const std::string& iPath, std::vector<std::string>& oFilenames, Filter iFilter) {
    bool result = false;
    DIR* dir = opendir(iPath.c_str());
    if (dir) {
        struct dirent* ent = NULL;
        while ((ent = readdir(dir)) != NULL) {
            std::string name(ent->d_name);
            std::string fullPath = iPath + "/" + name;
            if (0 != name.compare(".") && 0 != name.compare("..")) {
                bool store = (nofilter == iFilter);
                if (!store) {
                    bool is_dir;
                    // We can spare a call to isDir if the platform and the filesystem supports it
#                   ifdef _DIRENT_HAVE_D_TYPE
                    unsigned char dtype = ent->d_type;
                    if (DT_UNKNOWN == dtype) {
                        // d_type not supported by the filesystem
                        is_dir = isDirectory(fullPath);
                    }
                    else {
                        is_dir = (DT_DIR == dtype);
                    }
#                   else
                    // d_type not available on the compilation platform
                    is_dir = isDirectory(fullPath);
#                   endif
                    store = ((is_dir && directory == iFilter) || (!is_dir && not_a_directory == iFilter));
                }
                if (store) {
                    oFilenames.push_back(name);
                }
            }
        }
        closedir(dir);
        result = true;
    }
    return result;
}

bool FileTools::isDirectoryEmpty(const std::string& path) {
    uint n = 0;
    DIR *dir = opendir(path.c_str());
    if (dir == NULL) // Not a directory or doesn't exist
        return 1;
    // FIXME: the use of readdir assuming . and .. are reported is not portable ...
    while (readdir(dir) != NULL) {
        if (++n > 2)
            break;
    }
    closedir(dir);
    return (n <= 2);
}

bool FileTools::createDirectory(const std::string& path) {
    if (exists(path) && isDirectory(path)) {
        return true;
    }
    return 0 == ::mkdir(path.c_str(), 0755);
}

BinaryFileBase::~BinaryFileBase() = default;

void BinaryFileBase::setRawBytes(std::ofstream& f, const char* input, const size_t inputSize) {
    assert(input != NULL);
    f.write(input, inputSize);
}

void BinaryFileBase::updateRawBytes(std::fstream& f, const char* input, const size_t inputSize) {
    assert(input != NULL);
    f.write(input, inputSize);
}

void BinaryFileBase::setString(std::ofstream& f, const std::string& input) {
    setUint64(f, input.size());
    setRawBytes(f, input.data(), input.size());
}

void BinaryFileBase::updateString(std::fstream& f, const std::string& input) {
    updateUint64(f, input.size());
    updateRawBytes(f, input.data(), input.size());
}

void BinaryFileBase::getString(std::ifstream& f, std::string& output) {
    getStringEncoding<std::ifstream>(f, output);
}

void BinaryFileBase::fetchString(std::fstream& f, std::string& output) {
    getStringEncoding<std::fstream>(f, output);
}

void BinaryFileBase::setUint64(std::ofstream& f, const uint64_t uinteger) {
    setUint64Encoding<std::ofstream>(f, uinteger);
}

void BinaryFileBase::updateUint64(std::fstream& f, const uint64_t uinteger) {
    setUint64Encoding<std::fstream>(f, uinteger);
}

uint64_t BinaryFileBase::getUint64(std::ifstream& f) {
    return getUint64Encoding<std::ifstream>(f);
}

uint64_t BinaryFileBase::fetchUint64(std::fstream& f) {
    return getUint64Encoding<std::fstream>(f);
}

void FileTools::removeFile(const std::string& path) {
    remove(path.c_str());
}

static int removeDirectoryCallback(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) {
    if (remove(fpath)) {
        // remove failure
        int e = errno;
        LOG4IBS_ERROR(logger, "Could not remove " << fpath);
        LOG4IBS_ERROR(logger, "description: " << strerror(e));
        LOG4IBS_ERROR(logger, "sb: " << sb);
        LOG4IBS_ERROR(logger, "typeflag: " << typeflag);
        LOG4IBS_ERROR(logger, "ftwbuf: " << ftwbuf);
    }
    return 0;
}

void FileTools::removeDirectory(const std::string& path) {
    const char* pathname = path.c_str();
    const int nftw_options = FTW_DEPTH;
    // browse a directory using ntfw
    {
        const int nftw_depth = 128;
        int ret = nftw(pathname, removeDirectoryCallback, nftw_depth, nftw_options);
        if (ret > 0)
            return;
        remove(pathname);
    }
}

} /* namespace ibs */
