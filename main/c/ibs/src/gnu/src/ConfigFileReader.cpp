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
 * @file ConfigFileReader.cpp
 * @brief The configuration file reader/source
 */

#include "ConfigFileReader.h"
#include "FileTools.h"
#include <fstream>
#include <vector>

namespace ibs {

Logger_t ConfigFileReader::logger = ibs::Logger::getLogger("ConfigFileReader");

ConfigFileReader::ConfigFileReader(const std::string& filename) :
        filepath(FileTools::getAbsolutePath(filename)), loaded(false) {
}

ConfigFileReader::~ConfigFileReader() = default;

std::vector<std::string> ConfigFileReader::getAllKeys() {
    std::vector<std::string> res;

    for (auto& kv : cfg) {
        res.emplace_back(kv.first);
    }
    return res;
}

std::string ConfigFileReader::getString(const std::string& key) const {
    // NOTE: can't use [] operator because it adds an entry in the map ...
    auto found = this->cfg.find(key);
    if (found != this->cfg.end()) {
        return found->second;
    }
    else {
        return std::string();
    }
}

void ConfigFileReader::setString(const std::string& key, const std::string& value) {
    if (key.empty() || value.empty())
        return;
    this->cfg[key] = value;
}

int ConfigFileReader::getInt(const std::string& key) const {
    const std::string& value = getString(key);
    if (!value.empty())
        return ::atoi(value.c_str());
    else
        return 0;
}

void ConfigFileReader::setInt(const std::string& key, int value) {
    std::ostringstream ss;
    ss << value;
    setString(key, ss.str());
}

void ConfigFileReader::eraseKey(const std::string& key) {
    if (!key.empty()) {
        this->cfg.erase(key);
    }
}

void ConfigFileReader::read() {
    std::fstream f;
    std::string s, k, v;
    LOG4IBS_INFO(logger, "Reading configuration file fname='" << this->filepath << "'");
    f.open(this->filepath.c_str());
    assert(f.is_open());
    while (getline(f, s)) {
        if (s.find('#') == 0)
            continue;
        k = s.substr(0, s.find('='));
        v = s.substr(s.find('=') + 1, s.find('#') - s.find('=') - 1);
        if (!(k.empty() || v.empty())) {
            auto it = cfg.find(k);
            if (it == cfg.end()) {
                LOG4IBS_DEBUG(logger, "Configuration value " << k << "='" << v << "'");
                cfg[k] = v;
            }
            else {
                LOG4IBS_WARN(logger,
                        "Configuration value " << k << "='" << v << "' was already defined previously with value '" << cfg[k] << "'");
                LOG4IBS_WARN(logger, "The first defined value is assumed to be the right one");
            }
        }
    }
    loaded = true;
    f.close();
}

void ConfigFileReader::write() const {
    write(this->filepath);
}

void ConfigFileReader::write(const std::string& filename) const {
    std::ofstream f(filename.c_str(), std::ios_base::trunc);
    LOG4IBS_INFO(logger, "Saving configuration file '" << filename << "'");
    for (auto ite : cfg) {
        f << ite.first << "=" << ite.second << std::endl;
    }
    f.close();
}

const std::string& ConfigFileReader::getFilename() const {
    return this->filepath;
}

} /* namespace ibs */
