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
 * @file ConfigFileReader.h
 * @brief The configuration file reader/header
 */
#ifndef CONFIGFILEREADER_H_
#define CONFIGFILEREADER_H_

#include "Logger.h"
#include <unordered_map>
#include <string>

namespace ibs {

/**
 * @brief Reader of configuration file loading it in memory. Based on key=value scheme.
 *
 * Read a file containing simple key-value in memory association designed to store the current configuration.
 * <p>
 * Sample configuration file :
 * <pre>
 * loglevel=warn
 * ibp_path=/dir1,/dir2
 * uuid=88888888-4444-4444-4444-121212121212
 * </pre>
 * <p>
 *
 */
class ConfigFileReader {
    public:
        /**
         * @brief Constructor.
         *
         * @param filename The configuration file (full or relative path).
         */
        ConfigFileReader(const std::string& filename);

        /**
         * @brief Default destructor.
         */
        virtual ~ConfigFileReader();

        /**
         * @brief Return all loaded keys.
         */
        std::vector<std::string> getAllKeys();

        /**
         * @brief Fetch a value -string-
         *
         * This method search for a value associated to a key.
         *
         * @param key The record we are looking for.
         * @return The value or an empty string if not found.
         */
        std::string getString(const std::string& key) const;

        /**
         * @brief Store a value in memory configuration
         *
         * This method is used to store a value associated to a key, both of them are string.
         *
         * @param key A key that would be used to get back the value.
         * @param value A value that will be store.
         */
        void setString(const std::string& key, const std::string& value);

        /**
         * @brief Fetch a value -integer-.
         *
         * This method search for a value associated to a key.
         *
         * @param key The record we are looking for.
         * @return The value or 0 if not found.
         */
        int getInt(const std::string& key) const;

        /**
         * @brief Store an integer value
         *
         * This method is used to store a value associated to a key, both of them are string.
         *
         * @param key A key that would be used to get back the value.
         * @param value A value that will be store.
         */
        void setInt(const std::string& key, int value);

        /**
         * @brief Erases a key from the in memory configuration
         * @param key A key to delete
         */
        void eraseKey(const std::string& key);

        /**
         * @brief Get an tokenize a value -string-
         */
        void getTokenizedString(const std::string& key, std::vector<std::string>& paths) {
            std::istringstream iss(getString(key));
            std::string token;
            while (getline(iss, token, ',')) {
                LOG4IBS_DEBUG(logger, "token='" << token << "'");
                paths.emplace_back(token);
                token.clear();
            }
            paths.shrink_to_fit();
        }

        /**
         * @brief Indicate if the configuration is "loaded"
         * The configuration is loaded only if read method was called
         * and did not failed to charge the configuration keys in memory.
         */
        bool isLoaded() const {
            return loaded;
        }

        /**
         * @brief Read file and save it in map.
         */
        void read();

        /**
         * @brief Persist the current state.
         *
         * This method overwrite the file that was previously set when calling init.
         */
        void write() const;

        /**
         * @brief Write configuration in a specified filename.
         * @param filename The file that will be written.
         */
        void write(const std::string& filename) const;

        /**
         * @brief Return the configuration file path as absolute path.
         * @return The full configuration file path.
         */
        const std::string& getFilename() const;

    protected:
        ConfigFileReader() = delete;

    private:
        std::unordered_map<std::string, std::string> cfg; /** map containing key=value configuration */
        std::string filepath; /** configuration file path */
        bool loaded; /** if the configuration is loaded  */
        static Logger_t logger;

        // non copyable
        ConfigFileReader(const ConfigFileReader&) = delete;
        ConfigFileReader& operator=(const ConfigFileReader&) = delete;
};

} /* namespace ibs */
#endif /* CONFIGFILEREADER_H_ */
