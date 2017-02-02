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
 * @file Logger.h
 * @brief Log management wrapper/header
 */
#ifndef LOGGER_H_
#define LOGGER_H_

/* include log4cplus header files. */
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#include <log4cplus/configurator.h>
#include <log4cplus/syslogappender.h>
#include <iomanip>
#include <iostream>
#include <atomic>

namespace ibs {

/* default log level */
#define LOG_LEVEL_DEF_FUNC  log4cplus::WARN_LOG_LEVEL

/* define macros to change implementation easily
 * and to ensure the following order is respected
 * we have TRACE < DEBUG < INFO < WARN < ERROR < FATAL < OFF.
 * if the implementation doesn't do it.
 * NOTE: actually log4cplus does it so this is provided as a "facade".
 * */
#define LOG4IBS_TRACE(logger, ...)  \
        LOG4CPLUS_TRACE(logger, __VA_ARGS__)

#define LOG4IBS_DEBUG(logger, ...)  \
        LOG4CPLUS_DEBUG(logger, __VA_ARGS__)

#define LOG4IBS_INFO(logger,  ...)  \
        LOG4CPLUS_INFO(logger, __VA_ARGS__)

#define LOG4IBS_WARN(logger,  ...)  \
        LOG4CPLUS_WARN(logger, __VA_ARGS__)

#define LOG4IBS_ERROR(logger, ...)  \
        LOG4CPLUS_ERROR(logger, __VA_ARGS__)

#define LOG4IBS_FATAL(logger, ...)  \
        LOG4CPLUS_FATAL(logger, __VA_ARGS__)

#define LOG4IBS_IS_ENABLED_FOR_DEBUG(logger) \
    LOG4IBS_IS_ENABLED_FOR(logger, DEBUG_LOG_LEVEL)

#define LOG4IBS_IS_ENABLED_FOR(logger, logLevel)    \
    (logger).isEnabledFor(log4cplus::logLevel)

typedef log4cplus::Logger Logger_t;

/**
 * @brief IbsLogger class. It takes care about logger centralization.
 *
 * It partly hides log4cxx calls.
 */
class Logger {
    public:
        /**
         * @brief Fetch a logger by its name. It will be created if non existent.
         * @param name The name of the logger.
         * @return A pointer to the associated logger or a new one.
         */
        static Logger_t getLogger(const std::string& name);

        /**
         * @brief Fetch the root logger, see loc4cxx for details about logger hierarchy.
         * @return A pointer to the unique root logger.
         */
        static Logger_t getRootLogger();

        /**
         * @brief Ensure that the configuration in made only once
         */
        static void initialize_once();

        /**
         * @brief Initialize syslog.
         */
        static void initialize_syslog(bool enabled);

        /**
         * @brief Ensure that the configuration in made only once
         */
        static void setLevel(const std::string& loglevel);
    private:
        Logger() = delete;
        static std::atomic_bool _initialized;

};

} /* namespace ibs */
#endif /* LOGGER_H_ */
