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
 * @file Logger.cpp
 * @brief Log management wrapper/source
 */
#include "Logger.h"

namespace ibs {

std::atomic_bool Logger::_initialized(false);

void Logger::initialize_once() {
    if (false == _initialized.exchange(true)) {
        // Add a ConsoleAppender to see the logs on console
        // and ensure only one appenders is added to avoid crash
        // default configuration and format
        log4cplus::BasicConfigurator::doConfigure();
        log4cplus::Logger::getRoot().setLogLevel(LOG_LEVEL_DEF_FUNC);
        // Avoid possible crash on exit due to static logger wrong destruction
        // by safely closing and removing all appenders in all loggers at exit
        std::atexit(log4cplus::Logger::shutdown);
    }
}

void Logger::initialize_syslog(bool enabled) {
    if (enabled) {
        log4cplus::SharedAppenderPtr appender(new log4cplus::SysLogAppender("SYSLOG", "127.0.0.1", 514, "DAEMON"));
        std::auto_ptr<log4cplus::Layout> layout(new log4cplus::PatternLayout("[%T] %-5p %c{36} - %m%n"));
        appender->setLayout(layout);
        log4cplus::Logger::getRoot().addAppender(appender);
    }
}

Logger_t Logger::getLogger(const std::string& name) {
    initialize_once();
    return log4cplus::Logger::getInstance(name);
}

Logger_t Logger::getRootLogger() {
    initialize_once();
    return log4cplus::Logger::getRoot();
}

void Logger::setLevel(const std::string& loglevel) {
    if (loglevel == "fatal")
        getRootLogger().setLogLevel(log4cplus::FATAL_LOG_LEVEL);
    else if (loglevel == "error")
        getRootLogger().setLogLevel(log4cplus::ERROR_LOG_LEVEL);
    else if (loglevel == "warn")
        getRootLogger().setLogLevel(log4cplus::WARN_LOG_LEVEL);
    else if (loglevel == "info")
        getRootLogger().setLogLevel(log4cplus::INFO_LOG_LEVEL);
    else if (loglevel == "debug")
        getRootLogger().setLogLevel(log4cplus::DEBUG_LOG_LEVEL);
    else if (loglevel == "trace")
        getRootLogger().setLogLevel(log4cplus::TRACE_LOG_LEVEL);
    else if (loglevel == "off")
        getRootLogger().setLogLevel(log4cplus::OFF_LOG_LEVEL);
}

} /* namespace ibs */
