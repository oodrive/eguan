package io.eguan.utils;

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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.net.SyslogAppender;

/**
 * Utility class for logs.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class LogUtils {

    private static final String SYSLOG_ENABLE_STR = "io.eguan.log.enableSyslog";
    /** True if syslog log enable */
    private static final boolean SYSLOG_ENABLE = Boolean.getBoolean(SYSLOG_ENABLE_STR);

    private static final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final AtomicBoolean initSyslogDone = new AtomicBoolean(false);
    private static final String pattern = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n";

    /**
     * Tells if syslog is enabled.
     * 
     * @return true if the syslog is enabled
     */
    public static final boolean getSyslogEnable() {
        return SYSLOG_ENABLE;
    }

    /**
     * Initialize the syslog appender for logback. Use the system property io.eguan.log.enableSyslog to
     * activate log with syslog.
     * 
     * UDP syslog reception must be activated in /etc/rsyslog.conf with $ModLoad imudp and $UDPServerRun 514.
     * $EscapeControlCharactersOnReceive can be set to off, so that rsyslog will not escape the tab characters by adding
     * #011
     * 
     */
    public static final void initSysLog() {

        if (SYSLOG_ENABLE) {
            synchronized (initSyslogDone) {
                if (initSyslogDone.get()) {
                    return;
                }
                final SyslogAppender syslogAppender = new SyslogAppender();
                syslogAppender.setContext(rootLogger.getLoggerContext());
                syslogAppender.setName("SYSLOG");
                syslogAppender.setSuffixPattern(pattern);
                syslogAppender.setFacility("DAEMON");
                syslogAppender.setSyslogHost("localhost");
                syslogAppender.start();

                rootLogger.addAppender(syslogAppender);
                initSyslogDone.set(true);
            }
        }
    }

    /**
     * Stop the syslog appender for logback.
     */
    public static final void finiSyslog() {
        synchronized (initSyslogDone) {
            if (!initSyslogDone.get()) {
                return;
            }
            final SyslogAppender syslogAppender = (SyslogAppender) rootLogger.getAppender("SYSLOG");
            if (syslogAppender != null) {
                if (syslogAppender.isStarted()) {
                    syslogAppender.stop();
                }
            }
            initSyslogDone.set(false);
        }
    }
}
