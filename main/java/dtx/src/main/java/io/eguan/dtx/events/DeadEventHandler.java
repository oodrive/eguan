package io.eguan.dtx.events;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;

/**
 * Sink for dead events sent to any Guava EventBus.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DeadEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadEventHandler.class);

    private final Logger targetLogger;

    /**
     * Constructs a new instance with the optional destination {@link Logger}.
     * 
     * @param targetLogger
     *            the optional Logger to which to send warning messages upon receiving dead events
     */
    public DeadEventHandler(final Logger targetLogger) {
        this.targetLogger = targetLogger;
    }

    /**
     * Accepts any dead event and logs a warning message.
     * 
     * @param deadEvent
     *            the {@link DeadEvent} to log
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void acceptDeadEvents(final DeadEvent deadEvent) {
        // TODO: attempt recovery of dead events
        final String logMsg = "Dead event received; source=" + deadEvent.getSource() + ", event="
                + deadEvent.getEvent();
        if (targetLogger == null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logMsg);
            }
        }
        else {
            if (targetLogger.isDebugEnabled()) {
                targetLogger.debug(logMsg);
            }
        }

    }

}
