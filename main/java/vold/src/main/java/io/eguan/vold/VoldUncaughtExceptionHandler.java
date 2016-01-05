package io.eguan.vold;

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

import io.eguan.vold.model.Constants;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.Logger;

/**
 * Simple uncaught exception handler. It simply logs those uncaught exception.
 * 
 * @author oodrive
 * @author llambert
 */
final class VoldUncaughtExceptionHandler implements UncaughtExceptionHandler {
    private static final Logger LOGGER = Constants.LOGGER;

    VoldUncaughtExceptionHandler() {
        super();
    }

    @Override
    public final void uncaughtException(final Thread t, final Throwable e) {
        LOGGER.warn("Uncaught exception, thread: " + t.toString(), e);
    }
}
