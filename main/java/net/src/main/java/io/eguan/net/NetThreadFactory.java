package io.eguan.net;

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

import java.util.concurrent.ThreadFactory;

/**
 * Select the name of the threads.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
final class NetThreadFactory implements ThreadFactory {
    private final String prefix;
    private int index = 1;

    NetThreadFactory(final String prefix) {
        super();
        this.prefix = prefix;
    }

    @Override
    public final Thread newThread(final Runnable r) {
        final Thread thread = new Thread(r, prefix + index++);
        thread.setDaemon(true);
        return thread;
    }

}
