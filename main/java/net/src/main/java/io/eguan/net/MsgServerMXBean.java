package io.eguan.net;

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

import java.util.UUID;

public interface MsgServerMXBean {

    /**
     * Gets server uuid.
     * 
     * @return the string of the {@link UUID}.
     */
    String getUuid();

    /**
     * Gets server IP address.
     * 
     * @return the read-only ip address.
     */
    String getIpAddress();

    /**
     * Gets server port.
     * 
     * @return the read-only port.
     */
    int getPort();

    /**
     * Tells if the server is started.
     * 
     * @return <code>true</code> if started.
     */
    boolean isStarted();

    /**
     * Restart the server.
     * 
     */
    void restart();

}
