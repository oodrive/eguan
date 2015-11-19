package io.eguan.net;

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

import com.google.protobuf.MessageLite;

/**
 * The application logic is specified through the implementation of {@link MsgServerHandler}. When a message is
 * received, the message server call the function {@link MsgServerHandler#handleMessage(MessageLite)}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public interface MsgServerHandler {

    /**
     * The function called when a message is received. If an exception occurs while processing the function then the
     * server send the exception name to the client (in case of a synchronous call). The server may return a reply which
     * is received in case of a synchronous call only.
     * 
     * @param message
     *            the received message
     * @return message reply or <code>null</code>
     */
    MessageLite handleMessage(MessageLite message);
}
