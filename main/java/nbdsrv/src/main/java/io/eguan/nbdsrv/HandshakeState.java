package io.eguan.nbdsrv;

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
 * States of the handshake phase.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public enum HandshakeState {
    /* Initialization state */
    INITIALIZATION,
    /* Reception of the global flags from the client */
    GLOBAL_FLAGS_RECEPTION,
    /* Reception of the options from the client */
    OPTIONS_NEGOCIATION
}
