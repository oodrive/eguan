package io.eguan.iscsisrv;

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

import io.eguan.srv.ServerInetAddressConfigKey;

/**
 * Key defining the address of the server.
 * 
 * <table border='1'>
 * <tr>
 * <th>NAME</th>
 * <th>DESCRIPTION</th>
 * <th>REQUIRED</th>
 * <th>UNIT</th>
 * <th>TYPE</th>
 * <th>DEFAULT</th>
 * <th>MIN</th>
 * <th>MAX</th>
 * </tr>
 * <tr>
 * <td>{@value ServerInetAddressConfigKey#NAME}</td>
 * <td>Binding address of the server.</td>
 * <td>FALSE</td>
 * <td></td>
 * <td>InetAddress</td>
 * <td>0.0.0.0</td>
 * <td>NA</td>
 * <td>NA</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class IscsiServerInetAddressConfigKey extends ServerInetAddressConfigKey {

    private static final IscsiServerInetAddressConfigKey INSTANCE = new IscsiServerInetAddressConfigKey();

    public static IscsiServerInetAddressConfigKey getInstance() {
        return INSTANCE;
    }

}
