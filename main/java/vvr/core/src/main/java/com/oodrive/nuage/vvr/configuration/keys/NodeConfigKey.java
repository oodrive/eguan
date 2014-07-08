package com.oodrive.nuage.vvr.configuration.keys;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import com.oodrive.nuage.configuration.UuidConfigKey;

/**
 * Key holding the uuid of the node where this VVR is running.
 * <p>
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
 * <td>{@value #NAME}</td>
 * <td>Uuid of the node running this VOLD instance</td>
 * <td>FALSE (temporary)</td>
 * <td>RFC 4122 compliant string representation of a {@link UUID}</td>
 * <td>{@link UUID}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class NodeConfigKey extends UuidConfigKey {
    private static final String NAME = "node";

    private static final NodeConfigKey INSTANCE = new NodeConfigKey();

    public static NodeConfigKey getInstance() {
        return INSTANCE;
    }

    private NodeConfigKey() {
        super(NAME);
    }

    @Override
    protected final UUID getDefaultValue() {
        return null;
    }
}
