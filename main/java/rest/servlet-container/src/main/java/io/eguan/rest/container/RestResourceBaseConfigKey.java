package io.eguan.rest.container;

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

import io.eguan.configuration.FileConfigKey;
import io.eguan.configuration.UrlConfigKey;

import java.net.URL;

/**
 * Key holding the URL to the resource base for REST web application execution.
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
 * <td>The URL to the resource base for the REST web application execution.</td>
 * <td>FALSE</td>
 * <td>a valid URL</td>
 * <td>{@link String}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public final class RestResourceBaseConfigKey extends UrlConfigKey {

    protected static final String NAME = "rest.resourcebase";

    private static final RestResourceBaseConfigKey INSTANCE = new RestResourceBaseConfigKey();

    private static final URL DEFAULT_VALUE = RestResourceBaseConfigKey.class.getResource("webapp");

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #WebAppResourceBaseConfigKey()}
     */
    public static final RestResourceBaseConfigKey getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs the singleton instance as a {@link FileConfigKey} using the unique name {@value #NAME}.
     */
    private RestResourceBaseConfigKey() {
        super(NAME);
    }

    @Override
    public final URL getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
