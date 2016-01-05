package io.eguan.configuration;

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

import io.eguan.configuration.UrlConfigKey;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Test extension of the abstract {@link UrlConfigKey} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class UrlTestConfigKey extends UrlConfigKey {

    private final URL DEFAULT_VALUE;

    public UrlTestConfigKey() {
        super("url.test.key");
        try {
            DEFAULT_VALUE = new URL("file://./");
        }
        catch (final MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected final URL getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
