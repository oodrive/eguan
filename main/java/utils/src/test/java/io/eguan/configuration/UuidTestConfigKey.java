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

import io.eguan.configuration.UuidConfigKey;

import java.util.UUID;

/**
 * Test extension of the abstract {@link UuidConfigKey} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class UuidTestConfigKey extends UuidConfigKey {

    private static final UUID DEFAULT_VALUE = UUID.randomUUID();

    public UuidTestConfigKey() {
        super("uuid.test.key");
    }

    @Override
    protected final UUID getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
