package io.eguan.configuration;

import io.eguan.configuration.EnumConfigKey;

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

/**
 * {@link EnumConfigKey} implementation to be intentionally left undefined.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class UndefinedTestConfigKey extends EnumConfigKey<EnumTestValue> {

    /**
     * Default constructor creating a default {@link EnumTestValue}-based {@link EnumConfigKey}.
     */
    protected UndefinedTestConfigKey() {
        super("undefined.value", EnumTestValue.class);
    }

    @Override
    protected final Object getDefaultValue() {
        return null;
    }

    @Override
    public final boolean isRequired() {
        // explicitly override to allow undefined
        return false;
    }

}
