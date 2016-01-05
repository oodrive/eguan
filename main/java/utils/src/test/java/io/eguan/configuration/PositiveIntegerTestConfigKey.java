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

import io.eguan.configuration.IntegerConfigKey.PositiveIntegerConfigKey;

/**
 * Test extension of the abstract {@link PositiveIntegerConfigKey} class.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
final class PositiveIntegerTestConfigKey extends PositiveIntegerConfigKey {

    private static final String NAME = "integer.test.key";

    /**
     * Default constructor initializing a strict instance with {@value Integer#MAX_VALUE} as upper limit.
     */
    public PositiveIntegerTestConfigKey() {
        super(NAME, Integer.MAX_VALUE, true);
    }

    @Override
    public final Integer getDefaultValue() {
        return null;
    }

}
