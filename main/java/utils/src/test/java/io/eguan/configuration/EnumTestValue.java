package io.eguan.configuration;

import io.eguan.configuration.EnumConfigKey;

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
 * Testing enum for {@link ConfigTestContext#TEST_ENUM_KEY}, an {@link EnumConfigKey} implementation.
 * 
 * Constants intentionally implement specific dummy methods to appear as member instances of the enum class.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * @see ConfigTestContext#TEST_ENUM_KEY
 */
enum EnumTestValue {
    TEST_VALUE_1("value1") {
        @SuppressWarnings("unused")
        public String lowerCaseName() {
            return getName().toLowerCase();
        }
    },
    TEST_VALUE_2("value2") {
        @SuppressWarnings("unused")
        public String upperCaseName() {
            return getName().toUpperCase();
        }
    };

    private String name;

    private EnumTestValue(final String name) {
        this.name = name;
    }

    protected final String getName() {
        return name;
    }
}
