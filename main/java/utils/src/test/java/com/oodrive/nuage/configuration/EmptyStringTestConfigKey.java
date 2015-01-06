package com.oodrive.nuage.configuration;

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
 * Test extension of the abstract {@link StringConfigKey} class with an empty default value.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class EmptyStringTestConfigKey extends StringConfigKey {

    public EmptyStringTestConfigKey() {
        super("empty.string.test.key");
    }

    @Override
    protected final String getDefaultValue() {
        return "";
    }

}
