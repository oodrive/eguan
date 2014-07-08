package com.oodrive.nuage.configuration;

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

import javax.annotation.concurrent.Immutable;

/**
 * {@link ConfigurationContext} implementation for testing purposes.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 */
@Immutable
final class ConfigTestContext extends AbstractConfigurationContext implements Cloneable {

    /**
     * Gets the singleton instance of this class.
     * 
     * @return the singleton instance, never {@code null}
     */
    public static ConfigTestContext getInstance() {
        return INSTANCE;
    }

    /**
     * Name to be prefixed to property keys.
     */
    protected static final String NAME = "com.oodrive.nuage.java.utils.configuration.test";

    /**
     * A test key taking Integer values.
     */
    public static final PositiveIntegerTestConfigKey TEST_POS_INT_KEY = new PositiveIntegerTestConfigKey();

    /**
     * A test key taking File paths as values.
     */
    public static final FileTestConfigKey TEST_FILE_KEY = new FileTestConfigKey();

    /**
     * A test key taking the constants of {@link ConfigTestContext.EnumTestValue} as values.
     */
    public static final EnumTestConfigKey TEST_ENUM_KEY = new EnumTestConfigKey();

    /**
     * A test key taking boolean values.
     */
    public static final BooleanTestConfigKey TEST_BOOLEAN_KEY = new BooleanTestConfigKey();

    /**
     * A test key taking a file list.
     */
    public static final FileListTestConfigKey TEST_FILELIST_KEY = new FileListTestConfigKey();

    /**
     * A test key taking a UUID.
     */
    public static final UuidTestConfigKey TEST_UUID_KEY = new UuidTestConfigKey();

    /**
     * A test key taking a String.
     */
    public static final StringTestConfigKey TEST_STRING_KEY = new StringTestConfigKey();

    /**
     * A test key taking a String with an empty default value.
     */
    public static final EmptyStringTestConfigKey TEST_EMPTY_STRING_KEY = new EmptyStringTestConfigKey();

    /**
     * A test key taking a URL.
     */
    public static final UrlTestConfigKey TEST_URL_KEY = new UrlTestConfigKey();

    /**
     * A test key with the same values as {@link #TEST_ENUM_KEY}, but to be left intentionally undefined.
     */
    public static final UndefinedTestConfigKey TEST_UNDEFINED_KEY = new UndefinedTestConfigKey();

    /**
     * Field holding the singleton instance created at first class usage.
     */
    private static final ConfigTestContext INSTANCE = new ConfigTestContext();

    /**
     * Private constructor for singleton class.
     */
    private ConfigTestContext() {
        super(NAME, TEST_POS_INT_KEY, TEST_FILE_KEY, TEST_ENUM_KEY, TEST_BOOLEAN_KEY, TEST_FILELIST_KEY, TEST_UUID_KEY,
                TEST_STRING_KEY, TEST_EMPTY_STRING_KEY, TEST_URL_KEY, TEST_UNDEFINED_KEY);
    }
}
