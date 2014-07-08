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

import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_BOOLEAN_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_EMPTY_STRING_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_ENUM_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_FILELIST_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_FILE_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_POS_INT_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_STRING_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_URL_KEY;
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_UUID_KEY;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * Helper class for configuration tests.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
final class ConfigTestHelper {

    /**
     * Private default constructor to prevent instantiation.
     */
    private ConfigTestHelper() {
        throw new AssertionError("Not supposed to be instantiated");
    }

    /**
     * Gets a default configuration for the {@link ConfigTestContext}.
     * 
     * @return a complete and valid default configuration
     */
    protected static Properties getDefaultTestConfiguration() {
        final Properties result = new Properties();
        // sets a few values for defined keys
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final String posIntKey = testContext.getPropertyKey(TEST_POS_INT_KEY);
        result.setProperty(posIntKey, Integer.toString(Integer.MAX_VALUE));

        final String fileKey = testContext.getPropertyKey(TEST_FILE_KEY);
        result.setProperty(fileKey, "/tmp/data");

        final String enumKey = testContext.getPropertyKey(TEST_ENUM_KEY);
        result.setProperty(enumKey, EnumTestValue.TEST_VALUE_1.toString());

        final String booleanKey = testContext.getPropertyKey(TEST_BOOLEAN_KEY);
        result.setProperty(booleanKey, BooleanConfigKey.VALUES_FOR_FALSE.get(0));

        final String fileListKey = testContext.getPropertyKey(TEST_FILELIST_KEY);
        result.setProperty(fileListKey, "/var/run;/opt");

        final String uuidKey = testContext.getPropertyKey(TEST_UUID_KEY);
        result.setProperty(uuidKey, UUID.randomUUID().toString());

        final String stringKey = testContext.getPropertyKey(TEST_STRING_KEY);
        result.setProperty(stringKey, "non-default value");

        final String emptyStringKey = testContext.getPropertyKey(TEST_EMPTY_STRING_KEY);
        result.setProperty(emptyStringKey, "non-empty value");

        final String urlKey = testContext.getPropertyKey(TEST_URL_KEY);
        result.setProperty(urlKey, "http://www.example.com/index.html");

        final String unmgtKey1 = "com.oodrive.nuage.java.utils.configuration.test.unmanaged1";
        final String unmgtKey2 = "com.oodrive.nuage.java.utils.configuration.test.unmanaged2";
        final String unmgtValue1 = "unmanaged value 1";
        final String unmgtValue2 = "unmanaged value 2";
        result.setProperty(unmgtKey1, unmgtValue1);
        result.setProperty(unmgtKey2, unmgtValue2);

        return result;
    }

    /**
     * Helper method to get a given {@link Properties} instance as {@link InputStream}.
     * 
     * @param properties
     *            the non-{@code null} {@link Properties} instance to convert
     * @return an {@link InputStream} providing the exact content of the argument
     * @throws IOException
     *             if storing fails
     */
    protected static InputStream getPropertiesAsInputStream(final Properties properties) throws IOException {
        // construct an InputStream from the properties
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        properties.store(outputStream, "Configuration properties for testing");
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

}
