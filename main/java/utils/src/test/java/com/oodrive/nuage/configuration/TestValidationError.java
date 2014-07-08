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
import static com.oodrive.nuage.configuration.ConfigTestContext.TEST_ENUM_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Tests for the methods of {@link ValidationError} not covered by other tests.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * 
 */
public final class TestValidationError {

    /**
     * Tests successful execution of {@link ValidationError#getFormattedErrorReport(ValidationError)} on a
     * single-context, single-key error, non-null-value error.
     */
    @Test
    public final void testGetFormattedErrorReportSingleContextAndKey() {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final String errMsg = "is not set";
        final String value = "NaN";
        final ValidationError target = new ValidationError(ErrorType.VALUE_REQUIRED, testContext, TEST_ENUM_KEY, value,
                errMsg);
        final String result = ValidationError.getFormattedErrorReport(target);
        assertNotNull(result);
        assertTrue(result.contains(ErrorType.VALUE_REQUIRED.toString()));
        assertTrue(result.contains(ConfigTestContext.class.getSimpleName()));
        assertTrue(result.contains(testContext.getPropertyKey(TEST_ENUM_KEY)));
        assertTrue(result.contains(value));
        assertTrue(result.contains(errMsg));
    }

    /**
     * Tests successful execution of {@link ValidationError#getFormattedErrorReport(ValidationError)} on a
     * single-context, multi-key error, non-null-value error.
     */
    @Test
    public final void testGetFormattedErrorReportSingleContextMultiKey() {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final String errMsg = "cannot be true if the other one is not set";
        final String value = "NaN";
        final ValidationError target = new ValidationError(ErrorType.VALUE_REQUIRED,
                new ConfigurationContext[] { testContext }, new ConfigKey[] { TEST_ENUM_KEY, TEST_BOOLEAN_KEY }, value,
                errMsg);
        final String result = ValidationError.getFormattedErrorReport(target);
        assertNotNull(result);
        assertTrue(result.contains(ErrorType.VALUE_REQUIRED.toString()));
        assertTrue(result.contains(ConfigTestContext.class.getSimpleName()));
        assertTrue(result.contains(testContext.getPropertyKey(TEST_ENUM_KEY)));
        assertTrue(result.contains(testContext.getPropertyKey(TEST_BOOLEAN_KEY)));
        assertTrue(result.contains(value));
        assertTrue(result.contains(errMsg));
    }

    /**
     * Tests successful execution of {@link ValidationError#getFormattedErrorReport(ValidationError)} on a
     * single-context, no key error, non-null-value error.
     */
    @Test
    public final void testGetFormattedErrorReportSingleContextNoKey() {
        final ConfigTestContext testContext = ConfigTestContext.getInstance();
        final String errMsg = "cannot be true if the other one is not set";
        final String value = "NaN";
        final ValidationError target = new ValidationError(ErrorType.VALUE_REQUIRED, testContext, null, value, errMsg);
        final String result = ValidationError.getFormattedErrorReport(target);
        assertNotNull(result);
        assertTrue(result.contains(ErrorType.VALUE_REQUIRED.toString()));
        assertTrue(result.contains(ConfigTestContext.class.getSimpleName()));
        for (final AbstractConfigKey currKey : testContext.getConfigKeys()) {
            assertFalse(result.contains(testContext.getPropertyKey(currKey)));
        }
        assertTrue(result.contains(value));
        assertTrue(result.contains(errMsg));
    }

    /**
     * Tests failure of {@link ValidationError#getFormattedErrorReport(ValidationError)} due to a <code>null</code>
     * argument.
     */
    @Test(expected = NullPointerException.class)
    public final void testGetFormattedErrorReportFailNullError() {
        ValidationError.getFormattedErrorReport(null);
    }
}
