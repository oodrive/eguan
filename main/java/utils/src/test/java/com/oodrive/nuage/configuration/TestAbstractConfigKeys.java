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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

import com.oodrive.nuage.configuration.ValidationError.ErrorType;

/**
 * Abstract class testing implementations of the abstract methods of {@link AbstractConfigKey} on subclasses.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public abstract class TestAbstractConfigKeys {

    /**
     * Specific enum used as type certain to be rejected by all correct {@link AbstractConfigKey#checkValue(Object)} and
     * {@link AbstractConfigKey#parseValue(String)} implementations.
     * 
     * 
     */
    private static enum BadTypeEnum {
        /**
         * The constant no {@link AbstractConfigKey} should accept as value.
         */
        NOT_YOUR_TYPE;
    }

    /**
     * Tests successful executions of {@link AbstractConfigKey#parseValue(String)} and
     * {@link AbstractConfigKey#valueToString(Object)} using the default value.
     * 
     * @throws IllegalArgumentException
     *             if the value is not parsable. Not part of this test.
     * @throws NullPointerException
     *             if the value is {@code null}. Not part of this test.
     * @throws ConfigValidationException
     * @throws IOException
     */
    @Test
    public final void testGetTypedValue() throws IllegalArgumentException, NullPointerException, IOException,
            ConfigValidationException {
        final AbstractConfigKey target = getTestKey(false, true);

        final Object defaultValue = target.getDefaultValue();
        assertNotNull(defaultValue);

        final AbstractConfigurationContext testContext = new AbstractConfigurationContext("test.abstract.context",
                target) {
        };

        final String inputString = testContext.getPropertyKey(target) + "=" + target.valueToString(defaultValue);

        final MetaConfiguration config = MetaConfiguration.newConfiguration(
                new ByteArrayInputStream(inputString.getBytes()), testContext);

        assertEquals(defaultValue, target.getTypedValue(config));
    }

    /**
     * Tests successful executions of {@link AbstractConfigKey#parseValue(String)} and
     * {@link AbstractConfigKey#valueToString(Object)} using the default value.
     * 
     * @throws IllegalArgumentException
     *             if the value is not parsable. Not part of this test.
     * @throws NullPointerException
     *             if the value is {@code null}. Not part of this test.
     */
    @Test
    public final void testParseValueWithValueToString() throws IllegalArgumentException, NullPointerException {
        final AbstractConfigKey target = getTestKey(false, true);

        final Object defaultValue = target.getDefaultValue();
        assertNotNull(defaultValue);

        final String defaultString = target.valueToString(defaultValue);

        final Object result = target.parseValue(defaultString);

        assertNotNull(result);
        assertEquals("parsed result is of same type as default value", defaultValue.getClass(), result.getClass());
        assertEquals("parsed result has the same String value as initial default result", defaultString,
                target.valueToString(result));
    }

    /**
     * Tests successful execution of {@link AbstractConfigKey#parseValue(String)} with an empty {@link String} as
     * parameter.
     * 
     * @throws IllegalArgumentException
     *             if the value is not parsable. Considered a test failure.
     * @throws NullPointerException
     *             if the value is {@code null}. Not part of this test.
     */
    @Test
    public final void testParseValueEmptyString() {
        final AbstractConfigKey target = getTestKey(false, false);

        final Object value = target.parseValue("");

        assertTrue((value == null) || ("".equals(value)));

        assertEquals("", target.valueToString(value));

    }

    /**
     * Tests failure of {@link AbstractConfigKey#parseValue(String)} due to a {@code null} parameter.
     * 
     * @throws IllegalArgumentException
     *             if the value is not parsable. Not part of this test.
     * @throws NullPointerException
     *             if the value is {@code null}. Expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public final void testParseValueFailNull() throws IllegalArgumentException, NullPointerException {
        final AbstractConfigKey target = getTestKey(false, false);

        target.parseValue(null);

    }

    /**
     * Tests execution of {@link AbstractConfigKey#valueToString(Object)} with a {@code null} parameter.
     * 
     * @throws IllegalArgumentException
     *             if the value is not serializable. Not part of this test.
     */
    @Test
    public final void testValueToStringNull() throws IllegalArgumentException {
        final AbstractConfigKey target = getTestKey(false, false);

        final String result = target.valueToString(null);

        assertEquals("", result);

    }

    /**
     * Tests failure of {@link AbstractConfigKey#valueToString(Object)} due to parameter of wrong type.
     * 
     * @throws IllegalArgumentException
     *             if the value is of the wrong type. Expected for this test.
     * @throws NullPointerException
     *             if the value is {@code null}. Not part of this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testValueToStringFailWrongType() {
        final AbstractConfigKey target = getTestKey(false, false);

        target.valueToString(BadTypeEnum.NOT_YOUR_TYPE);

    }

    /**
     * Tests failure of {@link AbstractConfigKey#checkValue(Object)} due to passing an object of bad type.
     */
    @Test
    public final void testCheckValueFailWrongType() {
        final AbstractConfigKey target = getTestKey(false, false);

        final ValidationError result = target.checkValue(BadTypeEnum.NOT_YOUR_TYPE);

        assertEquals("validation result is ", ErrorType.VALUE_INVALID, result.getType());
        assertEquals("validation result has empty context list", 0, result.getConfigurationContexts().length);
        assertEquals("validation result has right key", target, result.getConfigKeys()[0]);
        assertEquals("validation result has provided value", BadTypeEnum.NOT_YOUR_TYPE, result.getValue());
    }

    /**
     * Tests failure of {@link AbstractConfigKey#checkValue(Object)} due to passing null to a
     * {@link AbstractConfigKey#isRequired() required} key.
     */
    @Test
    public final void testCheckValueFailNullAndRequired() {
        final AbstractConfigKey target = getTestKey(true, false);
        assertTrue("value is required", target.isRequired());
        assertFalse("has no default value", target.hasDefaultValue());
        assertNull("default value is null", target.getDefaultValue());

        final ValidationError result = target.checkValue(null);

        assertEquals("validation result is ", ErrorType.VALUE_REQUIRED, result.getType());
        assertEquals("validation result has empty context list", 0, result.getConfigurationContexts().length);
        assertEquals("validation result has right key", target, result.getConfigKeys()[0]);
        assertNull("validation result has provided value", result.getValue());
    }

    /**
     * Test successfully execution of {@link AbstractConfigKey#checkValue(Object)} given null on a key without default
     * value nor {@link AbstractConfigKey#isRequired() required} constraint.
     */
    @Test
    public final void testCheckValueNullAndNotRequired() {
        final AbstractConfigKey target = getTestKey(false, false);
        assertFalse("value is not required", target.isRequired());
        assertFalse("has no default value", target.hasDefaultValue());
        assertNull("default value is null", target.getDefaultValue());

        final ValidationError result = target.checkValue(null);

        assertEquals("validation produced no error", ValidationError.NO_ERROR, result);
    }

    /**
     * Test the {@link AbstractConfigKey#toString()} implementation for all extending classes.
     */
    @Test
    public final void testToString() {
        final AbstractConfigKey target = getTestKey(false, false);
        assertEquals("toString returns class' canonical name", target.getClass().getCanonicalName(), target.toString());
    }

    /**
     * Gets the {@link AbstractConfigKey key} to be tested.
     * 
     * @param required
     *            the value {@link AbstractConfigKey#isRequired()} on the resulting key will return
     * @param hasDefault
     *            the value {@link AbstractConfigKey#hasDefaultValue()} on the resulting key will return
     * @return an instance of the {@link AbstractConfigKey} subclass to be tested
     */
    protected abstract AbstractConfigKey getTestKey(boolean required, boolean hasDefault);

}
