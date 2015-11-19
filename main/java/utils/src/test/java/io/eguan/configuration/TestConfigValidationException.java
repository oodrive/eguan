package io.eguan.configuration;

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

import static org.junit.Assert.assertEquals;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.ValidationError.ErrorType;

import java.util.ArrayList;

import org.junit.Test;

/**
 * Tests for the methods of {@link ConfigValidationException} not covered by {@link TestMetaConfiguration}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestConfigValidationException {

    /**
     * Tests failure to construct a {@link ConfigValidationException} due to a {@code null} parameter.
     * 
     * @throws IllegalArgumentException
     *             if the report is empty. Not part of this test.
     * @throws NullPointerException
     *             if the argument is {@code null}. Expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public final void testCreateConfigValidationExceptionFailNullReport() throws IllegalArgumentException,
            NullPointerException {
        new ConfigValidationException(null);
    }

    /**
     * Tests failure to construct a {@link ConfigValidationException} due to a {@code null} parameter.
     * 
     * @throws IllegalArgumentException
     *             if the report is empty. Expected for this test.
     * @throws NullPointerException
     *             if the argument is {@code null}. Not part of this test.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testCreateConfigValidationExceptionFailEmptyReport() throws IllegalArgumentException,
            NullPointerException {
        new ConfigValidationException(new ArrayList<ValidationError>());
    }

    /**
     * Tests iterating through all constants of {@link ErrorType} and calling {@link ErrorType#valueOf(String)}.
     * 
     * This is necessary for getting 100% coverage on the enum type.
     * 
     * @throws IllegalArgumentException
     *             if a value fails parsing into a constant. Not part of this test.
     * @throws NullPointerException
     *             if a value is {@code null}. Not part of this test.
     */
    @Test
    public final void testValidationErrorTypes() throws IllegalArgumentException, NullPointerException {
        for (final ErrorType currType : ErrorType.values()) {
            assertEquals(currType, ErrorType.valueOf(currType.toString()));
        }
    }

}
