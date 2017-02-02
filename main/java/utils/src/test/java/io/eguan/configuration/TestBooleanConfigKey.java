package io.eguan.configuration;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.BooleanConfigKey;

import org.junit.Test;

/**
 * Implementation of {@link TestAbstractConfigKeys} for testing {@link BooleanConfigKey}s.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestBooleanConfigKey extends TestAbstractConfigKeys {

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        return new BooleanConfigKey("test.boolean.key") {

            @Override
            protected final Boolean getDefaultValue() {
                return hasDefault ? Boolean.FALSE : null;
            }

            @Override
            public final boolean isRequired() {
                return required;
            }
        };
    }

    /**
     * Tests {@link BooleanConfigKey#parseValue(String) parsing } all legal values for {@link Boolean} values.
     */
    @Test
    public final void testParseValueSyntax() {
        final AbstractConfigKey target = getTestKey(true, false);
        // test values for TRUE
        assertEquals(Boolean.TRUE, target.parseValue("yes"));
        assertEquals(Boolean.TRUE, target.parseValue("YES"));
        assertEquals(Boolean.TRUE, target.parseValue("true"));
        assertEquals(Boolean.TRUE, target.parseValue("TRUE"));
        // test values for FALSE
        assertEquals(Boolean.FALSE, target.parseValue("no"));
        assertEquals(Boolean.FALSE, target.parseValue("NO"));
        assertEquals(Boolean.FALSE, target.parseValue("false"));
        assertEquals(Boolean.FALSE, target.parseValue("FALSE"));
    }

    /**
     * Tests correct serialization of all possible values by {@link BooleanConfigKey#valueToString(Object)}.
     */
    @Test
    public final void testValueToString() {
        final AbstractConfigKey target = getTestKey(true, false);
        // test value for TRUE
        assertTrue(BooleanConfigKey.VALUES_FOR_TRUE.contains(target.valueToString(Boolean.TRUE)));
        // test value for FALSE
        assertTrue(BooleanConfigKey.VALUES_FOR_FALSE.contains(target.valueToString(Boolean.FALSE)));
    }

    /**
     * Tests failure to execute {@link BooleanConfigKey#parseValue(String)} due to an illegal value.
     */
    @Test(expected = IllegalArgumentException.class)
    public final void testParseValueFailNotBoolean() {
        final AbstractConfigKey target = getTestKey(true, false);
        target.parseValue("not boolean");
    }

}
