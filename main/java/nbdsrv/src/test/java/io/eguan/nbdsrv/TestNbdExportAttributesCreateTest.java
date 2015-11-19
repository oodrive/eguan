package io.eguan.nbdsrv;

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
import io.eguan.nbdsrv.NbdExportAttributes;

import org.junit.Test;

/**
 * Unit tests for the creation of a NBD export attributes.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public class TestNbdExportAttributesCreateTest {
    @Test(expected = NullPointerException.class)
    public void testNullName() {
        new NbdExportAttributes(null, 0, 2L, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeConnectionCount() {
        final NbdExportAttributes nbdExportAttributes = new NbdExportAttributes("1", -1, 2L, false);
        assertEquals("1", nbdExportAttributes.getName());
        assertEquals(0, nbdExportAttributes.getConnectionCount());
        assertEquals(2L, nbdExportAttributes.getSize());
        assertEquals(false, nbdExportAttributes.isReadOnly());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullSize() {
        new NbdExportAttributes("1", 0, 0L, false);
    }

    @Test
    public void testNormal() {
        final NbdExportAttributes nbdExportAttributes = new NbdExportAttributes("myName", 654, 213233334444841L, true);
        assertEquals("myName", nbdExportAttributes.getName());
        assertEquals(654, nbdExportAttributes.getConnectionCount());
        assertEquals(213233334444841L, nbdExportAttributes.getSize());
        assertEquals(true, nbdExportAttributes.isReadOnly());
    }

}
