package io.eguan.nbdsrv;

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

import io.eguan.nbdsrv.NbdDevice;
import io.eguan.nbdsrv.NbdExport;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Unit tests for the creation of a NBD target.
 * 
 * @author oodrive
 * @author ebredzinski
 */
public class TestNbdExportCreate {

    /** Dummy name */
    private static final String DUMMY_NAME = "dummy target";
    /** Dummy non null device */
    static final NbdDevice DUMMY_DEVICE = new NbdDevice() {
        @Override
        public final long getSize() {
            return 4096;
        }

        @Override
        public final boolean isReadOnly() {
            return false;
        }

        @Override
        public final void write(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            // No op
        }

        @Override
        public final void read(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            // No op
        }

        @Override
        public final void close() throws IOException {
            // No op
        }

        @Override
        public void trim(final long length, final long storageIndex) throws IOException {
            // No op
        }
    };

    @Test(expected = NullPointerException.class)
    public void testNullName() {
        new NbdExport(null, DUMMY_DEVICE);
    }

    @Test(expected = NullPointerException.class)
    public void testNullDevice() {
        new NbdExport(DUMMY_NAME, null);
    }
}
