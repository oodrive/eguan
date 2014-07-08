package com.oodrive.nuage.nbdsrv;

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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import com.oodrive.nuage.srv.AbstractDeviceFile;

public class NbdDeviceFile extends AbstractDeviceFile implements NbdDevice {

    NbdDeviceFile(final FileChannel fileChannel, final String path) {
        super(fileChannel, path);
    }

    private final ArrayList<TestTrim> trimList = new ArrayList<>();

    static class TestTrim {
        private final long length;
        private final long offset;

        TestTrim(final long length, final long offset) {
            super();
            this.length = length;
            this.offset = offset;
        }

        final long getLength() {
            return length;
        }

        final long getOffset() {
            return offset;
        }
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public final void trim(final long len, final long from) throws IOException {
        trimList.add(new TestTrim(len, from));
    }

    final TestTrim peekTrim() {
        return trimList.remove(0);
    }

    final int getTrimListSize() {
        return trimList.size();
    }
}
