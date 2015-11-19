package io.eguan.vold;

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

import io.eguan.hash.HashAlgorithm;
import io.eguan.vold.Vold;
import io.eguan.vold.model.VoldTestHelper;
import io.eguan.vold.model.VoldTestHelper.CompressionType;

import java.nio.channels.OverlappingFileLockException;

import org.junit.Test;

public class TestMultiVoldDirectory {

    @Test(expected = OverlappingFileLockException.class)
    public void testMultiVoldSameVoldDir() throws Exception {
        final VoldTestHelper helper = new VoldTestHelper(CompressionType.no, HashAlgorithm.MD5, Boolean.TRUE);

        try {
            helper.createTemporary();
            try {
                helper.start();
                // let vold run a little
                Thread.sleep(1000);

                // Try to init a new Vold on the same Vold file
                final Vold vold = new Vold(helper.getVoldFile());
                vold.init(false);
            }
            finally {
                helper.stop();
            }
        }
        finally {
            helper.destroy();
        }

    }

    @Test
    public void testMultiVoldSameVoldDirAfter() throws Exception {

        final VoldTestHelper helper = new VoldTestHelper(CompressionType.no, HashAlgorithm.MD5, Boolean.TRUE);

        try {
            helper.createTemporary();
            helper.start();
            Thread.sleep(1000); // let vold run a little
            helper.stop();

            // Try to start a new Vold on the same Vold file
            final Vold vold = new Vold(helper.getVoldFile());
            try {
                vold.init(true);

                try {
                    vold.start();
                    Thread.sleep(1000); // let vold run a little
                }
                finally {
                    vold.stop();
                }
            }
            finally {
                vold.fini();
            }
        }
        finally {
            helper.destroy();
        }
    }
}
